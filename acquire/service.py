"""签到队列的采集流程。

一条设备事务可能拆在多个队列消息里。这里的分工是：

* 每条消息的片段先持久保存（原件、片段号、领取凭据）；
* 只有片段齐备且整份摘要相符时才发布签到；
* 发布之后才向来源批量确认，因此确认响应丢失、消息重投都不会生成第二份签到；
* 未发布前不确认任何 handle，避免确认掉来源里的唯一副本；
* 片段缺失时继续领取，租约将到期时续租（不超过来源的 30 秒上限）。

对外查询由 Store 的 ``transaction_state`` / ``transaction_states`` /
``unacknowledged`` 提供。
"""

import hashlib
import json
import time

from .decode import decode
from .fragments import parse_message
from .queue import QueueClient
from .store import Store
from .transport import fetch

MAX_LEASE_SECONDS = QueueClient.MAX_LEASE_SECONDS


def collect_one(url, database, *, method='GET', headers=None, body=None, max_bytes=1048576):
    response=fetch(url,method=method,headers=headers,body=body,max_bytes=max_bytes)
    if not 200 <= response.status < 300:
        raise ValueError(f'source status {response.status}')
    rows=decode(response.body,dict(response.headers))
    receipt=hashlib.sha256(url.encode()+b'\0'+response.body).hexdigest()
    store=Store(database)
    try:
        store.save_response(receipt,response,rows)
        return dict(receipt=receipt,count=len(rows),documents=store.documents())
    finally:
        store.close()



class Collector:
    """一轮领取、保存、续租与确认。

    ``clock`` 可传入返回来源当前秒数的函数；默认读取来源的 ``/clock``，
    读取失败时退回本机时间。租约时长统一由 ``QueueClient`` 限制在 30 秒内。
    """

    def __init__(self, queue, store, *, lease_seconds=MAX_LEASE_SECONDS,
                 renew_before=5, clock=None):
        if not isinstance(queue, QueueClient):
            raise TypeError('queue must be a QueueClient')
        if not isinstance(store, Store):
            raise TypeError('store must be a Store')
        self.queue = queue
        self.store = store
        self.lease_seconds = queue.lease_seconds(lease_seconds)
        self.renew_before = max(0, int(renew_before))
        self._clock = clock

    # ---- 单轮采集 ------------------------------------------------------

    def poll(self, limit=10, lease_seconds=None):
        """领取至多 ``limit`` 条消息并就地处理。

        返回本轮报告；报告中的 ``waiting`` 来自存储层的真实事务状态，
        不代表所有领取成功的消息都已完成采集。
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError('limit must be a positive integer')
        lease = self.lease_seconds if lease_seconds is None else self.queue.lease_seconds(lease_seconds)
        _response, messages = self.queue.pull(limit, lease)

        report = dict(
            pulled=len(messages),
            new_fragments=[],
            duplicates=[],
            conflicts=[],
            rejected=[],
            published=[],
            acks={},
            ack_errors=[],
            renewed={},
            lease_seconds=lease,
        )
        touched = []
        for message in messages:
            self._handle_message(message, report, touched)

        attempted = set()
        for key in dict.fromkeys(touched):
            self._finish_transaction(key, report, attempted)

        # 片段来自本次领取之外的早期轮次：周期性续租仍缺片的事务。
        for state in self.store.transaction_states('waiting_parts'):
            if state['key'] not in attempted:
                self._renew_if_expiring(state['key'], state, report)

        # 确认响应丢失后消息不会再重投：对待确认事务重试确认，结果仍未知时保持
        # 待确认，而不是把已保存的内容当成已确认。
        for state in self.store.transaction_states('ack_pending'):
            if state['key'] in attempted:
                continue
            if state['unacknowledged_handles']:
                attempted.add(state['key'])
                self._acknowledge(state['key'], report)

        report['waiting'] = [
            self._summary(state) for state in self.store.pending_transactions()
        ]
        return report

    def _handle_message(self, message, report, touched):
        handle = message.get('receipt_handle') if isinstance(message, dict) else None
        message_id = message.get('message_id') if isinstance(message, dict) else None
        lease_until = message.get('lease_until') if isinstance(message, dict) else None
        try:
            fragment = parse_message(message)
        except ValueError as exc:
            if isinstance(handle, str) and handle:
                self.store.record_unparsable(
                    handle,
                    message_id,
                    raw=json.dumps(message, ensure_ascii=False),
                    lease_until=lease_until if isinstance(lease_until, int) else None,
                    note=str(exc),
                )
            report['rejected'].append(dict(handle=handle, message_id=message_id, reason=str(exc)))
            return

        self.store.record_lease(
            fragment, lease_until=fragment.lease_until,
            raw=json.dumps(message, ensure_ascii=False),
        )
        outcome = self.store.record_fragment(fragment)
        entry = dict(
            key=fragment.transaction_key,
            part=fragment.part,
            handle=fragment.receipt_handle,
            message_id=fragment.message_id,
        )
        if outcome == 'new':
            report['new_fragments'].append(entry)
            if fragment.end:
                self.store.resolve_gap(fragment.transaction_key, fragment.part)
        elif outcome == 'duplicate':
            report['duplicates'].append(entry)
        elif outcome == 'conflict':
            report['conflicts'].append(entry)
        else:
            report['rejected'].append(dict(entry, reason='fragment outside declared range'))
            return
        touched.append(fragment.transaction_key)

    def _finish_transaction(self, key, report, attempted):
        state = self.store.evaluate(key)
        if state is None:
            return

        if state['status'] == 'ready':
            created, receipt = self.store.publish(key)
            if created:
                report['published'].append(dict(key=key, receipt=receipt))
            state = self.store.transaction_state(key)

        if not state['data_saved']:
            # 内容尚未完整保存：不确认。只有在等缺片时才续租，冲突或来源已不可补
            # 的事务不再占用租约。
            if state['status'] == 'waiting_parts':
                attempted.add(key)
                self._renew_if_expiring(key, state, report)
            return

        # 内容已持久保存（或此前已保存）：可以确认这条事务的所有未确认凭据。
        attempted.add(key)
        self._acknowledge(key, report)

    # ---- 续租 ----------------------------------------------------------

    def _now(self):
        if self._clock is not None:
            return int(self._clock())
        return self.queue.now(default=int(time.time()))

    def _renew_if_expiring(self, key, state, report):
        leases = [l for l in self.store.leases(key) if l['acked_at'] is None]
        expiries = [
            max(v for v in (l['lease_until'], l['renewed_until']) if v is not None)
            for l in leases
            if l['lease_until'] is not None or l['renewed_until'] is not None
        ]
        if not expiries:
            return
        try:
            now = self._now()
        except Exception as exc:  # 来源时钟不可用：不冒险续租错误的时间
            report['renewed'].setdefault('errors', []).append(str(exc))
            return
        if min(expiries) - now > self.renew_before:
            return
        handles = [l['handle'] for l in leases]
        try:
            result = self.queue.renew(handles, self.lease_seconds)
        except Exception as exc:
            report['renewed'].setdefault('errors', []).append(str(exc))
            return
        accepted = []
        for handle in handles:
            status = result.status(handle, 'unknown')
            self.store.mark_renewed(handle, status, result.lease_until(handle))
            report['renewed'][handle] = status
            if status == 'accepted':
                accepted.append(handle)
        # 续租已被来源拒绝（过期/未知）时，只会等来重投的新 handle；旧 handle
        # 就此结束，不再占用后续续租与确认。
        for handle in handles:
            if handle not in accepted:
                self.store.mark_ack(handle, result.status(handle, 'unknown'))
        self.store.evaluate(key)

    # ---- 确认 ----------------------------------------------------------

    def acknowledge(self, handles):
        """批量确认给定 handle，并记录来源逐 handle 的结果。

        ``expired`` / ``unknown`` 只说明这次领取凭据不可用，已保存的原件
        不会因此删除；下一次重投会带来新的 handle。
        """
        handles = [h for h in dict.fromkeys(handles) if isinstance(h, str) and h]
        if not handles:
            return {}
        try:
            result = self.queue.ack(handles)
        except Exception as exc:
            return {'error': str(exc)}
        for handle in handles:
            if handle in result.statuses:
                self.store.mark_ack(handle, result.statuses[handle])
            else:
                # 来源没有提到这个 handle：不当作已确认，保留待确认状态。
                self.store.note_lease(handle, 'ack response omitted handle')
        return dict(result.statuses)

    def acknowledge_pending(self):
        """对内容已保存但确认结果未知的事务重试批量确认。

        消息一去不返（确认在来源已落地、响应丢失）时，这是把待确认收敛到已确认
        的路径；结果仍未知就保持待确认。返回 ``{key: {handle: status}}``。
        """
        result = {}
        for state in self.store.transaction_states('ack_pending'):
            handles = state['unacknowledged_handles']
            if not handles:
                continue
            result[state['key']] = self.acknowledge(handles)
            self.store.evaluate(state['key'])
        return result

    def _acknowledge(self, key, report):
        handles = [l['handle'] for l in self.store.leases(key) if l['acked_at'] is None]
        if not handles:
            return
        try:
            result = self.queue.ack(handles)
        except Exception as exc:
            report['ack_errors'].append(dict(key=key, error=str(exc), handles=handles))
            return
        for handle in handles:
            if handle not in result.statuses:
                report['ack_errors'].append(
                    dict(key=key, error='ack response omitted handle', handles=[handle]))
                continue
            self.store.mark_ack(handle, result.statuses[handle])
            report['acks'][handle] = result.statuses[handle]
        self.store.evaluate(key)

    # ---- 来源已不可补 --------------------------------------------------

    def close_gaps(self, reason='source retention period ended', keys=None, observed_handle=None):
        """来源不再提供缺片时，为每条未完成事务留下可定位缺口。

        内容完整但确认未落地的事务保持待确认，不当作缺口。
        """
        result = []
        wanted = set(keys) if keys is not None else None
        for state in self.store.pending_transactions():
            if wanted is not None and state['key'] not in wanted:
                continue
            if state['status'] in ('waiting_parts', 'conflict', 'invalid', 'digest_mismatch'):
                result.append(self.store.abandon(state['key'], reason, observed_handle))
            else:
                result.append(state)
        return result

    # ---- 报告辅助 ------------------------------------------------------

    @staticmethod
    def _summary(state):
        reason = {
            'waiting_parts': 'waiting_for_missing_parts',
            'ack_pending': 'awaiting_ack_confirmation',
            'unrecoverable': 'source_no_longer_available',
            'conflict': 'fragment_conflict',
            'invalid': 'invalid_fragment',
            'digest_mismatch': 'digest_mismatch',
            'published': 'confirmed',
        }.get(state['status'], state['status'])
        return dict(
            key=state['key'],
            device=state['device'],
            epoch=state['epoch'],
            transaction_id=state['transaction_id'],
            status=state['status'],
            reason=reason,
            collected_parts=state['collected_parts'],
            missing_parts=state['missing_parts'],
            data_saved=state['data_saved'],
            unacknowledged_handles=state['unacknowledged_handles'],
        )


def collect(url, database, *, limit=10, lease_seconds=MAX_LEASE_SECONDS,
            renew_before=5, clock=None, max_bytes=1048576, timeout=5):
    """打开一次来源、跑一轮采集并返回报告。"""
    queue = QueueClient(url, timeout=timeout, max_bytes=max_bytes)
    store = Store(database)
    try:
        return Collector(queue, store, lease_seconds=lease_seconds,
                         renew_before=renew_before, clock=clock).poll(limit)
    finally:
        store.close()


def transaction_status(database, key=None, *, status=None):
    """查询事务可用性；不给 key 时返回列表。"""
    store = Store(database)
    try:
        if key is not None:
            return store.transaction_state(key)
        return store.transaction_states(status)
    finally:
        store.close()


def unacknowledged_messages(database, key=None):
    """查询仍未确认的消息，并解释每条消息为什么还在等待。"""
    store = Store(database)
    try:
        return store.unacknowledged(key)
    finally:
        store.close()


def main(argv=None):
    from .cli import main as cli_main
    return cli_main(argv)


if __name__=='__main__':
    main()
