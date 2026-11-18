import hashlib
import json
from pathlib import Path
import tempfile
import threading
import unittest

from acquire.fragments import content_sha256, parse_message, transaction_key
from acquire.origin import make_server
from acquire.queue import QueueClient
from acquire.service import Collector, collect, transaction_status, unacknowledged_messages
from acquire.store import Store


def message(device='kiosk-A', epoch='boot-9', transaction_id=1, part=0,
            payload='', end=False, parts=None, receipt_handle='h', **extra):
    item = dict(message_id=f'{device}:{epoch}:{transaction_id}:{part}',
                receipt_handle=receipt_handle, device=device, epoch=epoch,
                transaction_id=transaction_id, part=part, payload=payload)
    if end:
        item['end'] = True
        item['parts'] = parts
        item['sha256'] = extra.pop('sha256', content_sha256([payload] * parts))
    item.update(extra)
    return item


def digest_for(payloads):
    return content_sha256(payloads)


class QueueHarness(unittest.TestCase):
    def start(self, queue_spec):
        scenario = dict(entry_path='/queue/pull', media='application/json', routes=[],
                        queue=queue_spec)
        self.server = make_server(scenario)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f'http://127.0.0.1:{self.server.server_port}/queue'
        self.store = Store(self.database)
        self.queue = QueueClient(self.base)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = str(Path(self.temporary.name) / 'data.sqlite3')

    def tearDown(self):
        if hasattr(self, 'store'):
            self.store.close()
        if hasattr(self, 'server'):
            self.server.shutdown()
            self.thread.join()
            self.server.server_close()
        self.temporary.cleanup()

    def collector(self, clock=None, **kwargs):
        return Collector(self.queue, self.store, clock=clock, **kwargs)

    def events(self, path):
        return [e for e in self.server.events if e['path'] == path]

    def advance(self, seconds):
        body = json.dumps({'seconds': seconds}).encode()
        from acquire.transport import fetch
        fetch(f'http://127.0.0.1:{self.server.server_port}/clock/advance',
              method='POST', body=body, headers={'Content-Type': 'application/json'})


class FragmentTests(unittest.TestCase):
    def test_transaction_identity_includes_device_epoch_and_id(self):
        a = transaction_key('kiosk-A', 'boot-9', 1)
        self.assertNotEqual(a, transaction_key('kiosk-A', 'boot-10', 1))
        self.assertNotEqual(a, transaction_key('kiosk-B', 'boot-9', 1))
        # 分隔符不能把两个不同事务拼成同一个键
        self.assertNotEqual(transaction_key('k', 'e', 1), transaction_key('k', 'e', 2))
        self.assertNotEqual(transaction_key('a|b', 'c', 1), transaction_key('a', 'b|c', 1))

    def test_end_fragment_requires_parts_and_digest(self):
        raw = dict(message(part=1, payload=''))
        raw.update(end=True, parts=2)
        with self.assertRaises(ValueError):
            parse_message(raw)
        raw = dict(message(part=1, payload=''))
        raw.update(end=True, sha256='a' * 64)
        with self.assertRaises(ValueError):
            parse_message(raw)
        raw = dict(message(part=0, payload=''))
        raw.update(end=True, parts=1, sha256='not-a-digest')
        with self.assertRaises(ValueError):
            parse_message(raw)

    def test_parts_and_sha256_only_apply_to_end_fragment(self):
        payloads = ['a', 'b']
        raw = message(part=0, payload='a')
        raw.update(parts=2, sha256=digest_for(payloads))
        fragment = parse_message(raw)
        self.assertIsNone(fragment.parts)
        self.assertIsNone(fragment.sha256)

    def test_digest_matches_protocol(self):
        self.assertEqual(digest_for(['signed-in', '']), hashlib.sha256(b'signed-in').hexdigest())


class CollectionTests(QueueHarness):
    def test_end_fragment_can_arrive_before_earlier_parts(self):
        payloads = ['a', 'b']
        self.start(dict(now=0, messages=[
            message(part=1, payload=payloads[1], end=True, parts=2,
                    sha256=digest_for(payloads)),
            message(part=0, payload=payloads[0]),
        ]))
        collector = self.collector()
        collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        self.assertEqual(self.store.transaction_state(key)['status'], 'published')

    def test_out_of_order_duplicate_transaction_publishes_once(self):
        payloads = ['sign-in ', '09:00']
        self.start(dict(now=0, messages=[
            message(part=1, payload=payloads[1], end=True, parts=2,
                    sha256=digest_for(payloads)),
            message(part=0, payload=payloads[0]),
            message(part=0, payload=payloads[0]),
        ]))
        collector = self.collector()
        first = collector.poll(limit=5)
        self.assertEqual(len(first['new_fragments']), 2)
        self.assertEqual(len(first['published']), 1)
        state = self.store.transaction_state(transaction_key('kiosk-A', 'boot-9', 1))
        self.assertEqual(state['status'], 'published')
        self.assertEqual(state['content_sha256'], digest_for(payloads))
        self.assertEqual(len(self.store.documents()), 1)

    def test_lease_expiry_redelivers_and_new_handle_is_recorded(self):
        payloads = ['a', 'b']
        self.start(dict(now=0, max_lease_seconds=30, messages=[
            message(part=0, payload=payloads[0]),
            message(part=1, payload=payloads[1], end=True, parts=2,
                    sha256=digest_for(payloads)),
        ]))
        collector = self.collector()
        collector.poll(limit=1)
        first_handle = self.store.fragments(transaction_key('kiosk-A', 'boot-9', 1))[0]['part']
        self.assertEqual(first_handle, 0)
        self.advance(31)
        report = collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        leases = self.store.leases(key)
        # 同一片段换了新 handle，旧 handle 作为历史保留
        self.assertEqual(leases[0]['part'], 0)
        self.assertEqual(leases[0]['handle'], 'h1')
        self.assertEqual([l['handle'] for l in leases if l['part'] == 0], ['h1', 'h2'])
        self.assertEqual([l['handle'] for l in leases if l['part'] == 1], ['h3'])
        self.assertEqual(report['pulled'], 2)
        self.assertEqual(report['duplicates'][0]['part'], 0)
        self.assertEqual(self.store.transaction_state(
            transaction_key('kiosk-A', 'boot-9', 1))['status'], 'published')

    def test_missing_part_keeps_waiting_and_not_acknowledged(self):
        payloads = ['a', 'b', 'c']
        self.start(dict(now=0, messages=[
            message(part=0, payload=payloads[0]),
            message(part=2, payload=payloads[2], end=True, parts=3,
                    sha256=digest_for(payloads)),
        ]))
        collector = self.collector()
        report = collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        state = self.store.transaction_state(key)
        self.assertEqual(state['status'], 'waiting_parts')
        self.assertEqual(state['missing_parts'], [1])
        self.assertEqual(state['unacknowledged_handles'],
                         ['h1', 'h2'])
        self.assertEqual(report['acks'], {})
        self.assertEqual(self.store.documents(), [])
        waiting = self.store.unacknowledged(key)
        self.assertTrue(all(m['reason'] == 'waiting_for_missing_parts' for m in waiting))

    def test_digest_mismatch_never_publishes(self):
        payloads = ['a', 'b']
        self.start(dict(now=0, messages=[
            message(part=0, payload=payloads[0]),
            message(part=1, payload=payloads[1], end=True, parts=2,
                    sha256=digest_for(['a', 'c'])),
        ]))
        collector = self.collector()
        collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        self.assertEqual(self.store.transaction_state(key)['status'], 'digest_mismatch')
        self.assertEqual(self.store.documents(), [])
        self.assertEqual(self.store.unacknowledged_handles(), ['h1', 'h2'])

    def test_conflicting_part_blocks_publication_and_preserves_originals(self):
        payloads = ['a', 'b']
        self.start(dict(now=0, messages=[
            message(part=0, payload=payloads[0]),
            message(part=0, payload='different'),
            message(part=1, payload=payloads[1], end=True, parts=2,
                    sha256=digest_for(payloads)),
        ]))
        collector = self.collector()
        report = collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        state = self.store.transaction_state(key)
        self.assertEqual(state['status'], 'conflict')
        self.assertEqual(len(report['conflicts']), 1)
        self.assertEqual(state['collected_parts'], [0, 1])
        self.assertEqual(self.store.fragments(key)[0]['payload'], payloads[0])
        self.assertEqual(self.store.documents(), [])
        self.assertEqual(state['unacknowledged_handles'], ['h1', 'h2', 'h3'])
        self.assertTrue(all(m['reason'] == 'fragment_conflict'
                            for m in self.store.unacknowledged(key)))

    def test_same_transaction_id_after_device_reset_is_a_new_transaction(self):
        payloads = ['a']
        self.start(dict(now=0, messages=[
            dict(message(epoch='boot-9', part=0, payload='old'), end=True, parts=1,
                 sha256=digest_for(['old'])),
            dict(message(epoch='boot-10', part=0, payload='new'), end=True, parts=1,
                 sha256=digest_for(['new'])),
        ]))
        collector = self.collector()
        collector.poll(limit=5)
        old = self.store.transaction_state(transaction_key('kiosk-A', 'boot-9', 1))
        new = self.store.transaction_state(transaction_key('kiosk-A', 'boot-10', 1))
        self.assertEqual(old['status'], 'published')
        self.assertEqual(new['status'], 'published')
        self.assertNotEqual(old['published_receipt'], new['published_receipt'])

    def test_ack_response_lost_then_redelivery_does_not_publish_twice(self):
        payloads = ['a']
        self.start(dict(now=0, ack_response='loss', messages=[
            message(part=0, payload=payloads[0], end=True, parts=1,
                    sha256=digest_for(payloads)),
        ]))
        collector = self.collector()
        report = collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        # 内容已保存，但确认结果未知：保持待确认，而不是已确认。
        self.assertEqual(report['published'], [dict(key=key,
                         receipt=self.store.transaction_state(key)['published_receipt'])])
        self.assertEqual(self.store.transaction_state(key)['status'], 'ack_pending')
        self.assertEqual(len(self.store.documents()), 1)
        self.assertTrue(self.store.unacknowledged(key))
        self.assertTrue(all(m['reason'] == 'awaiting_ack_confirmation'
                            for m in self.store.unacknowledged(key)))

        # 来源已确认但响应丢失，消息不会重投；事务仍可重试确认，但绝不生成第二份。
        second = collector.poll(limit=5)
        self.assertEqual(second['pulled'], 0)
        self.assertEqual(len(self.store.documents()), 1)
        self.assertEqual(self.store.transaction_state(key)['status'], 'ack_pending')

    def test_ack_error_keeps_content_and_waits_for_confirmation(self):
        payloads = ['a']
        self.start(dict(now=0, ack_response='error', messages=[
            message(part=0, payload=payloads[0], end=True, parts=1,
                    sha256=digest_for(payloads)),
        ]))
        collector = self.collector()
        report = collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        self.assertEqual(len(report['ack_errors']), 1)
        self.assertEqual(self.store.transaction_state(key)['status'], 'ack_pending')
        self.assertEqual(len(self.store.documents()), 1)

        # 修改来源行为后重试确认：仍只有一份签到。
        self.server.queue.ack_response = 'normal'
        collector.poll(limit=5)
        self.assertEqual(self.store.transaction_state(key)['status'], 'published')
        self.assertEqual(len(self.store.documents()), 1)

    def test_lease_is_capped_at_thirty_seconds(self):
        payloads = ['a']
        self.start(dict(now=0, max_lease_seconds=30, messages=[
            message(part=0, payload=payloads[0], end=True, parts=1,
                    sha256=digest_for(payloads)),
        ]))
        collector = self.collector(lease_seconds=300)
        self.assertEqual(collector.lease_seconds, 30)
        collector.poll(limit=1)
        request = json.loads(base64_decode(self.events('/queue/pull')[0]['body_base64']))
        self.assertEqual(request['lease_seconds'], 30)
        self.assertLessEqual(self.server.queue.lease_until[0], 30)

    def test_renew_extends_lease_while_waiting_for_parts(self):
        payloads = ['a', 'b']
        self.start(dict(now=20, messages=[
            message(part=0, payload=payloads[0]),
        ]))
        collector = self.collector(renew_before=15)
        collector.poll(limit=1)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        leases = self.store.leases(key)
        self.assertEqual(len(self.events('/queue/renew')), 1)
        self.assertIsNotNone(leases[0]['renewed_at'])
        self.assertEqual(leases[0]['renewed_until'], 50)

    def test_renewed_lease_keeps_pending_fragment_alive_then_publishes(self):
        payloads = ['a', 'b']
        self.start(dict(now=20, messages=[
            message(part=0, payload=payloads[0]),
        ]))
        collector = self.collector(renew_before=15)
        collector.poll(limit=1)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        # 续租把租约延到 now+30，来源在到期前不再重投
        self.advance(20)
        self.assertEqual(collector.poll(limit=5)['pulled'], 0)
        self.server.queue.add(
            message(part=1, payload=payloads[1], end=True, parts=2,
                    sha256=digest_for(payloads)))
        self.advance(1)
        report = collector.poll(limit=5)
        self.assertEqual(report['pulled'], 1)
        self.assertEqual(self.store.transaction_state(key)['status'], 'published')

    def test_unrecoverable_gap_is_locatable(self):
        payloads = ['a', 'b', 'c']
        self.start(dict(now=0, messages=[
            message(part=0, payload=payloads[0], available_until=5),
            message(part=2, payload=payloads[2], end=True, parts=3,
                    sha256=digest_for(payloads), available_until=5),
        ]))
        collector = self.collector()
        collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        self.advance(10)
        report = collector.poll(limit=5)
        self.assertEqual(report['pulled'], 0)
        states = collector.close_gaps(reason='retention ended')
        self.assertEqual(states[0]['status'], 'unrecoverable')
        state = self.store.transaction_state(key)
        self.assertEqual(state['missing_parts'], [1])
        self.assertEqual([g['part'] for g in state['gaps']], [1])
        self.assertEqual(state['gaps'][0]['reason'], 'retention ended')
        self.assertEqual(self.store.documents(), [])
        # 不能把不可补的事务当作已确认
        self.assertTrue(all(m['reason'] == 'source_no_longer_available'
                            for m in self.store.unacknowledged(key)))

    def test_store_survives_process_restart(self):
        payloads = ['a', 'b']
        self.start(dict(now=0, messages=[
            message(part=0, payload=payloads[0]),
            message(part=1, payload=payloads[1], end=True, parts=2,
                    sha256=digest_for(payloads)),
        ]))
        collector = self.collector()
        collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        self.store.close()
        self.store = Store(self.database)
        state = self.store.transaction_state(key)
        self.assertEqual(state['status'], 'published')
        self.assertEqual(len(self.store.documents()), 1)
        # 再次采集同一事务的重复片段不会改变状态，也不会生成第二份签到
        collector = self.collector()
        collector.poll(limit=5)
        self.assertEqual(len(self.store.documents()), 1)

    def test_late_conflicting_redelivery_does_not_revoke_published_checkin(self):
        payloads = ['a']
        self.start(dict(now=0, messages=[
            message(part=0, payload=payloads[0], end=True, parts=1,
                    sha256=digest_for(payloads)),
        ]))
        collector = self.collector()
        collector.poll(limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        receipt = self.store.transaction_state(key)['published_receipt']
        self.assertEqual(self.store.transaction_state(key)['status'], 'published')

        # 来源把同一片段以不同内容重投：原件与已发布签到都保留，冲突可查。
        self.server.queue.acked[0] = False
        self.server.queue.lease_until[0] = None
        self.server.queue.messages[0] = dict(self.server.queue.messages[0], payload='tampered')
        report = collector.poll(limit=5)
        self.assertEqual(len(report['conflicts']), 1)
        state = self.store.transaction_state(key)
        self.assertEqual(state['status'], 'published')
        self.assertEqual(state['published_receipt'], receipt)
        self.assertEqual(len(self.store.documents()), 1)
        self.assertEqual(len(state['conflicts']), 1)

    def test_queries_expose_transaction_availability_and_unacked_messages(self):
        payloads = ['a']
        self.start(dict(now=0, messages=[
            message(part=0, payload=payloads[0], end=True, parts=1,
                    sha256=digest_for(payloads)),
        ]))
        collect(self.base, self.database, limit=5)
        key = transaction_key('kiosk-A', 'boot-9', 1)
        state = transaction_status(self.database, key)
        self.assertEqual(state['status'], 'published')
        self.assertEqual(transaction_status(self.database)[0]['key'], key)
        self.assertEqual(unacknowledged_messages(self.database), [])
        self.assertEqual(self.store.unacknowledged_handles(), [])


def base64_decode(value):
    import base64
    return base64.b64decode(value)


if __name__ == '__main__':
    unittest.main()
