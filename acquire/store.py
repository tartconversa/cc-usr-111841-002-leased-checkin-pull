import hashlib
import json
import sqlite3
import time

from .fragments import content_sha256, transaction_key


SCHEMA = """
CREATE TABLE IF NOT EXISTS documents(key TEXT PRIMARY KEY,value TEXT NOT NULL,receipt TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY,url TEXT NOT NULL,status INTEGER NOT NULL,headers TEXT NOT NULL,body BLOB NOT NULL);

-- 一条设备事务：device/epoch/transaction_id 三者共同确定身份。
CREATE TABLE IF NOT EXISTS transactions(
    transaction_key TEXT PRIMARY KEY,
    device TEXT NOT NULL,
    epoch TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    declared_parts INTEGER,
    expected_sha256 TEXT,
    status TEXT NOT NULL DEFAULT 'collecting',
    published_receipt TEXT,
    first_seen_epoch INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    abandoned_at TEXT,
    abandoned_reason TEXT,
    note TEXT
);

-- 已收到的片段原件。同一事务同一片段号只能保留一份。
CREATE TABLE IF NOT EXISTS fragments(
    transaction_key TEXT NOT NULL,
    part INTEGER NOT NULL,
    payload TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    message_id TEXT,
    end INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    PRIMARY KEY(transaction_key,part)
);

-- 每次领取的凭据。handle 每次重投都会变化，旧 handle 只作为历史保留。
CREATE TABLE IF NOT EXISTS leases(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_key TEXT,
    part INTEGER,
    message_id TEXT,
    handle TEXT NOT NULL,
    raw TEXT,
    pulled_at TEXT NOT NULL,
    lease_until INTEGER,
    acked_at TEXT,
    ack_status TEXT,
    renewed_at TEXT,
    renewed_until INTEGER,
    note TEXT
);

-- 问题记录：同一片段号内容冲突、结束片段声明冲突、越界/无法解释的消息。
CREATE TABLE IF NOT EXISTS conflicts(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_key TEXT NOT NULL,
    part INTEGER NOT NULL,
    kind TEXT NOT NULL,
    existing_sha256 TEXT,
    incoming_sha256 TEXT,
    incoming_payload TEXT,
    incoming_handle TEXT,
    detail TEXT,
    created_at TEXT NOT NULL
);

-- 无法再补的缺口：来源不再提供该事务的缺片。
CREATE TABLE IF NOT EXISTS gaps(
    transaction_key TEXT NOT NULL,
    part INTEGER NOT NULL,
    reason TEXT NOT NULL,
    observed_handle TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    PRIMARY KEY(transaction_key,part)
);

CREATE INDEX IF NOT EXISTS leases_handle ON leases(handle);
CREATE INDEX IF NOT EXISTS leases_transaction ON leases(transaction_key);
CREATE INDEX IF NOT EXISTS leases_unacked ON leases(acked_at);
"""


def _id_text(value):
    # 身份键里整数与字符串被区分；展示与查询时统一成字符串。
    return str(value)


def _now_iso():
    return time.strftime('%Y-%m-%dT%H:%M:%fZ', time.gmtime())


class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)
        self.connection.execute("PRAGMA foreign_keys=ON")

    # ---- 单响应资料（兼容原有接口） -------------------------------------

    def save_response(self, receipt, response, rows):
        if len({r['key'] for r in rows}) != len(rows):
            raise ValueError('duplicate keys in response')
        with self.connection:
            self.connection.execute('INSERT OR IGNORE INTO receipts VALUES(?,?,?,?,?)',
                (receipt,response.url,response.status,json.dumps(response.headers),response.body))
            for row in rows:
                self.connection.execute('INSERT OR REPLACE INTO documents VALUES(?,?,?)',
                    (row['key'],json.dumps(row['value'],ensure_ascii=False),receipt))

    def documents(self):
        return [dict(key=k,value=json.loads(v),receipt=r) for k,v,r in self.connection.execute('SELECT key,value,receipt FROM documents ORDER BY key')]

    def receipt(self, identifier):
        row=self.connection.execute('SELECT url,status,headers,body FROM receipts WHERE id=?',(identifier,)).fetchone()
        return None if row is None else dict(url=row[0],status=row[1],headers=json.loads(row[2]),body=row[3])

    # ---- 事务 ----------------------------------------------------------

    def transaction(self, key):
        row = self.connection.execute(
            'SELECT * FROM transactions WHERE transaction_key=?', (key,)
        ).fetchone()
        return None if row is None else dict(row)

    def transactions(self, status=None):
        if status is None:
            rows = self.connection.execute(
                'SELECT * FROM transactions ORDER BY first_seen_epoch,transaction_key')
        else:
            rows = self.connection.execute(
                'SELECT * FROM transactions WHERE status=? ORDER BY first_seen_epoch,transaction_key',
                (status,))
        return [dict(r) for r in rows]

    def ensure_transaction(self, fragment):
        now = _now_iso()
        with self.connection:
            self.connection.execute(
                'INSERT OR IGNORE INTO transactions'
                '(transaction_key,device,epoch,transaction_id,first_seen_epoch,created_at,updated_at)'
                ' VALUES(?,?,?,?,?,?,?)',
                (fragment.transaction_key, str(fragment.device), str(fragment.epoch),
                 _id_text(fragment.transaction_id), int(time.time()), now, now),
            )

    def find_transaction(self, device, epoch, transaction_id):
        """按设备世代与事务号查找事务；``transaction_id`` 为整数或字符串都可。"""
        return self.transaction(transaction_key(device, epoch, transaction_id))

    def record_fragment(self, fragment):
        """保存一片原件。

        返回 ``new`` / ``duplicate`` / ``conflict`` / ``invalid``。冲突和越界
        内容不覆盖已保存的原件，只记问题，交由上层阻止发布。
        """
        key = fragment.transaction_key
        self.ensure_transaction(fragment)
        digest = fragment.digest
        now = _now_iso()
        with self.connection:
            transaction = self.connection.execute(
                'SELECT declared_parts,expected_sha256 FROM transactions WHERE transaction_key=?',
                (key,),
            ).fetchone()

            declared = transaction['declared_parts']
            if declared is not None and (fragment.part < 0 or fragment.part >= declared):
                self._record_conflict(key, fragment.part, 'out_of_range', None, digest,
                                      fragment.payload, fragment.receipt_handle,
                                      f'part {fragment.part} is outside declared parts {declared}', now)
                self._touch(key, now)
                return 'invalid'

            if fragment.end and declared is not None:
                expected = (fragment.parts, fragment.sha256)
                if (declared, transaction['expected_sha256']) != expected:
                    self._record_conflict(key, fragment.part, 'declaration', None, digest,
                                          fragment.payload, fragment.receipt_handle,
                                          'end fragment declares different parts or sha256', now)
                    self._touch(key, now)
                    return 'conflict'

            existing = self.connection.execute(
                'SELECT payload_sha256 FROM fragments WHERE transaction_key=? AND part=?',
                (key, fragment.part),
            ).fetchone()
            if existing is not None:
                if existing['payload_sha256'] == digest:
                    return 'duplicate'
                self._record_conflict(key, fragment.part, 'content',
                                      existing['payload_sha256'], digest,
                                      fragment.payload, fragment.receipt_handle,
                                      'same part delivered with different payload', now)
                self._touch(key, now)
                return 'conflict'

            self.connection.execute(
                'INSERT INTO fragments(transaction_key,part,payload,payload_sha256,message_id,end,created_at)'
                ' VALUES(?,?,?,?,?,?,?)',
                (key, fragment.part, fragment.payload, digest,
                 None if fragment.message_id is None else str(fragment.message_id),
                 1 if fragment.end else 0, now),
            )
            if fragment.end:
                self.connection.execute(
                    'UPDATE transactions SET declared_parts=?,expected_sha256=? WHERE transaction_key=?',
                    (fragment.parts, fragment.sha256, key),
                )
            self._touch(key, now)
        return 'new'

    def fragments(self, key):
        return [dict(r) for r in self.connection.execute(
            'SELECT part,payload,payload_sha256,message_id,end,created_at FROM fragments'
            ' WHERE transaction_key=? ORDER BY part', (key,))]

    def conflicts(self, key=None):
        if key is None:
            rows = self.connection.execute('SELECT * FROM conflicts ORDER BY id')
        else:
            rows = self.connection.execute(
                'SELECT * FROM conflicts WHERE transaction_key=? ORDER BY id', (key,))
        return [dict(r) for r in rows]

    # ---- 领取凭据 ------------------------------------------------------

    def record_lease(self, fragment, lease_until=None, raw=None):
        with self.connection:
            cursor = self.connection.execute(
                'INSERT INTO leases(transaction_key,part,message_id,handle,raw,pulled_at,lease_until)'
                ' VALUES(?,?,?,?,?,?,?)',
                (fragment.transaction_key, fragment.part,
                 None if fragment.message_id is None else str(fragment.message_id),
                 fragment.receipt_handle, raw, _now_iso(),
                 fragment.lease_until if lease_until is None else lease_until),
            )
            return cursor.lastrowid

    def record_unparsable(self, handle, message_id=None, *, raw=None, lease_until=None, note=None):
        """记录无法解释的消息：保留 handle 与原文，绝不确认。"""
        with self.connection:
            cursor = self.connection.execute(
                'INSERT INTO leases(handle,message_id,raw,pulled_at,lease_until,note) VALUES(?,?,?,?,?,?)',
                (handle, None if message_id is None else str(message_id), raw,
                 _now_iso(), lease_until, note),
            )
            return cursor.lastrowid

    def leases(self, key=None):
        if key is None:
            rows = self.connection.execute('SELECT * FROM leases ORDER BY id')
        else:
            rows = self.connection.execute(
                'SELECT * FROM leases WHERE transaction_key=? ORDER BY id', (key,))
        return [dict(r) for r in rows]

    def unacknowledged_handles(self):
        """仍有领取记录、尚未得到确认结果的 handle（按首次领取顺序去重）。"""
        return [r['handle'] for r in self.connection.execute(
            'SELECT handle FROM leases WHERE acked_at IS NULL GROUP BY handle ORDER BY MIN(id)'
        )]

    def mark_ack(self, handle, status):
        """把一次确认结果写到该 handle 的所有领取记录上。

        结果按 handle 记录，不按片段。``expired`` / ``unknown`` 只说明这次领取
        已不可用，不表示已保存的原件应当删除。
        """
        with self.connection:
            self.connection.execute(
                'UPDATE leases SET acked_at=?,ack_status=? WHERE handle=?',
                (_now_iso(), status, handle),
            )

    def note_lease(self, handle, note):
        with self.connection:
            self.connection.execute(
                'UPDATE leases SET note=? WHERE handle=? AND acked_at IS NULL',
                (note, handle),
            )

    def mark_renewed(self, handle, status, lease_until=None):
        with self.connection:
            self.connection.execute(
                'UPDATE leases SET renewed_at=?,renewed_until=?,note=?'
                ' WHERE handle=? AND acked_at IS NULL',
                (_now_iso(), lease_until, f'renew:{status}', handle),
            )

    # ---- 发布 ----------------------------------------------------------

    def publish(self, key):
        """发布一份完整事务。

        仅当全部片段齐备且摘要相符时调用。返回 ``(created, receipt)``：
        ``created`` 为 False 表示此前已发布，本次没有写入第二份签到。
        """
        transaction = self.transaction(key)
        if transaction is None:
            raise KeyError(key)
        state = self.evaluate(key, persist=False)
        if state['status'] not in ('ready', 'ack_pending', 'published'):
            raise ValueError(f'transaction {state["status"]} is not publishable')

        if transaction['published_receipt'] is not None:
            # 已发布：不重新拼装、不覆盖，避免迟到消息制造第二份签到。
            return False, transaction['published_receipt']

        declared = state['declared_parts']
        by_part = {f['part']: f for f in self.fragments(key)}
        payloads = [by_part[p]['payload'] for p in range(declared)]
        content = ''.join(payloads)
        digest = content_sha256(payloads)

        with self.connection:
            current = self.connection.execute(
                'SELECT published_receipt FROM transactions WHERE transaction_key=?', (key,)
            ).fetchone()
            if current['published_receipt'] is not None:
                return False, current['published_receipt']

            receipt = 'tx-' + hashlib.sha256(
                (key + '\0' + digest).encode('utf-8')).hexdigest()
            value = dict(
                kind='checkin',
                device=transaction['device'],
                epoch=transaction['epoch'],
                transaction_id=transaction['transaction_id'],
                parts=declared,
                sha256=transaction['expected_sha256'],
                content_sha256=digest,
                payload=content,
                part_sha256=[by_part[p]['payload_sha256'] for p in range(declared)],
                receipt=receipt,
            )
            self.connection.execute(
                'INSERT OR IGNORE INTO receipts VALUES(?,?,?,?,?)',
                (receipt, 'transaction:' + key, 200,
                 json.dumps({'parts': declared, 'sha256': transaction['expected_sha256']},
                            ensure_ascii=False),
                 content.encode('utf-8')),
            )
            self.connection.execute(
                'INSERT OR REPLACE INTO documents VALUES(?,?,?)',
                (key, json.dumps(value, ensure_ascii=False), receipt),
            )
            self.connection.execute(
                'UPDATE transactions SET published_receipt=?,note=NULL,updated_at=?'
                ' WHERE transaction_key=?',
                (receipt, _now_iso(), key),
            )
        self.evaluate(key)
        return True, receipt

    # ---- 缺口与放弃 ----------------------------------------------------

    def record_gap(self, key, part, reason, observed_handle=None):
        with self.connection:
            self.connection.execute(
                'INSERT OR IGNORE INTO gaps(transaction_key,part,reason,observed_handle,created_at)'
                ' VALUES(?,?,?,?,?)',
                (key, part, reason, observed_handle, _now_iso()),
            )

    def resolve_gap(self, key, part):
        with self.connection:
            self.connection.execute(
                'UPDATE gaps SET resolved_at=? WHERE transaction_key=? AND part=?',
                (_now_iso(), key, part),
            )

    def gaps(self, key=None):
        if key is None:
            rows = self.connection.execute(
                'SELECT * FROM gaps ORDER BY created_at,transaction_key,part')
        else:
            rows = self.connection.execute(
                'SELECT * FROM gaps WHERE transaction_key=? ORDER BY part', (key,))
        return [dict(r) for r in rows]

    def abandon(self, key, reason='source retention period ended',
                observed_handle=None):
        """来源已不再提供缺片：为每个缺口留记录，并标记事务不可补。

        已发布的事务不受影响；内容已完整只是确认未落地的仍保持待确认。
        """
        state = self.evaluate(key)
        if state is None:
            return state
        if state['status'] == 'published':
            # 已确认发布：来源保留期结束不影响已保存的签到。
            return state
        if state['status'] == 'ready':
            # 内容完整但尚未写入：直接发布，不把完整内容当作缺口。
            self.publish(key)
            return self.evaluate(key)
        missing = state['missing_parts']
        if missing is None:
            seen = state['collected_parts']
            missing = [p for p in range(max(seen) + 1) if p not in set(seen)] if seen else []
        for part in missing:
            self.record_gap(key, part, reason, observed_handle)
        if state['status'] == 'digest_mismatch':
            for part in state['collected_parts']:
                self.record_gap(key, part, reason, observed_handle)
        if state['status'] in ('conflict', 'invalid'):
            for part in state['collected_parts']:
                if state['declared_parts'] is None or part < state['declared_parts']:
                    self.record_gap(key, part, reason, observed_handle)
        with self.connection:
            self.connection.execute(
                'UPDATE transactions SET abandoned_at=?,abandoned_reason=?,updated_at=?'
                ' WHERE transaction_key=?',
                (_now_iso(), reason, _now_iso(), key),
            )
        return self.evaluate(key)

    # ---- 计算状态 ------------------------------------------------------

    def evaluate(self, key, *, persist=True):
        """计算事务的真实状态，解释它为什么能或不能发布。

        status:
        - ``waiting_parts``：缺片段，来源仍可能补发；
        - ``ready``：内容完整且摘要相符，尚未写入签到；
        - ``ack_pending``：内容完整、已保存为签到，仍有领取未被来源接受确认；
        - ``published``：内容完整且相关领取均已被来源接受确认；
        - ``unrecoverable``：缺片或内容不可用，且来源已不再提供；
        - ``conflict``：同一片段号内容冲突或结束声明冲突；
        - ``invalid``：出现越界片段；
        - ``digest_mismatch``：片段齐备但整份摘要不符。
        """
        transaction = self.transaction(key)
        if transaction is None:
            return None
        fragments = self.fragments(key)
        by_part = {f['part']: f for f in fragments}
        conflicts = self.conflicts(key)
        gaps = self.gaps(key)
        declared = transaction['declared_parts']

        acked_parts = {r['part'] for r in self.connection.execute(
            "SELECT DISTINCT part FROM leases WHERE transaction_key=? AND ack_status='accepted'"
            ' AND part IS NOT NULL', (key,))}
        unacked = [r['handle'] for r in self.connection.execute(
            'SELECT handle FROM leases WHERE transaction_key=? AND acked_at IS NULL'
            ' GROUP BY handle ORDER BY MIN(id),handle', (key,))]
        all_handles = [r['handle'] for r in self.connection.execute(
            'SELECT handle FROM leases WHERE transaction_key=? GROUP BY handle'
            ' ORDER BY MIN(id),handle', (key,))]

        extra = []
        missing = None
        digest = None
        if declared is not None:
            extra = sorted(p for p in by_part if p >= declared)
            missing = [p for p in range(declared) if p not in by_part]
            if not missing:
                payloads = [by_part[p]['payload'] for p in range(declared)]
                digest = content_sha256(payloads)

        if conflicts or extra:
            status = 'conflict' if conflicts else 'invalid'
        elif declared is None:
            status = 'waiting_parts'
        elif missing:
            open_missing = {g['part'] for g in gaps if g['resolved_at'] is None}
            status = 'unrecoverable' if set(missing) <= open_missing else 'waiting_parts'
        elif digest != transaction['expected_sha256']:
            status = 'digest_mismatch'
        elif transaction['published_receipt'] is None:
            status = 'ready'
        elif not unacked and set(range(declared)) <= acked_parts:
            status = 'published'
        else:
            status = 'ack_pending'

        # 已发布的签到不会因为迟到的冲突消息或来源保留期结束而被撤销；冲突仍在
        # conflicts 里可见，供排查，但状态以已保存的完整内容为准。
        if transaction['published_receipt'] is not None and status in (
                'conflict', 'invalid', 'digest_mismatch', 'waiting_parts',
                'unrecoverable', 'ready'):
            status = 'ack_pending' if unacked else 'published'

        if transaction['abandoned_at'] is not None and status in (
                'published', 'ack_pending', 'ready'):
            # 放弃记录只针对缺口；内容完整的事务不因来源保留期结束而失去已保存的签到。
            pass
        elif transaction['abandoned_at'] is not None:
            status = 'unrecoverable'

        if persist and transaction['status'] != status:
            self.connection.execute(
                'UPDATE transactions SET status=?,updated_at=? WHERE transaction_key=?',
                (status, _now_iso(), key),
            )
            self.connection.commit()

        open_gaps = [g for g in gaps if g['resolved_at'] is None]
        return dict(
            key=key,
            device=transaction['device'],
            epoch=transaction['epoch'],
            transaction_id=transaction['transaction_id'],
            status=status,
            data_saved=transaction['published_receipt'] is not None,
            published_receipt=transaction['published_receipt'],
            declared_parts=declared,
            expected_sha256=transaction['expected_sha256'],
            content_sha256=digest,
            collected_parts=sorted(by_part),
            missing_parts=missing,
            conflicts=conflicts,
            gaps=open_gaps,
            handles=all_handles,
            unacknowledged_handles=unacked,
            accepted_handles=[r['handle'] for r in self.connection.execute(
                "SELECT handle FROM leases WHERE transaction_key=?"
                " AND ack_status='accepted' GROUP BY handle ORDER BY MIN(id),handle", (key,))],
            note=transaction['note'],
        )

    def transaction_state(self, key):
        return self.evaluate(key)

    def transaction_states(self, status=None):
        states = [self.evaluate(r['transaction_key']) for r in self.transactions()]
        states = [s for s in states if s is not None]
        if status is not None:
            states = [s for s in states if s['status'] == status]
        return states

    def pending_transactions(self):
        """所有尚未被来源确认的事务状态（含缺片、待确认、冲突、不可补）。"""
        return [s for s in self.transaction_states() if s['status'] != 'published']

    # ---- 未确认消息 ----------------------------------------------------

    def unacknowledged(self, key=None):
        """列出仍未被确认的消息，并解释每一条为什么仍在等待。

        与事务状态结合：缺片时说明在等片段，内容已保存时说明在等确认结果，
        冲突/摘要不符/不可补时给出对应原因。
        """
        if key is None:
            rows = self.connection.execute(
                'SELECT * FROM leases WHERE acked_at IS NULL ORDER BY id').fetchall()
        else:
            rows = self.connection.execute(
                'SELECT * FROM leases WHERE acked_at IS NULL AND transaction_key=? ORDER BY id',
                (key,)).fetchall()
        states = {}
        result = []
        for row in rows:
            lease = dict(row)
            key = lease['transaction_key']
            if key is not None and key not in states:
                states[key] = self.evaluate(key)
            state = states.get(key)
            lease['transaction_status'] = None if state is None else state['status']
            lease['reason'] = self._unacked_reason(lease, state)
            lease['message'] = self._decode_raw(lease['raw'])
            lease.pop('raw', None)
            result.append(lease)
        return result

    @staticmethod
    def _decode_raw(raw):
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            return None

    @staticmethod
    def _unacked_reason(lease, state):
        if state is None:
            return 'unparsable_message'
        status = state['status']
        if status == 'published':
            return 'ack_result_missing'
        if status == 'ready':
            return 'content_ready_not_published'
        if status == 'ack_pending':
            return 'awaiting_ack_confirmation'
        if status == 'waiting_parts':
            return 'waiting_for_missing_parts'
        if status == 'unrecoverable':
            return 'source_no_longer_available'
        if status == 'conflict':
            return 'fragment_conflict'
        if status == 'digest_mismatch':
            return 'digest_mismatch'
        return 'invalid_fragment'

    # ---- 内部 ----------------------------------------------------------

    def _record_conflict(self, key, part, kind, existing, incoming, payload, handle, detail, now):
        duplicate = self.connection.execute(
            'SELECT 1 FROM conflicts WHERE transaction_key=? AND part=? AND kind=?'
            ' AND incoming_sha256 IS ? AND detail=?', (key, part, kind, incoming, detail),
        ).fetchone()
        if duplicate is not None:
            return
        self.connection.execute(
            'INSERT INTO conflicts(transaction_key,part,kind,existing_sha256,incoming_sha256,'
            'incoming_payload,incoming_handle,detail,created_at) VALUES(?,?,?,?,?,?,?,?,?)',
            (key, part, kind, existing, incoming, payload, handle, detail, now),
        )

    def _touch(self, key, now=None):
        self.connection.execute(
            'UPDATE transactions SET updated_at=? WHERE transaction_key=?',
            (now or _now_iso(), key),
        )

    def close(self):
        self.connection.close()
