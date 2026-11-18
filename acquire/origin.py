"""可配置的本地 HTTP 来源；按请求条件返回合成响应并保存实际请求日志。

除了通用的静态路由回放，本来源还可以用 ``queue`` 配置演练拉取队列：
租约、重投、续租、批量确认与来源保留期都由 ``/clock`` 的演练时钟驱动。
未配置 ``queue`` 时行为与静态回放完全一致。
"""

import argparse
import base64
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from urllib.parse import urlsplit


class QueueState:
    """拉取队列的演练状态。

    ``messages`` 每条是一个片段消息（可带 ``available_until`` 表示来源保留到
    哪个演练时刻）。消息一经确认即从队列释放；租约到期则重新投递并换新
    handle。``ack_response`` 可为 ``normal`` / ``loss``（已接受但响应丢失）/
    ``error``（不处理）。
    """

    def __init__(self, spec):
        self.now = int(spec.get('now', 0))
        self.max_lease = int(spec.get('max_lease_seconds', 30))
        self.messages = [dict(m) for m in spec.get('messages', [])]
        self.ack_response = spec.get('ack_response', 'normal')
        self.lock = Lock()
        self.generation = 0
        self.acked = [False] * len(self.messages)
        self.lease_until = [None] * len(self.messages)
        self.handles = [None] * len(self.messages)

    def _index(self, handle):
        for index, current in enumerate(self.handles):
            if current == handle:
                return index
        return None

    def add(self, message):
        """向演练队列追加一条消息，便于分轮模拟来源延迟投递。"""
        with self.lock:
            self.messages.append(dict(message))
            self.acked.append(False)
            self.lease_until.append(None)
            self.handles.append(None)

    def _deliverable(self, index):
        message = self.messages[index]
        if self.acked[index]:
            return False
        expires = message.get('available_until')
        if expires is not None and self.now > int(expires):
            return False
        leased_until = self.lease_until[index]
        return leased_until is None or leased_until <= self.now

    def pull(self, limit, lease_seconds):
        with self.lock:
            lease_seconds = max(1, min(int(lease_seconds), self.max_lease))
            selected = []
            for index in range(len(self.messages)):
                if len(selected) >= limit:
                    break
                if not self._deliverable(index):
                    continue
                self.generation += 1
                handle = f"h{self.generation}"
                self.handles[index] = handle
                self.lease_until[index] = self.now + lease_seconds
                message = dict(self.messages[index])
                message['receipt_handle'] = handle
                message['lease_until'] = self.now + lease_seconds
                selected.append(message)
            return {'messages': selected}

    def renew(self, handles, lease_seconds):
        with self.lock:
            lease_seconds = max(1, min(int(lease_seconds), self.max_lease))
            result = []
            for handle in handles:
                index = self._index(handle)
                if index is None:
                    result.append({'handle': handle, 'status': 'unknown'})
                elif self.lease_until[index] <= self.now:
                    result.append({'handle': handle, 'status': 'expired'})
                else:
                    self.lease_until[index] = self.now + lease_seconds
                    result.append({'handle': handle, 'status': 'accepted',
                                   'lease_until': self.now + lease_seconds})
            return {'handles': result}

    def ack(self, handles):
        with self.lock:
            result = []
            for handle in handles:
                index = self._index(handle)
                if index is None:
                    result.append({'handle': handle, 'status': 'unknown'})
                elif self.lease_until[index] <= self.now:
                    result.append({'handle': handle, 'status': 'expired'})
                else:
                    result.append({'handle': handle, 'status': 'accepted'})
                    if not self.acked[index]:
                        self.acked[index] = True
            return {'handles': result}

    def advance(self, seconds):
        with self.lock:
            self.now += int(seconds)
            return self.now


def make_server(scenario, log=None):
    lock = Lock()
    visits = {}
    events = []
    queue_spec = scenario.get('queue')
    queue = QueueState(queue_spec) if queue_spec else None

    def queue_route(method, path, raw):
        if queue is None:
            return None
        if method == 'GET' and path == '/clock':
            return 200, {'Content-Type': 'application/json'}, {'now': queue.now}
        if method == 'POST' and path == '/clock/advance':
            try:
                payload = json.loads(raw or b'{}')
            except ValueError:
                payload = {}
            return 200, {'Content-Type': 'application/json'}, {
                'now': queue.advance(payload.get('seconds', 0))}
        if method == 'POST' and path in ('/queue/pull', '/queue/renew', '/queue/ack'):
            try:
                payload = json.loads(raw or b'{}')
            except ValueError:
                return 400, {'Content-Type': 'application/json'}, {'error': 'invalid json'}
            if path == '/queue/pull':
                body = queue.pull(payload.get('limit', 1), payload.get('lease_seconds', 30))
            elif path == '/queue/renew':
                body = queue.renew(payload.get('handles', []), payload.get('lease_seconds', 30))
            else:
                if queue.ack_response == 'error':
                    return 500, {'Content-Type': 'application/json'}, {'error': 'ack failed'}
                body = queue.ack(payload.get('handles', []))
                if queue.ack_response == 'loss':
                    # 确认已在来源落地，但响应丢失：消息不再重投，客户端看不到结果。
                    return 502, {'Content-Type': 'application/json'}, {'error': 'ack response lost'}
            return 200, {'Content-Type': 'application/json'}, body
        return None

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
            entry = dict(method=self.command, path=self.path,
                         headers=list(self.headers.items()),
                         body_base64=base64.b64encode(raw).decode())
            with lock:
                events.append(entry)
                if log:
                    with open(log, 'a', encoding='utf-8') as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                dynamic = queue_route(self.command, urlsplit(self.path).path, raw)
                if dynamic is not None:
                    status, headers, body_object = dynamic
                    payload = json.dumps(body_object, ensure_ascii=False).encode('utf-8')
                    headers = dict(headers)
                else:
                    matching = []
                    for index, route in enumerate(scenario.get('routes', [])):
                        if route.get('method', 'GET') != self.command or route['path'] != self.path:
                            continue
                        if any(self.headers.get(k) != v
                               for k, v in route.get('require_headers', {}).items()):
                            continue
                        if 'body_contains' in route and \
                                route['body_contains'] not in raw.decode('utf-8', 'replace'):
                            continue
                        matching.append((index, route))
                    if matching:
                        index, route = matching[0]
                        count = visits.get(index, 0)
                        responses = route.get('responses', [route])
                        response = responses[min(count, len(responses) - 1)]
                        visits[index] = count + 1
                    else:
                        response = dict(status=404, body='unmatched source request')
                    status = response.get('status', 200)
                    headers = response.get('headers', {})
                    if 'body_base64' in response:
                        payload = base64.b64decode(response['body_base64'])
                    else:
                        payload = response.get('body', '').encode('utf-8')
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.events = events
    server.visits = visits
    server.queue = queue
    return server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scenario', default='fixtures/scenario.json')
    parser.add_argument('--log', default='requests.jsonl')
    args = parser.parse_args()
    server = make_server(json.loads(Path(args.scenario).read_text(encoding='utf-8')), args.log)
    print(f'http://127.0.0.1:{server.server_port}', flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
