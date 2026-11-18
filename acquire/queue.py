"""拉取队列的来源交互：领取、续租、批量确认。

来源契约见 docs/source-protocol.md。这里只负责把请求送到来源并解释响应，
不决定何时确认；确认与否由采集流程在持久化完整内容之后决定。
"""

from dataclasses import dataclass
import json

from .transport import fetch


class SourceError(Exception):
    """来源返回了无法按协议解释的响应。"""


@dataclass(frozen=True)
class BatchResult:
    #: 来源逐 handle 返回的状态：accepted / expired / unknown
    statuses: dict
    #: 来源对新租约给出的到期时刻（仅续租有）
    lease_untils: dict
    #: 原始响应体，便于保存领取凭据的来源原话
    body: bytes

    def status(self, handle, default=None):
        return self.statuses.get(handle, default)

    def lease_until(self, handle, default=None):
        return self.lease_untils.get(handle, default)


def _json(response, expected=(200,)):
    if response.status not in expected:
        raise SourceError(f"source status {response.status}")
    try:
        return json.loads(response.body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise SourceError("source returned invalid JSON") from exc


def _post(url, payload, *, timeout=5, max_bytes=1048576):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return fetch(
        url,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        body=body,
        timeout=timeout,
        max_bytes=max_bytes,
    )


class QueueClient:
    """按来源契约访问拉取队列。

    ``base`` 是队列入口的 URL（样例为 `.../queue`）；领取固定使用
    ``/pull``、``/renew``、``/ack`` 三个相对路径。
    """

    #: 来源允许的单次租约上限（秒）。超过会被来源拒绝，也不能由调用方放大。
    MAX_LEASE_SECONDS = 30

    def __init__(self, base, *, timeout=5, max_bytes=1048576):
        base = base.rstrip("/")
        # 允许直接传来源入口（样例 /queue/pull），内部统一按队列根路径拼接。
        if base.endswith("/pull"):
            base = base[: -len("/pull")]
        self.base = base
        self.timeout = timeout
        self.max_bytes = max_bytes

    def _url(self, suffix):
        return self.base + suffix

    def lease_seconds(self, lease_seconds):
        """把请求的租约限制在来源上限内。"""
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int):
            raise ValueError("lease_seconds must be an integer")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        return min(lease_seconds, self.MAX_LEASE_SECONDS)

    def now(self, *, default=None):
        """读取来源演练时钟；读取失败时返回 ``default`` 或抛出 ``SourceError``。"""
        response = fetch(self._url("/clock"), timeout=self.timeout,
                         max_bytes=self.max_bytes)
        if not 200 <= response.status < 300:
            if default is not None:
                return default
            raise SourceError(f"source status {response.status}")
        try:
            value = json.loads(response.body)["now"]
        except (ValueError, KeyError, TypeError) as exc:
            if default is not None:
                return default
            raise SourceError("clock response requires integer now") from exc
        if isinstance(value, bool) or not isinstance(value, int):
            if default is not None:
                return default
            raise SourceError("clock now must be an integer")
        return value

    def pull(self, limit=1, lease_seconds=MAX_LEASE_SECONDS, *, require_ok=True):
        payload = {"limit": limit, "lease_seconds": self.lease_seconds(lease_seconds)}
        response = _post(self._url("/pull"), payload, timeout=self.timeout, max_bytes=self.max_bytes)
        if not 200 <= response.status < 300:
            if require_ok:
                raise SourceError(f"source status {response.status}")
            return response, []
        body = _json(response)
        messages = body.get("messages", [])
        if not isinstance(messages, list):
            raise SourceError("pull response messages must be a list")
        return response, messages

    def renew(self, handles, lease_seconds=MAX_LEASE_SECONDS):
        handles = list(handles)
        if not handles:
            return BatchResult({}, {}, b"")
        payload = {"handles": handles, "lease_seconds": self.lease_seconds(lease_seconds)}
        response = _post(self._url("/renew"), payload, timeout=self.timeout, max_bytes=self.max_bytes)
        body = _json(response)
        return self._batch(body, response.body, handles)

    def ack(self, handles):
        handles = list(handles)
        if not handles:
            return BatchResult({}, {}, b"")
        response = _post(self._url("/ack"), {"handles": handles}, timeout=self.timeout, max_bytes=self.max_bytes)
        body = _json(response)
        return self._batch(body, response.body, handles)

    @staticmethod
    def _batch(body, raw, handles):
        entries = body.get("handles")
        if not isinstance(entries, list):
            raise SourceError("batch response handles must be a list")
        statuses = {}
        lease_untils = {}
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("handle"), str):
                raise SourceError("batch response entry requires a handle")
            status = entry.get("status")
            if status not in ("accepted", "expired", "unknown"):
                raise SourceError(f"unexpected batch status {status!r}")
            statuses[entry["handle"]] = status
            until = entry.get("lease_until")
            if until is not None:
                if isinstance(until, bool) or not isinstance(until, int):
                    raise SourceError("lease_until must be an integer")
                lease_untils[entry["handle"]] = until
        return BatchResult(statuses, lease_untils, raw)
