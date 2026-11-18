"""队列消息的片段身份、校验与整份内容摘要。

一条设备事务由 ``device``、``epoch``、``transaction_id`` 共同确定身份。设备重置后
``transaction_id`` 会重新使用，因此不能单独作为键。结束片段携带 ``parts`` 与
``sha256``，摘要按 part 顺序连接 UTF-8 payload 后计算。
"""

from dataclasses import dataclass
import hashlib
import json
import re

_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")

#: 单个事务允许声明的最大片段数，防止异常来源让缺片记录无限膨胀。
MAX_PARTS = 100000


def _scalar(value, field):
    """身份字段只接受非空字符串或整数，避免把列表/字典塞进身份键。"""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{field} must be a string or integer")
    if isinstance(value, str) and not value:
        raise ValueError(f"{field} must not be empty")
    return value


def transaction_key(device, epoch, transaction_id):
    """返回稳定且无歧义的事务键。

    使用 JSON 数组而不是字符串拼接，避免字段内容里的分隔符造成两个不同事务
    得到同一个键。``1`` 与 ``"1"`` 在来源的 JSON 里本就不是同一个值。
    """
    parts = [_scalar(device, "device"), _scalar(epoch, "epoch"),
             _scalar(transaction_id, "transaction_id")]
    # 索引字段按字符串归一：device/epoch 混用数字与字符串时仍稳定可查。
    return json.dumps([str(parts[0]), str(parts[1]), parts[2]],
                      ensure_ascii=False, separators=(",", ":"))


def content_sha256(payloads):
    """按 part 顺序连接 UTF-8 payload 后取 SHA256。"""
    digest = hashlib.sha256()
    for payload in payloads:
        digest.update(payload.encode("utf-8"))
    return digest.hexdigest()


@dataclass(frozen=True)
class Fragment:
    device: object
    epoch: object
    transaction_id: object
    part: int
    payload: str
    receipt_handle: str
    message_id: object = None
    end: bool = False
    parts: int | None = None
    sha256: str | None = None
    lease_until: int | None = None

    @property
    def transaction_key(self):
        return transaction_key(self.device, self.epoch, self.transaction_id)

    @property
    def digest(self):
        return hashlib.sha256(self.payload.encode("utf-8")).hexdigest()


def parse_message(message):
    """校验并解释一条拉取消息。

    无法确定事务身份、part 或领取凭据的消息会抛出 ``ValueError``；调用方不应
    确认这类消息，因为确认后来源会释放唯一副本。
    """
    if not isinstance(message, dict):
        raise ValueError("message must be an object")

    device = _scalar(message.get("device"), "device")
    epoch = _scalar(message.get("epoch"), "epoch")
    transaction_id_value = _scalar(message.get("transaction_id"), "transaction_id")

    part = message.get("part")
    if isinstance(part, bool) or not isinstance(part, int) or part < 0:
        raise ValueError("part must be a nonnegative integer")

    payload = message.get("payload")
    if not isinstance(payload, str):
        raise ValueError("payload must be a string")

    receipt_handle = message.get("receipt_handle")
    if not isinstance(receipt_handle, str) or not receipt_handle:
        raise ValueError("receipt_handle must be a nonempty string")

    message_id = message.get("message_id")
    if message_id is not None:
        _scalar(message_id, "message_id")

    end = message.get("end", False)
    if not isinstance(end, bool):
        raise ValueError("end must be a boolean")

    parts = message.get("parts")
    digest = message.get("sha256")
    if end:
        if isinstance(parts, bool) or not isinstance(parts, int) or parts < 1:
            raise ValueError("end fragment requires a positive integer parts")
        if parts > MAX_PARTS:
            raise ValueError("parts exceeds the supported fragment limit")
        if not isinstance(digest, str) or not _SHA256.match(digest):
            raise ValueError("end fragment requires a 64 character sha256")
        digest = digest.lower()
    else:
        # 非结束片段即使携带结束信息也不用于发布；总片段数与整份摘要只认结束
        # 片段，避免半份内容提前满足“完整”条件。
        if parts is not None and (
            isinstance(parts, bool) or not isinstance(parts, int) or parts < 1
        ):
            raise ValueError("parts must be a positive integer")
        if digest is not None and (
            not isinstance(digest, str) or not _SHA256.match(digest)
        ):
            raise ValueError("sha256 must be 64 hexadecimal characters")
        parts = None
        digest = None

    lease_until = message.get("lease_until")
    if lease_until is not None and (
        isinstance(lease_until, bool) or not isinstance(lease_until, int)
    ):
        raise ValueError("lease_until must be an integer")

    return Fragment(
        device=device,
        epoch=epoch,
        transaction_id=transaction_id_value,
        part=part,
        payload=payload,
        receipt_handle=receipt_handle,
        message_id=message_id,
        end=end,
        parts=parts,
        sha256=digest,
        lease_until=lease_until,
    )
