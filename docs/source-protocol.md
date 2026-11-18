# 来源协议

这是独立合成的大厅资料接入项目，不包含个人资料或真实政务凭据。来源入口样例为 `/queue/pull`，所有相对链接以响应 URL 为基准。来源账号、访问范围和允许访问的 origin 由调用配置提供；真实密钥不应保存在资料中。

HTTP POST /queue/pull 接收 `{limit,lease_seconds}`；响应 `messages`。POST /queue/renew 接收 `handles` 与 `lease_seconds`；POST /queue/ack 接收 `handles`，逐 handle 返回 `accepted/expired/unknown`。租期由演练时钟 /clock 的整数秒决定，单次最大 30 秒。片段身份为 `device,epoch,transaction_id,part`，最后片段含 `end=true,parts,sha256`；摘要为按 part 顺序连接 UTF-8 payload 的 SHA256。/clock/advance 仅为本地来源操作。

## 消息与事务

- 一条设备事务由 `device`、`epoch`、`transaction_id` 一起确定。设备重置后 `transaction_id` 从头使用，因此只有设备世代（`epoch`）连同事务号才能确定身份；实现里三个字段一起构成事务键。
- 同一事务的片段可能乱序、重复。`part` 从 0 开始，`parts` 是总数，结束片段是 `part == parts-1`。
- 结束片段给出 `parts` 与整份 `sha256`。整份摘要按 part 顺序连接各片段的 UTF-8 `payload` 后计算，不包含任何分隔符。
- 同一事务同一 `part` 只有在 payload 完全相同时才算重复；内容不同视为冲突，已保存的原件不覆盖。
- 非结束片段即使携带 `parts`/`sha256` 也不参与“完整”判定，总片段数与摘要只认结束片段。
- `receipt_handle` 每次重投都会变化。确认只对某一次领取的 handle 有效；旧 handle 的 `expired`/`unknown` 只说明那次领取过期，不代表已保存的数据应删除。

## 采集语义

1. 领取到的消息先保存片段原件与领取凭据（handle、领取时间、租约到期、消息原文）。
2. 只有片段齐备且整份摘要相符时才写入签到；缺片、摘要不符或片段冲突时都不发布半份签到。
3. 写入签到之后才批量确认。确认响应丢失、消息在租约到期后重投，都不会生成第二份签到；已发布的事务再次收到片段只记录为重复或冲突。
4. 未完整保存前不确认任何 handle，避免确认掉来源里的唯一副本。
5. 缺片期间继续领取；租约临近到期时对未确认的 handle 续租，续租时长不超过来源的 30 秒上限。
6. 来源保留期结束后无法再取得缺片时，为每个缺口留下可定位记录（事务键、片段号、原因、观察到的 handle），并把事务标为来源已不可补。

## 状态口径

`Store.evaluate` / `acquire.service.transaction_status` 区分：

| 状态 | 含义 |
| --- | --- |
| `waiting_parts` | 缺片段，来源仍可能补发 |
| `ready` | 片段齐备且摘要相符，尚未写入签到 |
| `ack_pending` | 已写入签到，仍有领取未被来源接受确认（内容已保存） |
| `published` | 完整内容已保存，相关领取均已确认 |
| `conflict` | 同一片段号内容不同，或结束片段声明不一致 |
| `invalid` | 出现越界片段 |
| `digest_mismatch` | 片段齐备但整份摘要不符 |
| `unrecoverable` | 缺片或内容不可用，且来源已不再提供 |

`Store.unacknowledged` 列出仍未被确认的消息，并用 `reason` 解释每条为什么还在等待：`waiting_for_missing_parts`、`awaiting_ack_confirmation`、`content_ready_not_published`、`fragment_conflict`、`digest_mismatch`、`source_no_longer_available`、`ack_result_missing`、`unparsable_message`。`data_saved` 单独表示内容是否已经落地，不能把领取成功当作采集完成。

## 已有接口

acquire.transport.fetch 保留 HTTP 状态、原始响应头与实体字节，不自动跟随重定向。一次读取最多 max_bytes，超出时报错；当前默认超时 5 秒，已有单次调用语义继续有效。Response.header 大小写不敏感，sha256 只描述响应实体字节，不证明业务完整性。

acquire.decode.decode(body,headers) 返回 key/value 数组，当前只解释 fixtures/response.bin 中的一份响应。acquire.store.Store 在一个本地事务中保存响应与当前 key/value，并新增事务、片段、领取凭据、冲突与缺口表。acquire.service.collect_one 绑定 URL、原件及已解读记录，兼容这些单次操作的返回字段。

来源回放程序支持任意 HTTP 路径、请求体条件、响应序列及字节原件，配置不等同于完整的远端系统。缺失路由不可当成协议承诺；使用者可按上述协议补充来源行为进行联调。样本内容不限定实际输入规模或异常组合。
