# 签到设备拉取队列的租约与确认采集

Python 3.11+ 的授权来源读取项目。提供单响应读取、已有结构提取和本地资料保存，并为签到设备的拉取队列补齐了领取、续租、批量确认与事务状态查询；协议口径见 docs/source-protocol.md。使用标准库运行，不需要安装运行依赖。

## 本地运行

启动演练来源：`python -m acquire.origin [--scenario fixtures/queue.json]`，终端显示实际监听地址。该服务仅监听本机回环地址，记录实际 HTTP 请求，不连接政务系统。`fixtures/queue.json` 是一个完整事务（两个片段、结束片段带 `parts`/`sha256`）的队列示例，可直接联调领取、续租与确认。

一轮队列采集：

```
python -m acquire.service pull http://127.0.0.1:PORT/queue data.sqlite3 --limit 10
```

查询事务可用性、未完成事务和仍未确认的消息：

```
python -m acquire.service status data.sqlite3 '"kiosk-A","boot-9",1'   # 事务键
python -m acquire.service status data.sqlite3 --status waiting_parts
python -m acquire.service pending data.sqlite3
python -m acquire.service messages data.sqlite3
```

`acquire.service.collect(url, database, limit=10, lease_seconds=30, ...)` 跑一轮并把报告作为字典返回；`acquire.service.Collector(queue, store)` 可在同一存储上反复 `poll()`，并在来源保留期结束后用 `close_gaps()` 留下缺口记录。租约时长一律受来源的 30 秒上限约束。

`acquire.service.collect_one(url, database, method='GET', headers=None, body=None, max_bytes=1048576)` 保留原有单响应读取，用于非队列来源；`python -m acquire.service URL DATABASE [--method]` 仍然可用。来源状态非 2xx 时不写资料。

测试：`python -m unittest discover -s tests -v`。
编译检查：`python -m compileall -q acquire`。
如需构建 wheel，可先安装构建依赖再执行 `python -m pip wheel --no-deps .`；日常运行和测试不需要 wheel。

## 事务与状态

一条设备事务由 `device`、`epoch`、`transaction_id` 共同确定；设备重置后事务号会重新使用，只有设备世代连同事务号才唯一。片段按 `part` 保存原件，结束片段的 `parts`/`sha256` 决定整份内容是否完整：摘要按 part 顺序连接 UTF-8 payload 计算。乱序、重复的片段都能收纳；同一片段号内容不同则记冲突且不覆盖原件。

只有内容完整、摘要相符时才写入签到（`documents` 中一份 `kind=checkin` 的记录）。写入之后才批量确认 handle；确认响应丢失或消息重投不会生成第二份签到。尚未完整保存时绝不确认，避免确认掉来源里的唯一副本。`receipt_handle` 每次重投都会变化，旧 handle 的 `expired`/`unknown` 只表示那次领取过期，已保存原件不因此删除。

`Store.evaluate(key)` / `Store.transaction_states()` 返回 `waiting_parts`、`ready`、`ack_pending`、`published`、`conflict`、`invalid`、`digest_mismatch`、`unrecoverable`，并用 `data_saved`、`missing_parts`、`collected_parts`、`gaps` 说明细节。`Store.unacknowledged()` 对每条未确认消息给出 `reason`（等缺片、待确认、冲突、来源已不可补等），因此可以解释一条消息为什么仍在等待确认，而不是把领取成功当作采集完成。来源保留期结束后，`Collector.close_gaps()` 为每个缺口记下事务键、片段号、原因与当时观察到的 handle。

## 资料

Store.documents() 返回当前键值与收据；Store.receipt(identifier) 返回当时 URL、状态、响应头和原字节。当前同 key 的新资料覆盖旧资料；同一响应重复保存不会增加相同键。事务签到的 key 是事务键，receipt 是 `tx-<摘要>`，重复发布返回已有 receipt。
fixtures/response.bin 是合成来源原始响应，scenario.json 给出可修改的本地来源路由及响应序列。可通过 responses 数组按命中次数返回不同响应，require_headers/body_contains 检查交互上下文，body_base64 表达任意字节；没有匹配的请求返回 404。该回放服务不模拟未在 scenario 中配置的协议行为，复杂联调需要按协议增加对应交互或使用另一个可控来源，不能把回放缺少路由解释成真实业务结果。
