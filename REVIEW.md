# 设备轮询采集发布审查（REVIEW）

审查对象：`protocol.py`、`transport.py`、`sampler.py`、`device.py`、`lab.py`、`checks.py`、`evidence_gate.py` 及 `docs/polling.md`。
本轮只做代码审查与文档维护：生产源码、样例、原断言均未修改，未实现修复。`docs/polling.md` 已按本文结论修订；它原稿不能作为正确性依据。

证据入口（均调用原入口）：

```text
python3 -B checks.py normal          # NORMAL_POLL_OK，原正常回归
python3 -B checks.py incident        # 仍以原断言失败（POLL_ORIGIN_MISMATCH）
python3 -B review_evidence.py        # 重新生成 evidence.json（11 个交付场景）
python3 -B review_evidence.py demo   # 展示真实批次、设备 seq 与请求归属
python3 -B evidence_gate.py          # 重跑正常回归+交付场景并重算核对
python3 -B lab.py review_cases.json late_reuse tcp   # 真实 TCP 复核（可选）
```

`review_evidence.expected(case)` 是同步纯函数，只依据场景动作、设备“请求处理时取值”规则与事件顺序；不导入/调用 Client、Sampler、replay，也不读实际结果或已存 evidence.json。`evidence.json` 的 `actual` 是完整 `await lab.replay(case)` 返回，`differences` 按轮次比较状态、值与 `evidence_gate.observed_origins` 还原的真实设备 seq。

---

## 1. 结论与发布意见

**不建议核准当前实现用于“超时后发布值归属”场景，也不建议按原 `docs/polling.md` 运维。**

存在两项独立的发布阻塞缺陷：

- **A. 事务号在迟到窗口内被复用，旧迟到响应被新尝试认领**，导致发布值来自错误采样轮次。`late_reuse`、`paused_arrival` 均复现；`late_no_reuse` 证明复用是触发条件。
- **B. Sampler 用上一成功轮缓存播种本轮 values，再以“键数==2”判成功**，导致缺项/超时/连接退役轮次被旧值补成 `success` 并写入 `publications`。`partial_failure_cache`、`total_failure_cache`、`retire_after_success` 均复现；`cold_failure`、`retire_pending_round` 证明“无旧缓存”会掩盖该缺陷。

两项缺陷叠加时，A 提供错值、B 提供“两个地址都有值”的资格，最坏结果是错误值与旧值混合后成功发布。原 `incident` 的真实结果 `new={"0":11,"1":220}`（期望 `22/220`）正是 A 的直接证据；其 `old` 轮 `values={"1":110}` 虽为失败轮部分值，保留方式本身符合“失败轮只保留本轮有效完成部分值”。

发布门槛（修复后应满足，本轮不实现）：交付场景全部 `differences` 为 `false`，`checks.py normal` 保持通过，`incident` 在改为契约期望后通过；在此之前不得把当前行为描述为满足契约。设备写入、多设备、连接池、崩溃恢复不在本审查范围。

---

## 2. 缺陷 A：超时释放的事务号被下一轮复用，迟到响应跨轮误配

### 2.1 源码事实与生命周期

- `transport.py:51-57`：`issue()` 选 tid 时**只跳过 `self.pending` 中的编号**，不检查该编号是否仍在迟到窗口内。
- `transport.py:58-65`：`Ticket` 的 `token` 为本地自增序号，`deadline = clock()+5`；写出请求后即返回。响应里没有 token（`protocol.py:26-29` 只写 tid 与载荷）。
- `transport.py:68-73`：`expire()` 对 `deadline <= now` 的 tid 执行 `pending.pop(tid)` 并给 future 置 `TimeoutError`，**不保留墓碑**。
- `transport.py:81-101`：接收循环先 `read_adu`、记录 `wire_seen`，随后在 `dispatch_gate` 之后 `pending.pop(tid)` 认领（`88-100`）。认领键只有 tid；`unclaimed` 只发生在“派发时 pending 里没有该 tid”。
- `transport.py:87`：`pause_dispatch()` 清 gate 后，帧可已经到达并被解析（`84-86`），但认领被推迟到 `resume`。

因此，tid 的“身份”只由当前 pending 集合临时维持；一旦超时把它移出 pending，它立刻回到候选池，而设备侧旧响应仍可能按 50tick 上界在途。

### 2.2 具体时序（`late_reuse`；与 `samples/polls.json` 的 incident 同构）

```text
t=0   values=[11,110]；start old cursor=7
      tid7 addr0 deadline=5；tid8 addr1 deadline=5
      device seq0(tid7,addr0,11), seq1(tid8,addr1,110)
t=0   reply old addr1 → seq1 到达 → answer token1=110
t=6   tick 6；expire：tid7 deadline5<=6，pending.pop(7)，old future=TimeoutError
t=6   wait old → failed，values={"1":110}（部分值，正确）
t=6   values=[22,220]；start new cursor=7
      tid7 不在 pending → 被复用给 new addr0（deadline=11）
      tid8 被复用给 new addr1
      device seq2(tid7,addr0,22), seq3(tid8,addr1,220)
t=6   reply old addr0 → 设备释放 seq0(value=11)，客户端 wire_seen tid7 后按 tid 认领 new token2
      → answer token2=11（真实来源 seq0，属于 old 轮）
t=6   reply new addr1 → seq3 → token3=220
t=6   reply new addr0 → seq2(value=22) 到达时 pending 已无 tid7 → unclaimed
t=6   wait new → success，values={"0":11,"1":220}
```

| | 独立期望（契约） | 实际（重放） |
|---|---|---|
| old | failed，`{"1":110}`，origins `{"1":1}` | failed，`{"1":110}`，origins `{"1":1}` |
| new | success，`{"0":22,"1":220}`，origins `{"0":2,"1":3}` | success，`{"0":11,"1":220}`，origins `{"0":0,"1":3}` |
| publications | new | new（发布值 11/220，跨轮混值） |

`checks.py:33-39` 保留的原事故断言即期望 `new={0:22,1:220}`，实际得到 `{'0':11,'1':220}`，失败信息 `POLL_ORIGIN_MISMATCH`；该失败在本轮被保留，未修生产码。

### 2.3 到达边界而非派发边界（`paused_arrival`）

```text
t=0   old 的 seq0/seq1；reply old addr1 → 110
t=6   expire old tid7；wait old → failed {"1":110}
t=6   values=[22,220]；pause（gate 清除）
t=6   reply old addr0 → seq0(value=11) 已写入客户端字节流；接收循环读到并 wire_seen tid7，
      卡在 dispatch_gate；此时旧响应到达时刻距发送仅 6tick，满足 50tick 到达上界
t=51  tick 51；start new cursor=7 → tid7/tid8 被新轮复用；设备 seq2/seq3 取值 22/220
t=51  resume drain=1 → gate 放行；先派发的却是 t=6 已到达的旧 seq0，pending 中 tid7 现为 new
      → answer token2=11
t=51  new addr1/addr0 的 seq3=220、seq2=22 随后到达；seq2 因 tid7 已被占用而 unclaimed
t=51  new → success {"0":11,"1":220}（期望 22/220）
```

该场景把“迟到上界约束到达、不约束即时派发”（`CONTRACT.md:19`，`transport.py:87`）与“按派发时 pending 认领”结合，证明仅靠“上界过去后重用”这条维护说明并不充分：响应可以在上界内到达，却在上界后被派发。若隔离方案只看发送时刻+50 而不看到达标记，此场景仍会误配。

### 2.4 单因与掩盖控制

- `late_no_reuse`：其余动作完全相同，只让新轮从 cursor=9 取 tid9/10。旧 seq0 到达时 pending 无 tid7 → `unclaimed`；new 得到 `22/220`，origins `2/3`，无差异。说明**tid 复用是直接原因**，不是“迟到响应本身”或“两轮重叠”。
- `other_valid_read`：a 的 addr0 在 t6 超时，b 在 t2 发送（deadline7）且 tid 不同。a failed 仅保留 `{"1":50}`，b success `6/60`，origins 正确。说明同连接其他有效请求在无 tid 冲突时不会被一个超时误配；但**有冲突时现有实现不能保证**。
- `new_connection_isolation`：旧连接 conn0 的 seq0 在 `reconnect` 后被 `Device.release` 判为 `delivered=false`（`device.py:74-76`，会话已关闭），字节不进入 conn1；新连接 tid7/8（`connection=1`）拿到 `22/220`。说明连接代际本身可隔离。
- 真实 TCP 复核：`python3 -B lab.py review_cases.json late_reuse tcp` 同样得到 `new={"0":11,"1":220}`；`new_connection_isolation tcp` 同样得到 `22/220`。该缺陷不是 memory 后端的产物。

---

## 3. 缺陷 B：缓存播种把缺项/失败轮次判成成功并发布

### 3.1 源码事实

- `sampler.py:22`：`_finish` 第一行 `values = dict(self.cache)`，把上一成功轮的两个值预先放入本轮结果。
- `sampler.py:26-32`：失败 outcome 只记 `errors`，**不删除**缓存里对应的地址键；成功 outcome 覆盖该地址。
- `sampler.py:33`：`status = "success" if len(values) == 2 else "failed"`，判定的是“字典键数”，不是“本轮两个 ticket 是否都成功”。
- `sampler.py:39-41`：status 为 success 时更新 `self.cache` 并 `publications.append(record)`；失败轮不更新缓存，但缓存仍在。
- `sampler.py:36-38`：`sources` 是客户端自报 `connection/token`，不是设备来源证明（`CONTRACT.md:27`）；否则 `evidence_gate.observed_origins` 不会发现 stale origin。

### 3.2 部分失败（`partial_failure_cache`）

```text
base：10/100 成功 → cache={0:10,1:100}，publications=[base]
values=[20,200]；start loss → tid2 addr0, tid3 addr1；device seq2=20, seq3=200
reply loss addr0 error=2 → 设备返回 0x83/2，response_value 抛 ValueError
reply loss addr1 → 200 成功
_finish：values 从 cache 起手 {0:10,1:100}；addr1 覆盖为 200；addr0 失败但键仍在
status：len({0,1})==2 → success
record：values={0:10,1:200}，errors=[addr0 ValueError]，sources 的 addr0=token0（base 轮）
publications 追加 loss
```

| | 独立期望 | 实际 |
|---|---|---|
| base | success `10/100` origins `0/1` | 同左 |
| loss | failed，仅 `{"1":200}`，origins `{"1":3}` | success `{"0":10,"1":200}`，origins `{"0":0,"1":3}` |
| publications | 仅 base | base、loss |

`errors` 明确记录 addr0 失败，但 status 仍 success，直接推翻原维护说明“errors 只是运维诊断信息”“两个位置均有值即可发布 success”。

### 3.3 全部失败（`total_failure_cache`）

`base` 成功后，`gone` 的两个 tid 均超时（`expire` 后 future 为 TimeoutError）。`_finish` 的 values 仍是 `{0:10,1:100}`，两个 address 都“有值”，于是 success 并发布 **base 的旧值**，`sources` 仍指向 base 的 token0/1。独立期望为 failed、`values={}`、origins `{}`。这说明失败轮不仅“缺项被补”，还可以在**没有任何本轮有效完成值**时发布成功。

### 3.4 连接退役失败被缓存掩盖（`retire_after_success`）

`base` 成功；`cut` 发出两读后 `reconnect`。`Client.close()` 走 `transport.py:108-113` → `_fail("connection retired")`（`75-79`），两个 pending future 均为 ConnectionError，`_finish` 的 `errors` 确实是两条 ConnectionError——**传输层失败了**。但缓存播种使 values 仍是 `{0:10,1:100}`，`cut` 被 sealed 为 success 并进入 publications。独立期望 `cut` failed、`values={}`、origins `{}`。

这推翻原维护说明“传输层已经处理失败，因此发布层无需另设退役规则”：传输失败只到 future，是否发布成功由 Sampler 的 `len(values)==2` 决定，二者之间的资格判定缺失。

### 3.5 单因与掩盖控制

- `cold_failure`：首轮无缓存，addr0 设备异常、addr1 成功；结果 failed，仅 `{"1":30}`，origins `{"1":1}`，不发布。同一异常在无缓存时正确封口，证明**缓存播种是假成功的必要条件**。
- `retire_pending_round`：无前置成功缓存，`cut` 退役 → failed、`values={}`；新连接 `fresh` 重读 0/1 → success `20/200`，origins `2/3`。证明退役通知与重连重读本身是通的；缺陷在“发布成功资格”而不是“传输没有失败”。
- `normal_out_of_order`：两轮乱序应答、tid 从 65534 回绕，均 success 且 origins 正确，`publications` 按封口顺序为 b、a（顺序不是契约错误；已发布成功无需撤回）。作为正常基线，说明 A/B 不是所有路径都触发。

---

## 4. 跨职责因果链

1. **超时裁决**（`transport.py:68-73`）只解除 future 与 pending 关系，未保留“该 tid 的旧响应尚未过迟到窗口”的状态。
2. **编号分配**（`transport.py:51-57`）把该 tid 立即视为可复用；设备仍在 50tick 到达上界内（`device.py:29,75-76`）。
3. **响应认领**（`transport.py:84-100`）以**派发时**的 tid 为唯一键，把旧载荷交给新 Ticket；旧 future 已 done，不会再被写，所以“旧轮不会成功”成立，但“新轮读到旧值”发生。
4. **轮次封口**（`sampler.py:22,33`）用缓存补齐缺失地址，把“一个地址是旧轮错值、另一个是本轮值”的混合结果判成 success。
5. **发布**（`sampler.py:39-41`）写入 publications；`sources` 自报 token 看似属于本轮，`observed_origins` 才能还原真实设备 seq（`evidence_gate.py:10-26`）。
6. **运维说明**（原 `docs/polling.md:7,9,13,17`）把“超时移除 pending”“上界过去即可重用”“两位置有值即成功”“传输处理失败即可”写成保证，掩盖了第 1-5 步的断点。

---

## 5. 两种兼容方案比较与推荐

两种方案都必须同时修 Sampler 的发布资格（第 3 节），否则退役/隔离再正确，`retire_after_success` 仍会假成功。以下只做有限范围内可执行的状态转移/伪代码，不研发通用调度器。

### 方案 A：超时即连接退役（connection retirement）

状态转移（`Client`）：

```text
expire()/任何读取超时:
  pending 中到期票 → future=TimeoutError；将其所在连接标记 retired
retire(connection):
  _fail 所有 pending（ConnectionError）；关闭 writer/接收任务；连接代际 +1
issue():
  retired 连接拒绝；调用方先重连
接收:
  旧连接字节不进入新连接（字节流边界天然隔离）
```

- **身份释放**：以连接代际为身份边界；新连接游标从头开始，旧 tid 在新连接没有 pending，不可能匹配。无需 50tick 墓碑。
- **重采**：退役后由连接所有者建立新连接，新的 `Sampler.start(round_id, new_client)` 重读地址 0、1；旧未决请求不得重发后宣称成功，旧轮不得复活。
- **失败传播**：退役使该连接上未完成轮次全部 failed；`_finish` 只保留本轮有效完成的部分值；已成功封口且已发布的轮次不撤回。
- **批次封口**：失败轮不得因缓存或任何旧值补成 success；新轮使用新 round id；最多两轮重叠的约束不变。
- **优点**：隔离彻底，连 `paused_arrival` 这类“已到达未派发”也随旧连接丢弃；实现点集中，`Client.close()` 已有失败路径（`transport.py:75-79,108-113`）。
- **代价/风险**：一个超时会让同连接另一有效轮次也失败（契约允许，但可用性下降）；需要跨组件通知“重连”到一个拥有连接工厂的角色（当前 `Sampler.start` 不接受工厂，`lab.py:43-47` 直接持有 client）；必须明确重试接纳，避免连续退役造成永久停采；`new_connection_isolation` 已验证旧字节不会泄漏。

### 方案 B：保留连接，隔离未过迟到上界的事务号（recommended）

状态转移（`Client`，不改变线协议）：

```text
Ticket 元数据: token（本地唯一）, tid, address, deadline, release_at
expire():
  pending.pop(tid)；future=TimeoutError（该轮不可能再成功）
  tombstone[tid] = {address, token, release_at = sent_tick + 50, arrived=False}
issue(address):
  候选 tid 必须 not in pending 且 not in tombstone（扩展 transport.py:51-57 的跳过条件）
接收循环（read_adu 后、gate 前，对应 transport.py:84-86）:
  若 tid in tombstone: tombstone[tid].arrived = True；该帧携带“旧代”标记
  gate 放行后（transport.py:87-100）:
  若帧带旧代标记 → unclaimed，丢弃；绝不交给任何 pending（包括新 ticket）
  否则按 tid 交给 pending（现有路径）
tid 释放（回到候选池）:
  clock() >= release_at 且该 tid 没有“已到达但未派发”的帧（该帧被丢弃后清除）
  （若 bound 已到但帧已到达且仍卡在 gate，继续隔离；派发并丢弃后再释放）
```

- **身份释放**：future 在 expire 已定局；tombstone 不持有可完成的 future，token 不重用；tid 仅在“上界已过且无未处置到达帧”后释放，因此新 ticket 不可能与旧帧同 tid 共存。
- **到达标记而非派发标记**：`_receive` 在 gate 前已读到帧（`transport.py:84`），把“旧代”标记绑定到帧到达时刻；即使 `resume` 发生在 50tick 之后，`paused_arrival` 的旧 seq0 也会被丢弃，new 得到 22/220。
- **重采/重试接纳**：下一轮在同一连接 `start`，`issue` 自动跳过隔离 tid，可能拿到不同编号；其他有效请求继续（`other_valid_read` 保持）。若某轮失败，使用新 round id 重读两寄存器；只要持续尝试就不算永久停采。pending 至多 4 个 tid；隔离墓碑只保留到 `发送tick+50`，数量由“最近 50tick 内失败的轮次数×2”界定，远小于 65536，不会触发 `transport.py:56-57` 的耗尽分支。
- **批次封口**：与方案 A 相同——只有本轮两个 ticket 都成功才 success；失败轮仅保留本轮有效完成值；缓存只显示；publications 只收 success。
- **跨组件通知**：全部在 transport 内完成；Sampler 仍通过 future 异常看到失败，无需新增连接工厂或改变 `issue`/`start` 签名，只在 `_finish` 去掉缓存播种并按“本轮 ticket 成功数”判定。
- **风险**：若只按“发送+50”时间隔离而忽略到达标记，pause 跨过上界的帧仍会误配（`paused_arrival` 是反例）；若把 50tick 当成派发上界也会误判。必须实现“到达即标记、派发前判丢弃”。

### 推荐

**推荐方案 B**，理由：

1. 直接消除根因（tid 在迟到窗口内复用），同时保留同连接其他有效读取，符合“其他有效请求不得误配”；
2. 不需要把重连/连接工厂引入 Sampler 的入口，跨组件通知最小，适配现有 `Client.issue`、`Sampler.start`、`Ticket` 字段与 `set_cursor`（只需把跳过条件从 pending 扩展到隔离 tid）；
3. 能覆盖 `CONTRACT.md:19` 的“到达上界、非派发上界”和 `paused_arrival` 边界；
4. 与 `new_connection_isolation` 不冲突：连接退役仍是更强隔离，可作为某些部署的退路。

方案 A 可作为退路：若连接层无法保存隔离状态，或部署选择“宁失败不混值”，退役更简单；但必须同时修 Sampler 资格，并明确重连所有权、失败传播和重试不停止。两种方案都不应通过删除 `pause/resume`、串行化所有请求或依赖响应里的地址/轮次来规避审查。

---

## 6. 身份、重采与部分批次失败规则（建议行为）

- **身份**：`token` 是客户端本地唯一身份，`tid` 是连接内身份；响应只有 16 位 tid，地址必须由客户端请求表维护。超时后 `token` 不得复用，`tid` 不得在迟到窗口内复用；旧帧不得写入任何当前 ticket。
- **重采**：失败/退役轮以新 `round_id` 重新读取地址 0、1；不得把旧未决请求的响应当作新轮结果；不得补入别轮缓存。重试受“最多两轮重叠”约束，但不得永久停采。
- **部分批次失败**：任一地址失败即该轮 failed；`values`/`sources` 只保留该轮有效完成的部分值（失败轮也按此原则）；`errors` 表明失败，不是仅诊断；失败轮不发布、不更新缓存。
- **成功封口**：两个地址都来自本轮有效 ticket 的完成值才算 success；此时才更新缓存（供显示）并追加 publications。已发布成功不撤回。
- **显示与发布边界**：`cache` 可以继续供界面显示最后已知值；`publications` 与 `values` 不得引用缓存值。`sources` 只是自报归属，核对真实来源须用 `observed_origins`。

---

## 7. 交付证据与核验

`review_cases.json`（11 个场景）与 `evidence.json` 同序：

| 场景 | 目的 | 独立期望 → 实际差异 |
|---|---|---|
| normal_out_of_order | 正常乱序、16 位回绕基线 | 无差异（a/b 均 success 且 origins 正确） |
| late_reuse | 超时后 tid 复用、迟到响应被新轮认领 | new 差异：期望 22/220，实际 11/220（origin 0） |
| late_no_reuse | 同场景不复用 tid 的对照 | 无差异（证明复用是单因） |
| retire_pending_round | 退役失败传播、重连重读 | 无差异（cut failed，fresh 成功） |
| retire_after_success | 退役失败被旧缓存掩盖 | cut 差异：期望 failed/空，实际 success/旧值 |
| cold_failure | 无缓存时同一异常正确失败 | 无差异（证明缓存是必要条件） |
| partial_failure_cache | 部分失败轮被缓存补成 success | loss 差异：期望 failed/仅 1:200，实际 success/10、200 |
| total_failure_cache | 全失败轮被旧值补成 success | gone 差异：期望 failed/空，实际 success/旧值 |
| other_valid_read | 超时请求与另一有效读取共存 | 无差异（b 正确；a 部分失败） |
| paused_arrival | 上界内到达、上界后派发 | new 差异：期望 22/220，实际 11/220 |
| new_connection_isolation | 新连接隔离旧响应 | 无差异（旧 seq0 delivered=false，新轮 22/220） |

`evidence_gate.py` 会重放全部交付场景、重算独立期望与真实 origins、比对已保存 evidence.json，并打印实际轮次；它不会替审查者证明期望语义。`.pointwise-runtime.json` 已把 `verify` 接到 `evidence_gate.py`、`demo` 接到 `review_evidence.py demo`，结构仍为 version=1/kind=cli/setup=[]。

已知限制：交付证据默认 memory 后端（同一字节帧与处理代码），另对 `late_reuse`、`new_connection_isolation` 做了真实 TCP 复核；未扩展设备写入、多设备、连接池或崩溃恢复，也不要求实现两种方案。

---

## 8. 兼容建议

- **不改线协议**：响应继续只有 tid、unit=1、功能与载荷（`protocol.py:26-36`）；不要为修复而回显地址、轮次或尝试号，也不要依赖 `sources` 作为设备来源证明。
- **保留入口与字段**：`Client.issue(address, timeout=5)`、`Ticket.token/tid/address/deadline/future`、`Sampler.start/wait`、记录字段 `round/status/values/sources/errors` 保留；方案 B 只在 Client 内增加墓碑/隔离状态，并把 `issue` 的跳过条件扩展为 pending + 隔离。
- **保留观测事件**：`wire_seen`、`unclaimed`、`answer`、`timeout`、`disconnected` 是 `observed_origins` 还原真实来源的依据，不要删除或改名。
- **缓存语义收紧**：`cache` 继续服务显示；`publications` 只收本轮两地址都成功的记录；`values` 不再从缓存起手。
- **入口兼容**：`set_cursor` 仍只改下一 tid 候选并跳过不可用编号；`pause/resume` 保留为实验屏障；`disconnect/reconnect` 仍使旧连接未完成轮失败且旧字节不进入新连接。
- **回归门槛**：`checks.py normal` 必须保持 `NORMAL_POLL_OK`；`checks.py incident` 的原始失败断言在本轮保留，作为真实事故证据，不得删除；文档修订不得把建议行为写成已实现。
