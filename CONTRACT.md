# 设备轮询采集发布审查

这是供发布审查使用的完整小仓库，不是待填实现。protocol.py解释有限Modbus/TCP，transport.py管理请求，sampler.py维护采样轮次与发布；device.py提供受控设备，lab.py负责重放。docs/polling.md是尚未核准的维护说明，不能作为正确性依据。

## 本次公开业务契约

每轮读取寄存器0、1，最多两轮重叠。寄存器值是0～65535的整数，每个请求由设备处理时独立取值，不要求两值构成设备瞬时快照。响应只有连接内16位事务号、固定unit=1、功能及载荷；不回显寄存器地址、应用轮次号或尝试号。一请求只返回一次，连接内响应可乱序，字节流可靠且保持实际发送顺序。

每个发布值必须来自该轮仍有效的读取尝试；超时尝试的迟到响应不得成为后续尝试的结果，缺项或失败轮次不得发布成功。某次读取被超时扫描或连接关闭判为失败后，该轮不可再成为成功轮次；后续重新采集使用新轮次。失败记录只保留本轮有效完成的部分值，不能补入别轮缓存。最后已知值可以继续用于显示，但不能成为当前轮次成功的凭据。已发布成功轮次无需撤回。

同连接其他有效请求不能因一个请求超时而被误配；采取连接退役方案时须明确其重新读取和失败传播规则，不能永久停采。允许退役使该连接上未完成的轮次失败，随后新轮次重读两个寄存器；不得伪造原未决请求的成功。联合审查传输请求生命周期与批次成功资格。

比较连接退役隔离和保留连接但隔离未过迟到上界的事务号两种兼容方案，推荐其一，说明身份释放、重试接纳及批次封口条件。建议必须适配现有读取与轮次入口，写清跨组件通知和仍可显示的缓存与可发布值之间的边界。只要求本题有限范围内可执行级状态转移或伪代码，不研发通用调度器，不要求实现两种方案或证明任意协议。

## 时间与可观测性

clock返回单调整数tick，回放通过tick动作推进；读取期限为发送tick+5。expire动作是超时裁决事件：当时deadline≤当前tick的未决读取失效；未被裁决的请求按事件先后处理。等待超时是场景的显式输入，不依赖机器速度。

设备保证完整响应从请求完整发送至在客户端字节流中可读不超过50tick；这是到达上界，不保证立即派发。pause只暂停客户端解析出一帧之后的派发，网络和设备仍可继续；resume恢复。连接关闭后的旧响应不会进入新连接。所有场景都须满足设备上界，不能以违约迟到构造反例。无需处理响应重复、写寄存器、多设备、连接池或进程崩溃恢复。

set_cursor是已有测试入口，只改变下一个事务号候选，仍跳过未决编号；用于在有限场景中模拟游标走到复用边界，不改变线帧。pause/resume也是已有调度屏障。它们不是修复建议，不能删除或用其将所有请求串行化规避审查。

## 现有Python入口与结果

Client(reader,writer,connection,clock,trace)使用asyncio字节流；Client.connect(host,port,connection,clock,trace)使用TCP。await issue(address,timeout=5)返回Ticket，含本地token、tid、address、deadline、future；expire()通知超时；await close()退役连接。token不在设备响应里。set_cursor(n)、pause_dispatch()和resume_dispatch()供实验。

await Sampler.start(round_id,client)发出两个读取，返回Ticket列表；await wait(round_id)返回实际封口记录。记录含round、status、values、sources、errors；values以字符串地址为键，sources的connection/token是客户端自报归属，不是设备来源证明。publications保存实际成功记录。观察现状不代表符合上述业务契约。

async lab.replay(case,backend="memory")返回name、submitted、device、rounds、publications、trace，全部可JSON序列化。submitted把每轮地址关联到设备seq和客户端token；device按请求处理顺序记录seq、连接、tid、地址和当时取值。trace含设备发帧、客户端到达/派发与采样封口事件。设备seq只存在于实验记录，不在线上传输。evidence_gate.observed_origins(result)按实际发帧及接收顺序还原客户端每个完成值的真实设备seq，供报告核对；它不计算业务期望。

## 场景格式与运行

samples/polls.json是对象数组；每个对象含唯一name及actions数组。动作均按数组次序执行：
- values：values=[寄存器0值,寄存器1值]，改变设备后续请求的取值。
- start：round为新轮次名，可选cursor为0～65535整数；发出本轮两个请求并等设备实际处理。
- tick：at为不小于当前时刻的整数。
- expire：对当前连接执行超时扫描。
- reply：round和address定位已处理请求，发送其原始快照；可选error为Modbus异常码1～255。不能重复发送。
- pause、resume：暂停/恢复当前连接派发；resume的drain默认1，表示等待已暂停帧完成派发的数量。
- disconnect：设备断开当前连接并等客户端处理EOF。
- reconnect：关闭旧连接后建立新连接，保留同一Sampler对象和历史记录。
- wait：round指定要等待封口的轮次，加入rounds输出。

结束前须释放所有开放连接的设备响应并wait全部轮次；退役连接可用reply记录丢弃。暂停时不要连续reply多帧并等待第二帧已解析，单一接收循环会阻塞；resume后再发后续响应。两秒wait_for仅是实验死锁看门狗，不是业务时钟。

python3 -B checks.py normal核对乱序双轮、16位回绕、设备异常和新连接正常读取，成功打印NORMAL_POLL_OK。normal中a为10/100、b为20/200。python3 -B checks.py incident保留真实失败断言，期望new为22/220，当前失败含POLL_ORIGIN_MISMATCH。python3 -B lab.py samples/polls.json incident打印完整真实轨迹。

memory用真实MBAP字节帧、分块字节流与同一Client/Device处理代码；它不模拟客户端返回结果。lab.py末尾加tcp改用127.0.0.1临时端口及真实TCP，其余入口相同。默认回归使用memory，不依赖端口监听。服务与客户端都在本次调用中自动关闭。

## 审查交付和证据接口

只做代码审查与文档维护，保留protocol.py、transport.py、sampler.py、device.py、lab.py、checks.py、evidence_gate.py、样例及原断言，不实现生产修复。交付REVIEW.md并修订docs/polling.md，准确区分已证实现状、适用边界与建议行为。REVIEW.md须有源码路径及行号、具体触发时序、独立期望与实际结果、跨职责因果链、发布意见和两种方案的比较及推荐。

新增review_cases.json沿用场景格式，新增review_evidence.py与evidence.json。证据须调用原入口，覆盖迟到复用、部分轮次失败、另一有效读取、正常乱序、新连接隔离及已到达但暂未派发的边界；用单因和必要组合说明触发与掩盖，不能只复制incident。不得把生产修好后再称原实现无风险。

review_evidence.expected(case)是同步纯函数，只从计划、设备取值规则及事件顺序独立给出期望，不调用Client、Sampler或replay，不读客户端实际结果或已有evidence.json；只需支持交付场景，不要求通用预言机。返回按wait顺序排列的列表，每项恰含round、status、values、origins；status为success或failed，origins以字符串地址映射真实设备seq，只包含该轮有效完成值，失败轮也按同一原则保留部分值。

evidence.json顶层为{"cases":[...]}，与review_cases.json同序。每项含name、expected、actual、differences；actual为实际await lab.replay(case)的完整返回；expected为上述独立期望；differences以轮次名为键，记录状态、值或真实来源是否存在差异。report中可以有额外审查文字。直接运行review_evidence.py重新生成evidence.json；传入demo展示真实正常与异常批次及其请求归属。

python3 -B evidence_gate.py重跑基础正常回归与全部交付场景，比较实际重放、独立期望及差异标记与已保存报告，输出实际轮次；它不会替审查者证明期望语义或文档结论，仍须审查独立判据及定位。

.pointwise-runtime.json现有version=1、kind="cli"、setup为argv数组列表，verify/demo各为单个argv数组。本次保留该结构，把verify接到evidence_gate.py，把demo接到review_evidence.py demo，使核验重算已提交证据，演示显示真实结果。无需新增服务或安装依赖。
