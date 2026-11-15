# 字典压缩资料流的依赖接收与重放

边缘网关为节省流量把检验记录改成了字典压缩，资料块和字典走不同队列，有时字典晚半小时才到。接收器在字典缺席时不会停住所有设备，也不会拿“当前字典”试解后入库：块按明确的字典摘要与资料流身份关联，原始压缩内容与接收顺序持久保留，字典补齐后只释放有充分依据的块。

## 当前能力

- `src/receiver.mjs`：字典依赖接收器 `DependencyReceiver`，把“等依赖 → 解码 → 结构校验 → 正式接收”连成可恢复的串行过程。
- `src/receiver-store.mjs`：校验状态快照（原子写 + fsync）与只增、每行自带 sha256 的正式接收日志。
- `src/demo.mjs`：本地两台网关交错演练（独立运行 `npm run demo:receiver`），覆盖迟到字典、错误字典、短编号跨重启复用、重送幂等、待解上限与重启恢复。
- `npm run demo` 仍从真实本地源读取并保留 `domain.preview` 预览，同时把源块交给接收器，报告正式接收、仍在等待与隔离原因。
- `src/domain.mjs` 保留原有领域预览函数；`src/store.mjs` 仍是带完整性校验的单进程顺序接收文件，不是跨进程事务数据库。

### 依赖与隔离规则

- 关联键是资料流身份 `(gateway, epoch, chunkId)` 加块**明确声明**的字典 sha256 摘要；短编号在网关重启后靠 `epoch` 区分，绝不按当前字典猜测。
- 错误/损坏字典（摘要不匹配）、块的非法摘要引用、解压失败、解压后超过 `maxOutputLength`、结构校验不通过的内容一律进入隔离区，只保留截断证据，不试解、不入库。
- 待解块、字典、隔离证据、事件环都有显式上限，不会无界占用内存。
- 待解资料到达上限时策略可查询：默认 `reject`（明确拒绝并写事件），或配置 `pendingPolicy: 'evict-oldest'`（淘汰最旧等待块、留痕且永不入库），不会悄悄丢掉最早一块。
- 已正式接收的块重送幂等忽略；同流身份的冲突重送、淘汰后重送都隔离并注明原因。
- 所有变更经互斥串行化落校验快照，正式接收另写只增日志；进程重开后等待、隔离、接收与计数全部恢复，并以只增日志为正式接收的权威来源纠正陈旧快照。

## 运行

固定 Node.js 22.22.1，不依赖外部服务。所有 HTTP 资料源由进程在回环随机端口启动并关闭。

```sh
npm ci
npm run build
npm test
npm run demo
npm run demo:receiver
```

推荐使用固定镜像构建，首次构建会安装锁文件中的依赖；运行时不需联网。

```sh
docker build -t plant-dictionary-stream .
docker run --rm --network none --ipc=host plant-dictionary-stream npm test
docker run --rm --network none --ipc=host plant-dictionary-stream npm run demo
```

开发时 `npm start` 打印本地源地址；设置 `PORT` 可指定监听端口，未设置时自动分配回环端口。浏览器页面或 HTTP 接口可直接查看。`CAPTURE_PATH` 可改变预览接收文件路径，`RECEIVER_PATH` 可改变接收器状态目录。删除 `var` 可从空接收状态开始。

## 模块

- `src/receiver.mjs`：字典依赖接收器，负责依赖关联、等待/隔离/上限策略、解码与结构校验、幂等正式接收和恢复。
- `src/receiver-store.mjs`：校验状态快照与只增正式接收日志。
- `src/demo.mjs`：两台网关交错、错误字典与迟到正确字典的本地可重复演练。
- `src/source.mjs`：该业务的本地资料源与演示控制，独立于接收行为。
- `src/domain.mjs`：现有原始资料预览和领域数据结构。
- `src/preview.mjs`：读取本地源、预览并驱动接收器的一次运行入口。
- `fixtures/source.json` 与 `fixtures/controls.json`：本地供方资料及可切换情形。
- `docs/source.md`：接口和来源约定。
- `test`：接收器业务边界、预览、源端特殊情形与接收文件完整性测试。

## 接收器查询接口

`DependencyReceiver` 实例提供：

- `ingestBlock({ gateway, epoch, chunkId, expectedDigest, payload })`：交付压缩块，返回 `waiting` / `accepted` / `quarantined` / `rejected` 或各类 `redelivered-*`。
- `ingestDictionary({ digest, payload })`：交付字典，校验摘要后返回 `stored`（含 `released` / `failed`）、`quarantined`、`duplicate-dictionary` 或 `rejected`。
- `status()`：返回待解队列（含顺序与字节）、隔离区（含原因与截断证据）、淘汰条目、有界事件环与各项计数。
- `listReceived()`：读取只增正式接收日志，按到达顺序返回记录。

## 业务核对

以下情况现在由接收器独立观察，而不仅是源端能够制造：

- 字典晚于资料到达：块保持等待并保留原始字节与到达顺序，字典补齐后按序释放。
- 短编号相同但摘要不同：靠 `(gateway, epoch, chunkId)` 流身份区分，重启复用不串档。
- 错误字典、错误摘要引用、解压失败与解压上限：全部隔离并注明原因，不试解、不入库。
- 暂存上限可解释：`reject` 或 `evict-oldest` 显式可查询，事件环留痕，不静默丢块。
- 正确字典到达仅释放声明摘要匹配的块，其他网关/代次的块继续等待。
- 已接收块重送幂等；进程重启后等待、隔离、接收状态与计数可恢复。

源端模拟器保持原样，不替接收器完成核对；接收器在独立模块实现，原有预览调用保留。

## 短时操作演示

从空 `var` 运行 `npm run demo`，展示对真实本地源的本次接收、仍在等待部分及隔离原因；运行 `npm run demo:receiver` 查看两台网关交错、错误字典与迟到正确字典的完整演练；再运行 `npm test` 查看接收器业务边界、本地源边界与接收文件检查。固定环境中的这些步骤应在 90 秒内完成。两个入口都展示本次实际完成情况、未确认部分及原因，不回放旧日志，不以源端控制操作代替接收结果。
