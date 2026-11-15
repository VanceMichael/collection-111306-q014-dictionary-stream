# 本地资料源约定

/api/chunk/:id 领取真实 zlib 字典压缩字节，X-Dictionary-Digest 指向内容摘要，X-Gateway-Epoch 给出重启代次。/api/dictionary 领取字典，delay 参数模拟迟到。源端允许在 restart 后复用相同块号。现有 decodePreview 只解单个指定字典块，没有依赖队列。

## 接收器依赖约定（src/receiver.mjs）

接收器不依赖源端的“当前字典”，只信任块自身声明的摘要：

- 资料流身份为 `(gateway, epoch, chunkId)`，与块声明的字典 sha256 摘要共同决定依赖；重启后复用短编号由 epoch 区分。
- 字典到达时重新计算 sha256 并与声明摘要比对，不一致即隔离为 `dictionary-digest-mismatch`，绝不参与解压。
- 解压使用 `maxOutputLength` 限制，超限（`ERR_BUFFER_TOO_LARGE`）隔离为 `output-limit-exceeded`，其他解压错误隔离为 `decompress-failed`，结构校验失败隔离为 `structure-invalid`。
- 待解块上限策略 `reject`（默认）或 `evict-oldest`，均在 `status()` 与事件环中可查询；隔离证据、字典留存与事件环都有界。
- 状态写入 `state.json`（校验快照，原子替换），正式接收写入只增的 `received.log`（每行自带 sha256）；重启后以日志为权威来源恢复。

## 通用控制

`GET /api/snapshot` 查看当前原始源状态；`POST /api/action` 接收 JSON 对象 `{"action":"动作名"}`。可用动作：reset、restart。控制只改变模拟源，不代表接收器已经处理了变化。

所有资料均由本项目构造，包含的机构、设备、会话和文件仅供本地使用，不访问真实账号。浏览器操作应通过正常可见界面；测试用源控制接口不是业务采集捷径。

## 来源证据

源端请求记录只用于核查服务行为，接收器自身的审计、状态和输出需由扩展功能负责。源端重启会恢复默认资料，但本地接收文件不会自动清空。
