# 本地资料源约定

/api/chunk/:id 领取真实 zlib 字典压缩字节，X-Dictionary-Digest 指向内容摘要，X-Gateway-Epoch 给出重启代次。/api/dictionary 领取字典，delay 参数模拟迟到。源端允许在 restart 后复用相同块号。现有 decodePreview 只解单个指定字典块，没有依赖队列。

## 通用控制

`GET /api/snapshot` 查看当前原始源状态；`POST /api/action` 接收 JSON 对象 `{"action":"动作名"}`。可用动作：reset、restart。控制只改变模拟源，不代表接收器已经处理了变化。

所有资料均由本项目构造，包含的机构、设备、会话和文件仅供本地使用，不访问真实账号。浏览器操作应通过正常可见界面；测试用源控制接口不是业务采集捷径。

## 来源证据

源端请求记录只用于核查服务行为，接收器自身的审计、状态和输出需由扩展功能负责。源端重启会恢复默认资料，但本地接收文件不会自动清空。
