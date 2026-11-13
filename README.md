# 字典压缩资料流的依赖接收与重放

边缘网关为节省流量把检验记录改成了字典压缩，资料块和字典走不同队列，有时字典晚半小时才到。这里保存现有资料预览工具和本地供方模拟服务，便于离线开发和复查领取行为。

## 当前能力

`npm run demo` 从实际本地资料源获取一次预览，将结果写入 `var/captures.json`，并打印接收文件和本次可见内容。它还不是业务批量接收器：当前只提供单次观察，尚未承诺以下业务边界。`src/domain.mjs` 保留已有领域预览函数；`src/store.mjs` 提供带完整性校验的单进程顺序接收文件，不是跨进程事务数据库。

## 运行

固定 Node.js 22.22.1，不依赖外部服务。所有 HTTP 资料源由进程在回环随机端口启动并关闭。

```sh
npm ci
npm run build
npm test
npm run demo
```

推荐使用固定镜像构建，首次构建会安装锁文件中的依赖；运行时不需联网。

```sh
docker build -t plant-dictionary-stream .
docker run --rm --network none --ipc=host plant-dictionary-stream npm test
docker run --rm --network none --ipc=host plant-dictionary-stream npm run demo
```

开发时 `npm start` 打印本地源地址；设置 `PORT` 可指定监听端口，未设置时自动分配回环端口。浏览器页面或 HTTP 接口可直接查看。`CAPTURE_PATH` 可改变接收文件路径。重复运行预览追加记录；删除 `var` 可从空接收状态开始。

## 模块

- `src/source.mjs`：该业务的本地资料源与演示控制，独立于待扩展的接收行为。
- `src/domain.mjs`：现有原始资料预览和领域数据结构。
- `src/preview.mjs`：读取、预览及持久化的一次运行入口。
- `fixtures/source.json` 与 `fixtures/controls.json`：本地供方资料及可切换情形。
- `docs/source.md`：接口和来源约定。
- `test`：现有预览、源端特殊情形、接收文件完整性测试。

## 业务核对

后续接收行为应使以下情况可被独立观察，而不仅是源端能够制造这些情况：

- 字典晚于资料到达。
- 短编号相同但摘要不同。
- 错误字典及解压上限。
- 暂存上限可解释。
- 正确字典到达仅释放匹配块。

测试只验证现有工具与源端行为，不代表新增业务能力已经交付。新增行为可在独立模块扩展，保留原有预览调用；禁止把源端模拟器改成替接收器完成核对。

## 短时操作演示

从空 `var` 运行 `npm run demo`，展示本次真实读取和接收路径；再运行 `npm test` 查看本地源边界与接收文件检查。固定环境中的这两步应在 90 秒内完成。业务功能扩展后同步更新该入口，使其展示实际完成情况、未确认部分及原因，不回放旧日志，不以源端控制操作代替接收结果。
