import { resolve } from 'node:path';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { deflateSync } from 'node:zlib';
import { Buffer } from 'node:buffer';
import { DependencyReceiver, streamKey } from './receiver.mjs';
import { sha256 } from './receiver-store.mjs';

/**
 * 本地可重复的两台网关交错演练（不依赖网络与源端模拟器）：
 *  - g1/g2 的资料块交错到达，字典走另一路且迟到；
 *  - 错误摘要字典与损坏引用被隔离，不试解、不入库；
 *  - 短编号在重启后靠 epoch 复用；
 *  - 重送幂等、待解上限策略可查询、进程重启后等待与接收状态可恢复。
 */
const dictG1 = Buffer.from('检验记录|设备编号|温度|质量|合格');
const dictG1Epoch2 = Buffer.from('检验记录|设备编号|温度|质量|复核');
const dictG2 = Buffer.from('检验记录|设备编号|压力|流量|合格');
const dG1 = sha256(dictG1);
const dG1e2 = sha256(dictG1Epoch2);
const dG2 = sha256(dictG2);
const pack = (text, dict) => deflateSync(Buffer.from(text), { dictionary: dict });

function line(title) { console.log(`\n=== ${title} ===`); }
function show(result) { console.log(JSON.stringify(result)); }

export async function runDemo({ base = resolve(process.env.RECEIVER_PATH ?? 'var/receiver-demo') } = {}) {
  // 每次演示从空状态开始，保证结论来自本次真实运行而非旧日志。
  await rm(base, { recursive: true, force: true });

  line('阶段 1：两台网关的资料块交错到达，字典全部缺席，应进入等待并保留原始字节与到达顺序');
  let receiver = await new DependencyReceiver(base, {
    maxPending: 4,
    maxQuarantine: 8,
    maxEvents: 32,
    maxOutputLength: 4096,
  }).start();
  show(await receiver.ingestBlock({ gateway: 'g1', epoch: 1, chunkId: 'c1', expectedDigest: dG1, payload: pack('检验记录|设备编号=A1|温度=22|质量=合格', dictG1) }));
  show(await receiver.ingestBlock({ gateway: 'g2', epoch: 1, chunkId: 'c1', expectedDigest: dG2, payload: pack('检验记录|设备编号=B1|压力=0.8|流量=正常', dictG2) }));
  show(await receiver.ingestBlock({ gateway: 'g1', epoch: 1, chunkId: 'c2', expectedDigest: dG1, payload: pack('检验记录|设备编号=A2|温度=23|质量=合格', dictG1) }));
  show(await receiver.ingestBlock({ gateway: 'g2', epoch: 1, chunkId: 'c2', expectedDigest: dG2, payload: pack('检验记录|设备编号=B2|压力=0.9|流量=正常', dictG2) }));
  console.log('等待队列（按接收顺序）:', receiver.status().pending.map(p => `${p.order}. ${p.key}`));

  line('阶段 2：错误字典（摘要不匹配）与块的非法摘要引用，必须隔离且绝不试解');
  show(await receiver.ingestDictionary({ digest: dG1, payload: dictG2 })); // 声明 dG1 却发来 g2 字典
  show(await receiver.ingestBlock({ gateway: 'g1', epoch: 1, chunkId: 'cX', expectedDigest: 'bad-ref', payload: pack('x', dictG1) }));
  console.log('仍在等待的块数:', receiver.status().counts.waiting, '| 隔离条目:', receiver.status().quarantine.map(q => `${q.kind}:${q.reason}`));

  line('阶段 3：g1 正确字典迟到，仅释放声明匹配 dG1 的块；g2 块继续等待');
  show(await receiver.ingestDictionary({ digest: dG1, payload: dictG1 }));
  console.log('正式接收:', (await receiver.listReceived()).map(r => `${r.key} <- ${r.record}`));
  console.log('剩余等待:', receiver.status().pending.map(p => p.key));

  line('阶段 4：已处理块原样重送幂等忽略，不重复入库');
  show(await receiver.ingestBlock({ gateway: 'g1', epoch: 1, chunkId: 'c1', expectedDigest: dG1, payload: pack('检验记录|设备编号=A1|温度=22|质量=合格', dictG1) }));
  console.log('接收记录数仍为:', (await receiver.listReceived()).length);

  line('阶段 5：网关 g1 重启，短编号 c1 带新 epoch 与新摘要复用，不与旧记录混淆');
  show(await receiver.ingestBlock({ gateway: 'g1', epoch: 2, chunkId: 'c1', expectedDigest: dG1e2, payload: pack('检验记录|设备编号=A1|温度=22|质量=复核', dictG1Epoch2) }));
  console.log('等待中的新身份块:', receiver.status().pending.map(p => p.key));

  line('阶段 6：待解上限（maxPending=4，reject）— 超限明确拒绝并在事件中可查询，不悄悄丢最早块');
  // 当前等待：g2/c1、g2/c2、g1e2/c1 共 3 个，再喂 2 个使第 5 个触发拒绝。
  show(await receiver.ingestBlock({ gateway: 'g2', epoch: 1, chunkId: 'c3', expectedDigest: dG2, payload: pack('检验记录|设备编号=B3|压力=1.0|流量=正常', dictG2) }));
  show(await receiver.ingestBlock({ gateway: 'g2', epoch: 1, chunkId: 'c4', expectedDigest: dG2, payload: pack('检验记录|设备编号=B4|压力=1.1|流量=正常', dictG2) }));
  console.log('拒绝事件:', receiver.status().recentEvents.filter(e => e.type === 'block-rejected'));

  line('阶段 7：g2 正确字典迟到，按接收顺序释放其全部匹配块');
  show(await receiver.ingestDictionary({ digest: dG2, payload: dictG2 }));
  console.log('g2 正式接收顺序:', (await receiver.listReceived()).filter(r => r.gateway === 'g2').map(r => r.key));

  line('阶段 8：进程重启恢复——关闭后用同一目录重开，等待、隔离、接收与计数全部保留');
  await receiver.close();
  receiver = await new DependencyReceiver(base, { maxPending: 4, maxQuarantine: 8, maxEvents: 32, maxOutputLength: 4096 }).start();
  const before = receiver.status().counts;
  console.log('重启后计数:', before);
  // 重启后重送已接收块仍然幂等。
  show(await receiver.ingestBlock({ gateway: 'g2', epoch: 1, chunkId: 'c1', expectedDigest: dG2, payload: pack('检验记录|设备编号=B1|压力=0.8|流量=正常', dictG2) }));
  // 迟到的 g1 第二代字典补齐，释放唯一仍在等待的块。
  show(await receiver.ingestDictionary({ digest: dG1e2, payload: dictG1Epoch2 }));

  line('最终核对');
  const received = await receiver.listReceived();
  const status = receiver.status();
  console.log(JSON.stringify({
    正式接收数: received.length,
    接收明细: received.map(r => ({ key: r.key, arrivalSeq: r.arrivalSeq, record: r.record })),
    仍在等待: status.pending.map(p => p.key),
    隔离数: status.counts.quarantined,
    隔离原因: [...new Set(status.quarantine.map(q => q.reason))],
    拒绝数: status.counts.rejected,
    字典留存数: status.counts.dictionariesStored,
  }, null, 2));
  await receiver.close();
  return { received, status, base };
}

// 独立运行入口：使用临时目录，避免污染默认演示目录。
if (import.meta.url === `file://${process.argv[1]}`) {
  const base = await mkdtemp(`${tmpdir()}/receiver-demo-`);
  try { await runDemo({ base }); }
  finally { await rm(base, { recursive: true, force: true }); }
}
