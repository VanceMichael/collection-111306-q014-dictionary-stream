import { resolve } from 'node:path';
import { Buffer } from 'node:buffer';
import { startSource } from './source.mjs';
import { CaptureStore } from './store.mjs';
import { preview, dictionaryDigest } from './domain.mjs';
import { DependencyReceiver } from './receiver.mjs';

/**
 * 一次真实接收演练：
 * 从本地源读取快照与压缩块，交给字典依赖接收器，
 * 先制造“字典缺席”的等待，再以迟到字典补齐并报告完成/等待/隔离，而不是回放旧日志。
 */
const source = await startSource();
try {
  const snapshot = await (await fetch(source.url + '/api/snapshot')).json();
  const observed = preview(snapshot); // 保留原有领域预览调用
  const capturePath = resolve(process.env.CAPTURE_PATH ?? 'var/captures.json');
  const store = new CaptureStore(capturePath);
  const receipt = await store.append({ kind: 'dictionary-stream', observed, at: new Date().toISOString() });

  const receiverPath = resolve(process.env.RECEIVER_PATH ?? 'var/receiver');
  const receiver = await new DependencyReceiver(receiverPath, { maxOutputLength: 65536 }).start();

  // 1) 先领取全部资料块（字典尚未提交给接收器），建立明确摘要关联与等待顺序。
  const fed = [];
  for (const row of snapshot.chunks) {
    const response = await fetch(`${source.url}/api/chunk/${row.id}`);
    const expectedDigest = response.headers.get('X-Dictionary-Digest');
    const epoch = Number(response.headers.get('X-Gateway-Epoch'));
    const payload = Buffer.from(await response.arrayBuffer());
    fed.push(await receiver.ingestBlock({
      gateway: snapshot.gateway, epoch, chunkId: row.id, expectedDigest, payload,
    }));
  }

  // 2) 提交一个摘要错误的字典，验证它被隔离且不会试解任何等待块。
  const wrong = Buffer.from('错误字典内容与源字典不符');
  const wrongResult = await receiver.ingestDictionary({ digest: observed.dictionary, payload: wrong });

  // 3) 迟到的正确字典到达，仅释放声明摘要匹配的块。
  const dictionary = Buffer.from(snapshot.dictionary);
  const correct = await receiver.ingestDictionary({ digest: dictionaryDigest(dictionary), payload: dictionary });

  const received = await receiver.listReceived();
  const status = receiver.status();
  console.log(JSON.stringify({
    资料源: source.url,
    预览接收文件: capturePath,
    预览记录版本: receipt.revision,
    接收器目录: receiverPath,
    块到达结果: fed.map(r => r.outcome),
    错误字典处理: wrongResult,
    正确字典释放: correct.released,
    正式接收: received.map(r => ({ key: r.key, record: r.record })),
    仍在等待: status.pending.map(p => p.key),
    隔离: status.quarantine.map(q => ({ kind: q.kind, reason: q.reason })),
    计数: status.counts,
  }, null, 2));
  await receiver.close();
} finally {
  await source.close();
}
