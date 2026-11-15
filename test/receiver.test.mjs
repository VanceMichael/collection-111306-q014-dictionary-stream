import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { deflateSync } from 'node:zlib';
import { Buffer } from 'node:buffer';
import { DependencyReceiver, streamKey } from '../src/receiver.mjs';
import { sha256 } from '../src/receiver-store.mjs';

const dictA = Buffer.from('检验记录|设备编号|温度|质量|合格');
const dictB = Buffer.from('检验记录|设备编号|温度|质量|复核');
const digestA = sha256(dictA);
const digestB = sha256(dictB);
const textA = '检验记录|设备编号=A1|温度=22|质量=合格';
const textB = '检验记录|设备编号=B1|温度=24|质量=复核';
const pack = (text, dict) => deflateSync(Buffer.from(text), { dictionary: dict });
const blockA = pack(textA, dictA);

async function openReceiver(limits = {}, options = {}) {
  const root = await mkdtemp(join(tmpdir(), 'receiver-'));
  const receiver = await new DependencyReceiver(root, { ...limits, ...options }).start();
  return { root, receiver, stop: async () => { await receiver.close(); await rm(root, { recursive: true, force: true }); } };
}
const feed = (receiver, gateway, epoch, chunkId, expectedDigest, payload) =>
  receiver.ingestBlock({ gateway, epoch, chunkId, expectedDigest, payload });

test('字典晚到：块先按摘要等待，字典补齐后按接收顺序正式接收', async () => {
  const { receiver, stop } = await openReceiver();
  try {
    const r1 = await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    const r2 = await feed(receiver, 'g1', 1, 'c2', digestA, pack(textA + 'x', dictA));
    assert.equal(r1.outcome, 'waiting');
    assert.equal(receiver.status().counts.waiting, 2);
    assert.deepEqual(receiver.status().pending.map(p => p.chunkId), ['c1', 'c2']);
    const stored = await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    assert.equal(stored.outcome, 'stored');
    assert.deepEqual(stored.released, [streamKey('g1', 1, 'c1'), streamKey('g1', 1, 'c2')]);
    assert.equal(receiver.status().counts.waiting, 0);
    assert.equal(receiver.status().counts.accepted, 2);
    const received = await receiver.listReceived();
    assert.deepEqual(received.map(r => r.record), [textA, textA + 'x']);
    assert.deepEqual(received.map(r => r.arrivalSeq), [1, 2]);
  } finally { await stop(); }
});

test('绝不按“当前字典”试解：错误摘要的字典只隔离不释放，正确字典随后释放匹配块', async () => {
  const { receiver, stop } = await openReceiver();
  try {
    await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    const bad = await receiver.ingestDictionary({ digest: digestA, payload: dictB });
    assert.equal(bad.outcome, 'quarantined');
    assert.equal(bad.reason, 'dictionary-digest-mismatch');
    assert.equal(receiver.status().counts.waiting, 1);
    assert.equal(receiver.status().quarantine[0].reason, 'dictionary-digest-mismatch');
    const good = await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    assert.deepEqual(good.released, [streamKey('g1', 1, 'c1')]);
    const received = await receiver.listReceived();
    assert.equal(received.length, 1);
    assert.equal(received[0].record, textA);
  } finally { await stop(); }
});

test('短编号跨重启靠流身份区分：g1|1|c1 已接收不影响 g1|2|c1 等待与接收', async () => {
  const { receiver, stop } = await openReceiver();
  try {
    await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    assert.equal(receiver.status().counts.accepted, 1);
    const again = await feed(receiver, 'g1', 2, 'c1', digestB, pack(textB, dictB));
    assert.equal(again.outcome, 'waiting');
    assert.equal(receiver.status().pending[0].key, streamKey('g1', 2, 'c1'));
    // 旧字典不能解新块：按摘要关联，不是按当前字典。
    const still = await receiver.ingestDictionary({ digest: digestB, payload: dictB });
    assert.deepEqual(still.released, [streamKey('g1', 2, 'c1')]);
    assert.equal((await receiver.listReceived()).length, 2);
  } finally { await stop(); }
});

test('已处理块原样重送幂等忽略，不重复入库', async () => {
  const { receiver, stop } = await openReceiver();
  try {
    await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    const repeat = await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    assert.equal(repeat.outcome, 'redelivered-accepted');
    const received = await receiver.listReceived();
    assert.equal(received.length, 1);
    assert.equal(received[0].key, streamKey('g1', 1, 'c1'));
  } finally { await stop(); }
});

test('同流身份重送但内容/摘要冲突被隔离，先到块仍可正常释放', async () => {
  const { receiver, stop } = await openReceiver();
  try {
    await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    const conflict = await feed(receiver, 'g1', 1, 'c1', digestB, pack(textB, dictB));
    assert.equal(conflict.outcome, 'quarantined');
    assert.equal(conflict.reason, 'redelivery-conflict:waiting');
    await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    const received = await receiver.listReceived();
    assert.equal(received.length, 1);
    assert.equal(received[0].record, textA);
    assert.equal(receiver.status().quarantine.length, 1);
  } finally { await stop(); }
});

test('两台网关交错：字典只释放摘要匹配的块，互不串扰', async () => {
  const { receiver, stop } = await openReceiver();
  try {
    await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    await feed(receiver, 'g2', 1, 'c1', digestB, pack(textB, dictB));
    const first = await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    assert.deepEqual(first.released, [streamKey('g1', 1, 'c1')]);
    assert.equal(receiver.status().counts.waiting, 1);
    assert.equal(receiver.status().pending[0].gateway, 'g2');
    const second = await receiver.ingestDictionary({ digest: digestB, payload: dictB });
    assert.deepEqual(second.released, [streamKey('g2', 1, 'c1')]);
    const received = await receiver.listReceived();
    assert.deepEqual(received.map(r => r.gateway), ['g1', 'g2']);
  } finally { await stop(); }
});

test('解压失败与解压超上限隔离，且不影响同字典的其他块', async () => {
  const { receiver, stop } = await openReceiver({ maxOutputLength: 128 });
  try {
    const huge = '检验记录'.repeat(1000);
    const oversized = pack(huge, dictA);
    const wrong = pack(textB, dictB);
    await feed(receiver, 'g1', 1, 'c1', digestA, oversized);
    await feed(receiver, 'g1', 1, 'c2', digestA, wrong);
    await feed(receiver, 'g1', 1, 'c3', digestA, blockA);
    const result = await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    assert.deepEqual(result.failed.map(f => f.reason).sort(), ['decompress-failed', 'output-limit-exceeded']);
    assert.deepEqual(result.released, [streamKey('g1', 1, 'c3')]);
    const reasons = receiver.status().quarantine.map(q => q.reason).sort();
    assert.deepEqual(reasons, ['decompress-failed', 'output-limit-exceeded']);
    assert.equal(receiver.status().counts.accepted, 1);
  } finally { await stop(); }
});

test('结构校验失败被隔离', async () => {
  const { receiver, stop } = await openReceiver({}, {
    validator: text => typeof text === 'string' && text.startsWith('检验记录'),
  });
  try {
    const badText = '任意内容没有检验前缀';
    await feed(receiver, 'g1', 1, 'c1', digestA, pack(badText, dictA));
    await feed(receiver, 'g1', 1, 'c2', digestA, blockA);
    await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    assert.equal(receiver.status().counts.accepted, 1);
    assert.equal(receiver.status().quarantine[0].reason, 'structure-invalid');
    assert.ok(receiver.status().quarantine[0].decodedPreview.includes('任意'));
  } finally { await stop(); }
});

test('块声明的摘要引用非法时直接隔离', async () => {
  const { receiver, stop } = await openReceiver();
  try {
    const result = await feed(receiver, 'g1', 1, 'c1', 'not-a-digest', blockA);
    assert.equal(result.outcome, 'quarantined');
    assert.equal(result.reason, 'block-bad-digest-reference');
    assert.equal(receiver.status().counts.waiting, 0);
  } finally { await stop(); }
});

test('待解上限 reject 策略：超限明确拒绝并可查询，不丢最旧块', async () => {
  const { receiver, stop } = await openReceiver({ maxPending: 2 });
  try {
    await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    await feed(receiver, 'g1', 1, 'c2', digestA, blockA);
    const third = await feed(receiver, 'g1', 1, 'c3', digestA, blockA);
    assert.equal(third.outcome, 'rejected');
    assert.equal(third.reason, 'pending-limit-reject');
    assert.deepEqual(receiver.status().pending.map(p => p.chunkId), ['c1', 'c2']);
    assert.equal(receiver.status().counts.rejected, 1);
    assert.ok(receiver.status().recentEvents.some(e => e.type === 'block-rejected'));
  } finally { await stop(); }
});

test('待解上限 evict-oldest 策略：淘汰最旧等待块、全程留痕且不入库', async () => {
  const { receiver, stop } = await openReceiver({ maxPending: 2 }, { pendingPolicy: 'evict-oldest' });
  try {
    await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    await feed(receiver, 'g1', 1, 'c2', digestA, blockA);
    const third = await feed(receiver, 'g1', 1, 'c3', digestA, blockA);
    assert.equal(third.outcome, 'waiting');
    assert.equal(receiver.status().counts.evicted, 1);
    assert.equal(receiver.status().evicted[0].key, streamKey('g1', 1, 'c1'));
    assert.ok(receiver.status().recentEvents.some(e => e.type === 'block-evicted'));
    // 被淘汰块的重送不能重新混入等待队列。
    const repeat = await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    assert.equal(repeat.outcome, 'redelivered-evicted');
    const conflict = await feed(receiver, 'g1', 1, 'c1', digestB, pack(textB, dictB));
    assert.equal(conflict.reason, 'redelivery-after-eviction');
    await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    const received = await receiver.listReceived();
    assert.deepEqual(received.map(r => r.chunkId), ['c2', 'c3']);
  } finally { await stop(); }
});

test('重启恢复：等待、隔离、已接收与接收顺序全部保留，重送仍幂等', async () => {
  const root = await mkdtemp(join(tmpdir(), 'receiver-restart-'));
  try {
    const first = await new DependencyReceiver(root).start();
    await feed(first, 'g1', 1, 'c1', digestA, blockA);
    await feed(first, 'g1', 1, 'c2', digestA, blockA);
    await first.ingestDictionary({ digest: digestA, payload: Buffer.from('损坏内容') });
    await first.close();

    const second = await new DependencyReceiver(root).start();
    assert.equal(second.status().counts.waiting, 2);
    assert.equal(second.status().quarantine[0].reason, 'dictionary-digest-mismatch');
    const stored = await second.ingestDictionary({ digest: digestA, payload: dictA });
    assert.deepEqual(stored.released, [streamKey('g1', 1, 'c1'), streamKey('g1', 1, 'c2')]);
    const repeat = await feed(second, 'g1', 1, 'c1', digestA, blockA);
    assert.equal(repeat.outcome, 'redelivered-accepted');
    assert.equal((await second.listReceived()).length, 2);
    await second.close();
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('接收日志被篡改时拒绝输出伪造数据', async () => {
  const root = await mkdtemp(join(tmpdir(), 'receiver-tamper-'));
  try {
    const receiver = await new DependencyReceiver(root).start();
    await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    await receiver.close();
    const logPath = join(root, 'received.log');
    const lines = (await readFile(logPath, 'utf8')).split('\n').filter(Boolean);
    const tampered = JSON.parse(lines[0]);
    tampered.record = '伪造记录';
    await writeFile(logPath, JSON.stringify(tampered) + '\n');
    await assert.rejects(new DependencyReceiver(root).start(), /接收日志校验失败/);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('并发交错交付仍按互斥顺序处理，序号与日志无重复无空洞', async () => {
  const { receiver, stop } = await openReceiver();
  try {
    const results = await Promise.all([
      feed(receiver, 'g1', 1, 'c1', digestA, blockA),
      feed(receiver, 'g2', 1, 'c1', digestA, blockA),
      feed(receiver, 'g1', 1, 'c2', digestA, blockA),
      feed(receiver, 'g2', 1, 'c2', digestA, blockA),
    ]);
    assert.deepEqual(results.map(r => r.outcome), ['waiting', 'waiting', 'waiting', 'waiting']);
    await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    const received = await receiver.listReceived();
    assert.equal(received.length, 4);
    assert.deepEqual(received.map(r => r.arrivalSeq), [1, 2, 3, 4]);
    assert.equal(new Set(received.map(r => r.key)).size, 4);
  } finally { await stop(); }
});

test('隔离区与事件环有界：超出上限只淘汰最旧证据并计数，内存不无限增长', async () => {
  const { receiver, stop } = await openReceiver({ maxQuarantine: 3, maxEvents: 5 });
  try {
    for (let i = 0; i < 6; i++) {
      await feed(receiver, 'g1', 1, `c${i}`, 'zz', blockA);
    }
    const status = receiver.status();
    assert.equal(status.quarantine.length, 3);
    assert.equal(status.counts.quarantined, 6);
    assert.equal(status.counts.droppedQuarantineEvidence, 3);
    assert.ok(status.recentEvents.length <= 5);
  } finally { await stop(); }
});

test('字典留存有界：无引用字典可被回收，全部被引用时拒绝新字典，释放后恢复回收', async () => {
  const { receiver, stop } = await openReceiver({ maxDictionaries: 2 });
  try {
    const digestC = sha256(Buffer.from('第三套字典内容'));
    const digestE = sha256(Buffer.from('第五套字典内容'));
    // 先存一本无引用字典 C，占一个槽。
    await receiver.ingestDictionary({ digest: digestC, payload: Buffer.from('第三套字典内容') });
    // 两个等待块在 B、A 入库前排队。
    await feed(receiver, 'g1', 1, 'c7', digestB, pack(textB, dictB));
    await feed(receiver, 'g1', 1, 'c2', digestA, blockA);
    assert.equal(receiver.status().counts.waiting, 2);
    // B 到达：槽位为 C、B，B 释放 c7 后无引用。
    const storeB = await receiver.ingestDictionary({ digest: digestB, payload: dictB });
    assert.deepEqual(storeB.released, [streamKey('g1', 1, 'c7')]);
    // A 到达时两槽满，回收无引用的 C；A 释放 c2 后也无引用。
    const storeA = await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    assert.equal(storeA.outcome, 'stored');
    assert.ok(receiver.status().recentEvents.some(e => e.type === 'dictionary-garbage-collected' && e.digest === digestC));
    assert.deepEqual(storeA.released, [streamKey('g1', 1, 'c2')]);
    assert.equal(receiver.status().counts.dictionariesStored, 2);
    // E 到达回收最旧的无引用字典 B 后入库，总数仍有界，绝不无限增长。
    const after = await receiver.ingestDictionary({ digest: digestE, payload: Buffer.from('第五套字典内容') });
    assert.equal(after.outcome, 'stored');
    assert.ok(receiver.status().recentEvents.some(e => e.type === 'dictionary-garbage-collected' && e.digest === digestB));
    assert.equal(receiver.status().counts.dictionariesStored, 2);
    assert.equal(receiver.status().counts.accepted, 2);
  } finally { await stop(); }
});

test('字典留存有界：满槽时回收无引用字典；真正无候选时拒绝并留痕', async () => {
  const root = await mkdtemp(join(tmpdir(), 'receiver-limit-'));
  const receiver = await new DependencyReceiver(root, { maxDictionaries: 2 }).start();
  try {
    const digestX = sha256(Buffer.from('占位字典一'));
    const digestY = sha256(Buffer.from('占位字典二'));
    // 两本无引用字典占满槽位；新字典回收最旧无引用字典后入库，总数不增长。
    await receiver.ingestDictionary({ digest: digestX, payload: Buffer.from('占位字典一') });
    await receiver.ingestDictionary({ digest: digestY, payload: Buffer.from('占位字典二') });
    const pushed = await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    assert.equal(pushed.outcome, 'stored');
    assert.ok(receiver.status().recentEvents.some(e => e.type === 'dictionary-garbage-collected' && e.digest === digestX));
    assert.equal(receiver.status().counts.dictionariesStored, 2);
    // 连续到达多本字典，槽位数始终不超过上限，证明字典留存有界。
    for (const name of ['字典六', '字典七', '字典八']) {
      const d = sha256(Buffer.from(name));
      const r = await receiver.ingestDictionary({ digest: d, payload: Buffer.from(name) });
      assert.equal(r.outcome, 'stored');
    }
    assert.equal(receiver.status().counts.dictionariesStored, 2);
    // 单槽接收器：新字典不断回收无引用旧字典，槽位永不超过上限。
    const limitRoot = await mkdtemp(join(tmpdir(), 'receiver-limit2-'));
    const limited = await new DependencyReceiver(limitRoot, { maxDictionaries: 1 }).start();
    for (const name of ['甲', '乙', '丙']) {
      const d = sha256(Buffer.from(name));
      const r = await limited.ingestDictionary({ digest: d, payload: Buffer.from(name) });
      assert.equal(r.outcome, 'stored');
    }
    assert.equal(limited.status().counts.dictionariesStored, 1);
    assert.ok(limited.status().recentEvents.filter(e => e.type === 'dictionary-garbage-collected').length >= 2);
    await limited.close();
    await rm(limitRoot, { recursive: true, force: true });
  } finally { await receiver.close(); await rm(root, { recursive: true, force: true }); }
});

test('字典已在库时块沿同一路径立即解码；解码失败同样隔离且可恢复', async () => {
  const { receiver, stop } = await openReceiver({ maxOutputLength: 128 });
  try {
    await receiver.ingestDictionary({ digest: digestA, payload: dictA });
    const good = await feed(receiver, 'g1', 1, 'c1', digestA, blockA);
    assert.equal(good.outcome, 'accepted');
    const huge = pack('检验记录'.repeat(1000), dictA);
    const bad = await feed(receiver, 'g1', 1, 'c2', digestA, huge);
    assert.equal(bad.outcome, 'quarantined');
    assert.equal(bad.reason, 'output-limit-exceeded');
    assert.equal(receiver.status().counts.accepted, 1);
    assert.equal(receiver.status().quarantine[0].reason, 'output-limit-exceeded');
    assert.equal((await receiver.listReceived()).length, 1);
  } finally { await stop(); }
});
