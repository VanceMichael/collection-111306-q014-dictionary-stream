import { inflateSync } from 'node:zlib';
import { Buffer } from 'node:buffer';
import { Checkpoint, ReceivedLog, sha256 } from './receiver-store.mjs';

const HEX64 = /^[0-9a-f]{64}$/;
const EVIDENCE_BYTES = 256;

export const DEFAULT_LIMITS = Object.freeze({
  maxPending: 1000,            // 待解资料块上限
  maxDictionaries: 64,         // 已留存字典个数上限
  maxQuarantine: 500,          // 隔离区条目上限（超出后最旧证据被淘汰并计数）
  maxEvents: 1000,             // 可查询事件环上限
  maxDictionaryBytes: 1 << 20, // 单个字典字节上限
  maxBlockBytes: 1 << 20,      // 单个压缩块字节上限
  maxOutputLength: 65536,      // 解压后字节上限
});

export function streamKey(gateway, epoch, chunkId) {
  return `${gateway}|${epoch}|${chunkId}`;
}

function defaultValidator(text) {
  return typeof text === 'string' && text.trim().length > 0 && !text.includes('�');
}

function asBuffer(payload, label) {
  if (Buffer.isBuffer(payload)) return payload;
  if (payload instanceof Uint8Array || payload instanceof ArrayBuffer) return Buffer.from(payload);
  throw new TypeError(`${label} 必须是 Buffer/Uint8Array/ArrayBuffer`);
}

/**
 * 字典依赖接收器。
 *
 * 关联依据：资料流身份 (gateway, epoch, chunkId) + 块明确声明的字典 sha256 摘要。
 * 短编号在网关重启后靠 epoch 区分；绝不按“当前字典”猜测解压。
 * 所有变更串行化并落校验快照，正式接收另写只增日志，进程重启后可恢复。
 */
export class DependencyReceiver {
  constructor(directory, options = {}) {
    this.directory = directory;
    this.config = { ...DEFAULT_LIMITS, ...options };
    this.pendingPolicy = options.pendingPolicy === 'evict-oldest' ? 'evict-oldest' : 'reject';
    this.validator = options.validator ?? defaultValidator;
    if (!Number.isInteger(this.config.maxPending) || this.config.maxPending < 1) throw new TypeError('maxPending 必须为正整数');
    this.checkpoint = new Checkpoint(`${directory}/state.json`);
    this.log = new ReceivedLog(directory);
    this.mutex = Promise.resolve();
    this.closed = false;
  }

  async start() {
    const saved = await this.checkpoint.load();
    const received = await this.log.readAll();
    this.sequence = 0;
    this.eventSeq = 0;
    this.counters = { accepted: 0, quarantined: 0, evicted: 0, rejected: 0, droppedQuarantine: 0 };
    this.blocks = new Map();      // key -> 非已接收条目（waiting/quarantined/evicted）
    this.dictionaries = new Map();// digest -> {payload, refs:Set<key>}
    this.quarantine = [];
    this.events = [];
    this.acceptedKeys = new Set(received.map(record => record.key));
    if (saved) {
      if (saved.version !== 1) throw new Error('接收器状态版本不支持');
      for (const key of Object.keys(DEFAULT_LIMITS)) {
        if (saved.config[key] !== this.config[key]) throw new Error(`配置 ${key} 与状态快照不一致，拒绝混用`);
      }
      if (saved.pendingPolicy !== this.pendingPolicy) throw new Error('待解上限策略与状态快照不一致，拒绝混用');
      this.sequence = saved.sequence;
      this.eventSeq = saved.eventSeq;
      Object.assign(this.counters, saved.counters);
      for (const entry of saved.blocks) {
        if (this.acceptedKeys.has(entry.key)) continue;
        const restored = { ...entry };
        if (restored.status === 'waiting') restored.payload = Buffer.from(restored.payloadBase64, 'base64');
        delete restored.payloadBase64;
        this.blocks.set(entry.key, restored);
      }
      for (const dictionary of saved.dictionaries) {
        this.dictionaries.set(dictionary.digest, { payload: Buffer.from(dictionary.payloadBase64, 'base64'), refs: new Set() });
      }
      this.quarantine = saved.quarantine.map(entry => ({ ...entry }));
      this.events = saved.events.map(event => ({ ...event }));
    }
    // 以只增日志为正式接收的权威来源，纠正可能的陈旧快照。
    let reconciled = false;
    if (this.counters.accepted !== received.length) { this.counters.accepted = received.length; reconciled = true; }
    for (const key of [...this.blocks.keys()]) {
      if (this.acceptedKeys.has(key)) { this.blocks.delete(key); reconciled = true; }
    }
    // 依据现存 waiting 块重建字典引用关系。
    for (const entry of this.blocks.values()) {
      if (entry.status === 'waiting' && this.dictionaries.has(entry.expectedDigest)) {
        this.dictionaries.get(entry.expectedDigest).refs.add(entry.key);
      }
    }
    if (reconciled) {
      this.addEvent({ type: 'state-reconciled', acceptedFromLog: received.length });
      await this.persist();
    }
    return this;
  }

  // ---- 内部工具 ----
  serialize(job) {
    if (this.closed) return Promise.reject(new Error('接收器已关闭'));
    const run = this.mutex.then(() => job());
    this.mutex = run.catch(() => {});
    return run;
  }

  now() { return new Date().toISOString(); }

  addEvent(detail) {
    this.eventSeq += 1;
    this.events.push({ seq: this.eventSeq, at: this.now(), ...detail });
    while (this.events.length > this.config.maxEvents) this.events.shift();
  }

  addQuarantine(entry) {
    this.quarantine.push(entry);
    this.counters.quarantined += 1;
    while (this.quarantine.length > this.config.maxQuarantine) {
      this.quarantine.shift();
      this.counters.droppedQuarantine += 1;
      this.addEvent({ type: 'quarantine-evidence-dropped' });
    }
  }

  evidence(payload) {
    const truncated = payload.length > EVIDENCE_BYTES;
    return {
      payloadBase64: payload.subarray(0, truncated ? EVIDENCE_BYTES : payload.length).toString('base64'),
      payloadSize: payload.length,
      payloadTruncated: truncated,
    };
  }

  async persist() {
    const blocks = [...this.blocks.values()].map(entry => {
      const { payload, ...rest } = entry;
      return entry.status === 'waiting' ? { ...rest, payloadBase64: payload.toString('base64') } : rest;
    });
    const dictionaries = [...this.dictionaries.entries()].map(([digest, value]) => ({
      digest, payloadBase64: value.payload.toString('base64'), size: value.payload.length, refs: [...value.refs],
    }));
    await this.checkpoint.save({
      version: 1,
      sequence: this.sequence,
      eventSeq: this.eventSeq,
      counters: { ...this.counters },
      config: Object.fromEntries(Object.keys(DEFAULT_LIMITS).map(key => [key, this.config[key]])),
      pendingPolicy: this.pendingPolicy,
      blocks, dictionaries,
      quarantine: this.quarantine,
      events: this.events,
    });
  }

  waitingCount() {
    let count = 0;
    for (const entry of this.blocks.values()) if (entry.status === 'waiting') count += 1;
    return count;
  }

  // ---- 资料块入口 ----
  ingestBlock(input) {
    return this.serialize(() => this.handleBlock(input));
  }

  async handleBlock({ gateway, epoch, chunkId, expectedDigest, payload: rawPayload, now = this.now() }) {
    if (typeof gateway !== 'string' || gateway.length === 0) throw new TypeError('gateway 必须为非空字符串');
    if (typeof chunkId !== 'string' || chunkId.length === 0) throw new TypeError('chunkId 必须为非空字符串');
    if (!Number.isInteger(epoch) || epoch < 0) throw new TypeError('epoch 必须为非负整数');
    const payload = asBuffer(rawPayload, '压缩块');
    const key = streamKey(gateway, epoch, chunkId);

    // 摘要本身不合法：永远等不到字典，直接隔离。
    if (typeof expectedDigest !== 'string' || !HEX64.test(expectedDigest)) {
      this.sequence += 1;
      this.addQuarantine({
        kind: 'block', key, gateway, epoch, chunkId, expectedDigest: String(expectedDigest ?? ''),
        status: 'quarantined', reason: 'block-bad-digest-reference', arrivalSeq: this.sequence, arrivalAt: now,
        ...this.evidence(payload), updatedAt: now,
      });
      this.addEvent({ type: 'block-quarantined', key, reason: 'block-bad-digest-reference' });
      await this.persist();
      return { outcome: 'quarantined', key, reason: 'block-bad-digest-reference' };
    }
    const digest = expectedDigest;

    // 已正式接收块的重送：幂等忽略，绝不重复入库。
    if (this.acceptedKeys.has(key)) {
      return { outcome: 'redelivered-accepted', key, accepted: true };
    }

    const existing = this.blocks.get(key);
    if (existing) {
      const samePayload = existing.status === 'waiting'
        ? existing.payload.equals(payload)
        : existing.payloadSize === payload.length && existing.payloadBase64 === payload.subarray(0, EVIDENCE_BYTES).toString('base64');
      if (samePayload && existing.expectedDigest === expectedDigest) {
        return { outcome: `redelivered-${existing.status}`, key, status: existing.status };
      }
      // 同流身份但内容/摘要不一致：
      // - 已淘汰条目重送：明确隔离并指出是淘汰后重送；
      // - 其他冲突证据：隔离，先到条目保持不变（保留接收顺序）。
      const reason = existing.status === 'evicted' ? 'redelivery-after-eviction' : `redelivery-conflict:${existing.status}`;
      this.sequence += 1;
      this.addQuarantine({
        kind: 'block', key, gateway, epoch, chunkId, expectedDigest,
        status: 'quarantined', reason,
        arrivalSeq: this.sequence, arrivalAt: now, ...this.evidence(payload), updatedAt: now,
      });
      this.addEvent({ type: 'block-quarantined', key, reason });
      await this.persist();
      return { outcome: 'quarantined', key, reason };
    }

    // 超大压缩块不进入待解队列，隔离截断证据，避免无界内存。
    if (payload.length > this.config.maxBlockBytes) {
      this.sequence += 1;
      this.addQuarantine({
        kind: 'block', key, gateway, epoch, chunkId, expectedDigest,
        status: 'quarantined', reason: 'block-too-large',
        arrivalSeq: this.sequence, arrivalAt: now, ...this.evidence(payload), updatedAt: now,
      });
      this.addEvent({ type: 'block-quarantined', key, reason: 'block-too-large' });
      await this.persist();
      return { outcome: 'quarantined', key, reason: 'block-too-large' };
    }

    // 待解上限：显式可查询策略，绝不悄悄丢最早一块。
    if (this.waitingCount() >= this.config.maxPending) {
      if (this.pendingPolicy === 'reject') {
        this.sequence += 1;
        this.counters.rejected += 1;
        this.addEvent({ type: 'block-rejected', key, reason: 'pending-limit-reject', arrivalSeq: this.sequence });
        await this.persist();
        return { outcome: 'rejected', key, reason: 'pending-limit-reject' };
      }
      const oldest = [...this.blocks.values()]
        .filter(entry => entry.status === 'waiting')
        .sort((a, b) => a.arrivalSeq - b.arrivalSeq)[0];
      const evidence = this.evidence(oldest.payload);
      oldest.status = 'evicted';
      oldest.reason = 'evicted-pending-limit';
      oldest.updatedAt = now;
      delete oldest.payload;
      Object.assign(oldest, evidence);
      this.dictionaries.get(oldest.expectedDigest)?.refs.delete(oldest.key);
      this.counters.evicted += 1;
      this.addEvent({ type: 'block-evicted', key: oldest.key, reason: 'evicted-pending-limit' });
    }

    this.sequence += 1;
    const entry = {
      kind: 'block', key, gateway, epoch, chunkId, expectedDigest: digest,
      status: 'waiting', reason: null,
      arrivalSeq: this.sequence, arrivalAt: now, updatedAt: now, payload,
    };
    this.blocks.set(key, entry);
    this.addEvent({ type: 'block-waiting', key, expectedDigest: digest, arrivalSeq: this.sequence });
    const dictionary = this.dictionaries.get(digest);
    if (dictionary) {
      // 字典已在库：登记后立即沿同一条解码/隔离路径处理，不制造无意义等待。
      dictionary.refs.add(key);
      const decision = await this.decodeOne(entry, dictionary.payload, now);
      await this.persist();
      return decision.ok
        ? { outcome: 'accepted', key, arrivalSeq: this.sequence }
        : { outcome: 'quarantined', key, reason: decision.reason };
    }
    await this.persist();
    return { outcome: 'waiting', key, arrivalSeq: this.sequence };
  }

  // ---- 字典入口 ----
  ingestDictionary(input) {
    return this.serialize(() => this.handleDictionary(input));
  }

  async handleDictionary({ digest, payload: rawPayload, now = this.now() }) {
    if (typeof digest !== 'string' || !HEX64.test(digest)) throw new TypeError('digest 必须为 64 位小写十六进制 sha256');
    const payload = asBuffer(rawPayload, '字典');

    if (payload.length > this.config.maxDictionaryBytes) {
      this.addQuarantine({
        kind: 'dictionary', digest, status: 'quarantined', reason: 'dictionary-too-large',
        arrivalAt: now, ...this.evidence(payload), updatedAt: now,
      });
      this.addEvent({ type: 'dictionary-quarantined', digest, reason: 'dictionary-too-large' });
      await this.persist();
      return { outcome: 'quarantined', digest, reason: 'dictionary-too-large', released: [] };
    }

    const actual = sha256(payload);
    if (actual !== digest) {
      // 损坏/错误字典：摘要对不上，绝不参与任何解压尝试。
      this.addQuarantine({
        kind: 'dictionary', digest, actualDigest: actual, status: 'quarantined',
        reason: 'dictionary-digest-mismatch', arrivalAt: now, ...this.evidence(payload), updatedAt: now,
      });
      this.addEvent({ type: 'dictionary-quarantined', digest, reason: 'dictionary-digest-mismatch' });
      await this.persist();
      return { outcome: 'quarantined', digest, reason: 'dictionary-digest-mismatch', released: [] };
    }

    if (this.dictionaries.has(digest)) {
      return { outcome: 'duplicate-dictionary', digest, released: [] };
    }

    if (this.dictionaries.size >= this.config.maxDictionaries) {
      const unused = [...this.dictionaries.entries()].find(([, value]) => value.refs.size === 0);
      if (!unused) {
        this.counters.rejected += 1;
        this.addEvent({ type: 'dictionary-rejected', digest, reason: 'dictionary-limit' });
        await this.persist();
        return { outcome: 'rejected', digest, reason: 'dictionary-limit', released: [] };
      }
      this.dictionaries.delete(unused[0]);
      this.addEvent({ type: 'dictionary-garbage-collected', digest: unused[0] });
    }

    const refs = new Set();
    this.dictionaries.set(digest, { payload, refs });
    const waiting = [...this.blocks.values()]
      .filter(entry => entry.status === 'waiting' && entry.expectedDigest === digest)
      .sort((a, b) => a.arrivalSeq - b.arrivalSeq);
    for (const entry of waiting) refs.add(entry.key);
    this.addEvent({ type: 'dictionary-stored', digest, matched: waiting.length });

    const released = [];
    const failed = [];
    for (const entry of waiting) {
      const decision = await this.decodeOne(entry, payload, now);
      if (decision.ok) released.push(entry.key);
      else failed.push({ key: entry.key, reason: decision.reason });
    }
    await this.persist();
    return { outcome: 'stored', digest, released, failed };
  }

  async decodeOne(entry, dictionary, now) {
    let text;
    try {
      text = inflateSync(entry.payload, { dictionary, maxOutputLength: this.config.maxOutputLength }).toString('utf8');
    } catch (error) {
      const reason = error.code === 'ERR_BUFFER_TOO_LARGE' ? 'output-limit-exceeded' : 'decompress-failed';
      this.quarantineEntry(entry, reason, now);
      return { ok: false, reason };
    }
    if (!this.validator(text)) {
      this.quarantineEntry(entry, 'structure-invalid', now, { decodedPreview: text.slice(0, EVIDENCE_BYTES) });
      return { ok: false, reason: 'structure-invalid' };
    }
    // 正式接收：先写只增日志并 fsync，再更新内存与快照。
    // 日志写入在互斥串行区内完成；若抛错，内存不推进、快照不变，重启后可用同字典重做。
    const record = {
      key: entry.key,
      gateway: entry.gateway,
      epoch: entry.epoch,
      chunkId: entry.chunkId,
      dictionaryDigest: entry.expectedDigest,
      arrivalSeq: entry.arrivalSeq,
      receivedAt: entry.arrivalAt,
      acceptedAt: now,
      decodedBytes: Buffer.byteLength(text),
      record: text,
    };
    await this.log.append(record);
    this.blocks.delete(entry.key);
    this.dictionaries.get(entry.expectedDigest)?.refs.delete(entry.key);
    this.acceptedKeys.add(entry.key);
    this.counters.accepted += 1;
    this.addEvent({ type: 'block-accepted', key: entry.key, arrivalSeq: entry.arrivalSeq });
    return { ok: true };
  }

  quarantineEntry(entry, reason, now, extra = {}) {
    this.blocks.delete(entry.key);
    this.dictionaries.get(entry.expectedDigest)?.refs.delete(entry.key);
    this.addQuarantine({
      kind: 'block', key: entry.key, gateway: entry.gateway, epoch: entry.epoch, chunkId: entry.chunkId,
      expectedDigest: entry.expectedDigest, status: 'quarantined', reason,
      arrivalSeq: entry.arrivalSeq, arrivalAt: entry.arrivalAt, updatedAt: now,
      ...this.evidence(entry.payload), ...extra,
    });
    this.addEvent({ type: 'block-quarantined', key: entry.key, reason });
  }

  // ---- 查询 ----
  status() {
    const pending = [...this.blocks.values()]
      .filter(entry => entry.status === 'waiting')
      .sort((a, b) => a.arrivalSeq - b.arrivalSeq)
      .map((entry, index) => ({
        order: index + 1, key: entry.key, gateway: entry.gateway, epoch: entry.epoch,
        chunkId: entry.chunkId, expectedDigest: entry.expectedDigest,
        arrivalSeq: entry.arrivalSeq, arrivalAt: entry.arrivalAt, queuedBytes: entry.payload.length,
      }));
    const terminal = [...this.blocks.values()].filter(entry => entry.status !== 'waiting');
    return {
      pendingPolicy: this.pendingPolicy,
      limits: { ...this.config },
      counts: {
        waiting: pending.length,
        accepted: this.counters.accepted,
        quarantined: this.counters.quarantined,
        evicted: this.counters.evicted,
        rejected: this.counters.rejected,
        droppedQuarantineEvidence: this.counters.droppedQuarantine,
        dictionariesStored: this.dictionaries.size,
      },
      pending,
      quarantine: this.quarantine.map(({ payloadBase64, ...rest }) => ({ ...rest, evidenceBase64: payloadBase64 })),
      evicted: terminal.filter(entry => entry.status === 'evicted'),
      recentEvents: this.events,
    };
  }

  async listReceived() {
    return this.log.readAll();
  }

  async close() {
    await this.mutex;
    this.closed = true;
  }
}
