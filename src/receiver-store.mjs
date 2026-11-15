import { mkdir, readFile, rename, open, rm } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

export function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

function checksum(text) { return sha256(text); }

/**
 * 带完整性校验的原子状态快照。
 * 写入采用 临时文件 + fsync + rename + 目录 fsync，崩溃只会留下上一个完整版本。
 */
export class Checkpoint {
  constructor(path) { this.path = path; }
  async load() {
    let text;
    try { text = await readFile(this.path, 'utf8'); }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
    const envelope = JSON.parse(text);
    if (typeof envelope !== 'object' || envelope === null || checksum(envelope.body) !== envelope.checksum) {
      throw new Error('接收器状态快照校验失败');
    }
    return JSON.parse(envelope.body);
  }
  async save(body) {
    const text = JSON.stringify(body);
    const temporary = `${this.path}.${randomUUID()}.tmp`;
    await mkdir(dirname(this.path), { recursive: true });
    try {
      const handle = await open(temporary, 'wx', 0o600);
      try {
        await handle.writeFile(JSON.stringify({ checksum: checksum(text), body: text }));
        await handle.sync();
      } finally { await handle.close(); }
      await rename(temporary, this.path);
      const directory = await open(dirname(this.path), 'r');
      try { await directory.sync(); } finally { await directory.close(); }
    } finally { await rm(temporary, { force: true }); }
  }
}

function lineHash(record) {
  const { _sha256, ...rest } = record;
  return sha256(JSON.stringify(rest));
}

/**
 * 只增的正式接收日志（JSONL），每行自带 sha256，追加后 fsync。
 * 磁盘增长只来自真正正式接收的业务记录；内存中不保留全文。
 */
export class ReceivedLog {
  constructor(directory, filename = 'received.log') {
    this.path = join(directory, filename);
  }
  async append(record) {
    await mkdir(dirname(this.path), { recursive: true });
    const line = { ...record, _sha256: lineHash(record) };
    const text = JSON.stringify(line) + '\n';
    const handle = await open(this.path, 'a', 0o600);
    try { await handle.writeFile(text); await handle.sync(); }
    finally { await handle.close(); }
  }
  async readAll() {
    let text;
    try { text = await readFile(this.path, 'utf8'); }
    catch (error) { if (error.code === 'ENOENT') return []; throw error; }
    const records = [];
    const lines = text.split('\n');
    for (const line of lines) {
      if (line === '') continue;
      let record;
      try { record = JSON.parse(line); }
      catch { throw new Error('接收日志存在无法解析的行'); }
      if (record._sha256 !== lineHash(record)) throw new Error('接收日志校验失败');
      const { _sha256, ...rest } = record;
      records.push(rest);
    }
    return records;
  }
}

export { lineHash };
