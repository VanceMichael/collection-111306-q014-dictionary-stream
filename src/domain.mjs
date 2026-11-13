import {deflateSync,inflateSync} from 'node:zlib';
import {createHash} from 'node:crypto';
export function dictionaryDigest(bytes){return createHash('sha256').update(bytes).digest('hex');}
export function compress(text,dictionary){return deflateSync(Buffer.from(text),{dictionary});}
export function inspectChunk(bytes){return {bytes:bytes.length,digest:dictionaryDigest(bytes)};}
export function decodePreview(bytes,dictionary){return inflateSync(bytes,{dictionary,maxOutputLength:65536}).toString('utf8');}
export function preview(data){const dictionary=Buffer.from(data.dictionary);return {dictionary:dictionaryDigest(dictionary),chunks:data.chunks.map(row=>({id:row.id,...inspectChunk(compress(row.text,dictionary))}))};}
