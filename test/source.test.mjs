import test from 'node:test';
import assert from 'node:assert/strict';
import {startSource} from '../src/source.mjs';
test('本地资料源可复现业务边界',async()=>{
 const source=await startSource();const action=async action=>{const response=await fetch(source.url+'/api/action',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({action})});assert.equal(response.status,200);return response.json();};
 try{const response=await fetch(source.url+'/api/chunk/c1');const before=response.headers.get('X-Dictionary-Digest');assert.ok((await response.arrayBuffer()).byteLength>0);await action('restart');const next=await fetch(source.url+'/api/chunk/c1');assert.notEqual(next.headers.get('X-Dictionary-Digest'),before);assert.equal(next.headers.get('X-Gateway-Epoch'),'2');}finally{await source.close();}
});
test('未知控制动作不改变源状态',async()=>{const source=await startSource();try{const before=await(await fetch(source.url+'/api/snapshot')).json();const response=await fetch(source.url+'/api/action',{method:'POST',body:JSON.stringify({action:'not-registered'})});assert.equal(response.status,400);assert.deepEqual(await(await fetch(source.url+'/api/snapshot')).json(),before);}finally{await source.close();}});
