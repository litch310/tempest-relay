// Dev test for server/relay.js: node server/test_relay.js   (starts its own relay on a spare port)
'use strict';
process.env.PORT = '18787';
const { server } = require('./relay.js');
const URL = 'ws://127.0.0.1:18787';
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
function client(name) {
  const ws = new WebSocket(URL);
  ws.binaryType = 'arraybuffer';
  const c = { ws, name, ctrl: [], data: [] };
  ws.onmessage = (e) => {
    if (typeof e.data === 'string') c.ctrl.push(JSON.parse(e.data));
    else c.data.push(new Uint8Array(e.data));
  };
  c.open = new Promise((r) => (ws.onopen = r));
  c.send = (o) => ws.send(JSON.stringify(o));
  c.last = (t) => [...c.ctrl].reverse().find((m) => m.t === t);
  return c;
}
let fails = 0;
const ok = (cond, what) => { console.log((cond ? 'OK   ' : 'FAIL ') + what); if (!cond) fails++; };
(async () => {
  const a = client('Ann'), b = client('Bo'), c = client('Cy'), d = client('Di');
  await Promise.all([a.open, b.open, c.open, d.open]);
  a.send({ t: 'create', name: 'Ann', max: 3, ver: '1' });
  await wait(100);
  const code = a.last('created')?.code;
  ok(/^[A-Z2-9]{5}$/.test(code || ''), 'create gives a code ' + code);
  b.send({ t: 'join', code: code.toLowerCase(), name: 'Bo', ver: '1' });
  await wait(100);
  const jb = b.last('joined');
  ok(jb && jb.id === 2 && jb.host === 1 && jb.peers.length === 1, 'join (code case-insensitive) as peer 2');
  ok(a.last('peer_in')?.id === 2, 'host told of the new peer');
  d.send({ t: 'join', code: 'ZZZZZ', name: 'Di', ver: '1' });
  b.ws.send(new Uint8Array([0, 7, 8, 9]));                 // Bo -> everyone
  a.ws.send(new Uint8Array([2, 42]));                      // Ann -> Bo only
  await wait(100);
  ok(d.last('error')?.why === 'no such room', 'wrong code refused');
  ok(a.data.length === 1 && a.data[0].join() === '2,7,8,9', 'broadcast reaches host with sender id');
  ok(b.data.length === 1 && b.data[0].join() === '1,42', 'direct message reaches peer 2 only');
  // a big snapshot-sized message (64 KiB) survives the relay
  const big = new Uint8Array(65537); big[0] = 2; for (let i = 1; i < big.length; i++) big[i] = i & 255;
  a.ws.send(big);
  await wait(200);
  const got = b.data[1];
  ok(got && got.length === 65537 && got[0] === 1 && got[65536] === (65536 & 255), 'a 64 KiB message arrives whole');
  a.send({ t: 'lock' });
  c.send({ t: 'join', code, name: 'Cy', ver: '1' });
  await wait(100);
  ok(c.last('error')?.why === 'the game already started', 'locked room refuses joins');
  b.send({ t: 'leave' });
  await wait(100);
  ok(a.last('peer_out')?.id === 2, 'host told when a peer leaves');
  // a new room: host leaves -> peers told the room closed
  const e = client('Ed'), f = client('Fy');
  await Promise.all([e.open, f.open]);
  e.send({ t: 'create', name: 'Ed', ver: '1' });
  await wait(100);
  f.send({ t: 'join', code: e.last('created').code, name: 'Fy', ver: '1' });
  await wait(100);
  e.ws.close();
  await wait(200);
  ok(f.last('closed')?.why === 'the host left', 'room closes when the host leaves');
  f.send({ t: 'join', code: 'XXXXX', name: 'v', ver: '0' });
  await wait(100);
  ok(f.last('error')?.why === 'version', 'old game version refused');
  for (const x of [a, b, c, d, f]) x.ws.close();
  server.close();
  console.log(fails ? `${fails} FAILED` : 'ALL OK');
  process.exit(fails ? 1 : 0);
})();
