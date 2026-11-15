#!/usr/bin/env node
// Tempest Chronicles relay: rooms with short join codes; it forwards game messages between the
// players of a room and nothing else (the host's game runs the battle). Plain WebSocket, so PC,
// Android, iOS and web builds all connect the same way. No dependencies: node server/relay.js
//
//   PORT=8787 node server/relay.js        (default port 8787)
//
// Protocol
//   text frames: JSON control messages
//     client -> relay  {"t":"create","name":"Ann","max":4,"ver":"1"}   make a room; you are its host (peer 1)
//                      {"t":"join","code":"KX4PT","name":"Bo","ver":"1"}
//                      {"t":"lock"}      host only: the game started, nobody else may join
//                      {"t":"kick","id":3}  host only
//                      {"t":"leave"}
//     relay -> client  {"t":"created","code":"KX4PT","id":1}
//                      {"t":"joined","code":"KX4PT","id":2,"host":1,"peers":[{"id":1,"name":"Ann"}, ...]}
//                      {"t":"peer_in","id":2,"name":"Bo"}   {"t":"peer_out","id":2}
//                      {"t":"closed","why":"..."}           the host left: the room is gone
//                      {"t":"error","why":"..."}
//   binary frames: game data. client -> relay: [to (1 byte, 0 = everyone else)] + payload
//                                relay -> client: [from (1 byte)] + payload
//
// Abuse limits (all overridable by env; defaults sit far above a real 4-player match so legit
// games never trip them). A real host sends ~15 snapshots/s per peer (~45 frames/s, a few MiB/s
// at most); guests send ~12-30 orders/s. See server/SECURITY.md for the threat model.
'use strict';
const http = require('http');
const crypto = require('crypto');

const num = (name, def) => { const v = parseInt(process.env[name] || '', 10); return Number.isFinite(v) && v >= 0 ? v : def; };

const PORT = num('PORT', 8787);
const VERSION = '1';
const MAX_PLAYERS = 4;
const MAX_FRAME = 1 << 20;            // 1 MiB per message
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';   // no 0/O, 1/I

// --- abuse limits ---------------------------------------------------------------------------
const MAX_CONNS        = num('MAX_CONNS', 4000);        // total live sockets
const MAX_CONNS_PER_IP = num('MAX_CONNS_PER_IP', 24);  // sockets from one address
const MAX_ROOMS        = num('MAX_ROOMS', 2000);        // total open rooms
const MAX_ROOMS_PER_IP = num('MAX_ROOMS_PER_IP', 20);  // rooms hosted from one address
const JOIN_GRACE_MS    = num('JOIN_GRACE_MS', 30000);  // drop a socket that never joins a room
const PING_MS          = num('PING_MS', 5000);         // ping every 5 s; no answer by the next ping = gone
const HB_TIMEOUT_MS    = num('HB_TIMEOUT_MS', 10000);  // a game that sends heartbeats and then goes quiet this long = gone
const CTRL_RATE        = num('CTRL_RATE', 15);         // control msgs/s (create/join/lock/kick/leave)
const CTRL_BURST       = num('CTRL_BURST', 30);
const DATA_BYTES_PER_S = num('DATA_BYTES_PER_S', 8 << 20);   // game-data byte budget, 8 MiB/s
const DATA_BURST       = num('DATA_BURST', 4 << 20);         // 4 MiB bucket
const SEND_BUF_MAX     = num('SEND_BUF_MAX', 8 << 20);  // kill a client whose outbox backs up past this
const MAX_FRAGMENTS    = num('MAX_FRAGMENTS', 256);     // continuation frames per message
const HANDSHAKE_MS     = num('HANDSHAKE_MS', 10000);    // time allowed to complete the HTTP upgrade
const TRUST_PROXY      = /^(1|true|yes)$/i.test(process.env.TRUST_PROXY || '');   // read X-Forwarded-For
const ALLOWED_ORIGINS  = (process.env.ALLOWED_ORIGINS || '').split(',').map((s) => s.trim()).filter(Boolean);

const rooms = new Map();              // code -> {code, host, peers: Map id -> conn, locked, next, ip}
const conns = new Set();              // all live connections
const perIp = new Map();              // ip -> {conns, rooms}

function log(...a) { console.log(new Date().toISOString(), ...a); }

function ipStat(ip) { let s = perIp.get(ip); if (!s) { s = { conns: 0, rooms: 0 }; perIp.set(ip, s); } return s; }
function ipDrop(ip) { const s = perIp.get(ip); if (s && s.conns <= 0 && s.rooms <= 0) perIp.delete(ip); }

// token bucket: refills at `rate` per second, holds at most `burst`
function bucket(rate, burst) { return { tokens: burst, rate, burst, t: Date.now() }; }
function take(b, n) {
  const now = Date.now();
  b.tokens = Math.min(b.burst, b.tokens + ((now - b.t) / 1000) * b.rate);
  b.t = now;
  if (b.tokens >= n) { b.tokens -= n; return true; }
  return false;
}

// ------------------------------------------------------------------ WebSocket (RFC 6455) by hand

class Conn {
  constructor(sock, ip) {
    this.sock = sock;
    this.ip = ip;
    this.buf = Buffer.alloc(0);
    this.frags = [];
    this.fragOp = 0;
    this.fragBytes = 0;
    this.room = null;
    this.id = 0;
    this.name = '';
    this.alive = true;
    this.hb = false;                       // true once this game sends heartbeats (newer builds)
    this.lastApp = Date.now();             // last game message (heartbeat, control or data)
    this.closed = false;
    this.ctrlB = bucket(CTRL_RATE, CTRL_BURST);
    this.dataB = bucket(DATA_BYTES_PER_S, DATA_BURST);
    this.grace = setTimeout(() => { if (!this.room) this.kill(1008); }, JOIN_GRACE_MS);
    sock.setNoDelay(true);
    sock.on('data', (d) => this.onData(d));
    sock.on('drain', () => { if (this.sock.writableLength <= SEND_BUF_MAX) { /* caught up */ } });
    sock.on('close', () => this.onClose());
    sock.on('error', () => this.onClose());
  }

  onData(d) {
    this.alive = true;                     // anything from the player proves the connection is alive
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) {
        if (this.buf.length < 10) return;
        const hi = this.buf.readUInt32BE(2), lo = this.buf.readUInt32BE(6);
        if (hi !== 0) return this.kill(1009);
        len = lo; off = 10;
      }
      if (len > MAX_FRAME) return this.kill(1009);
      if (!masked) return this.kill(1002);       // clients must mask
      if (this.buf.length < off + 4 + len) return;
      const mask = this.buf.subarray(off, off + 4);
      const data = Buffer.from(this.buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
      this.buf = this.buf.subarray(off + 4 + len);
      this.onFrame(fin, op, data);
      if (this.closed) return;
    }
  }

  onFrame(fin, op, data) {
    if (op === 0x8) return this.kill(1000);
    if (op === 0x9) {                                      // ping -> pong, but not as a flood
      if (!take(this.ctrlB, 1)) return this.kill(1008);
      return this.send(0xA, data);
    }
    if (op === 0xA) { this.alive = true; return; }
    if (op === 0x1 || op === 0x2) {
      if (fin) { this.fragBytes = 0; return this.onMessage(op, data); }
      this.fragOp = op; this.frags = [data]; this.fragBytes = data.length; return;
    }
    if (op === 0x0) {
      if (this.frags.length === 0) return this.kill(1002);           // continuation with nothing to continue
      if (this.frags.length >= MAX_FRAGMENTS) return this.kill(1009); // fragment bomb
      this.fragBytes += data.length;
      if (this.fragBytes > MAX_FRAME) return this.kill(1009);
      this.frags.push(data);
      if (fin) { const all = Buffer.concat(this.frags); this.frags = []; this.fragBytes = 0; this.onMessage(this.fragOp, all); }
    }
  }

  send(op, data) {
    if (this.closed) return;
    const len = data.length;
    let head;
    if (len < 126) head = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
    this.sock.write(Buffer.concat([head, data]));
    // slow reader: if the kernel/Node outbox is backing up, this client isn't keeping up — drop it
    // rather than buffer without bound. Data frames (0x2) are the bulk; never drop a close frame.
    if (op !== 0x8 && this.sock.writableLength > SEND_BUF_MAX) this.kill(1009);
  }

  json(o) { this.send(0x1, Buffer.from(JSON.stringify(o))); }

  kill(code) {
    if (this.closed) return;
    try { this.send(0x8, Buffer.from([code >> 8, code & 0xff])); } catch (e) { /* gone */ }
    this.sock.end();
    this.onClose();
  }

  onClose() {
    if (this.closed) return;
    this.closed = true;
    clearTimeout(this.grace);
    leaveRoom(this);
    conns.delete(this);
    const s = perIp.get(this.ip);
    if (s) { s.conns--; ipDrop(this.ip); }
  }

  onMessage(op, data) {
    this.lastApp = Date.now();
    if (op === 0x2) {
      if (!take(this.dataB, Math.max(1, data.length))) return this.kill(1009);   // byte-rate flood
      return relayData(this, data);
    }
    if (!take(this.ctrlB, 1)) return this.kill(1008);      // control flood
    let m;
    try { m = JSON.parse(data.toString('utf8')); } catch (e) { return this.json({ t: 'error', why: 'bad message' }); }
    if (m && m.t === 'hb') { this.hb = true; return; }     // heartbeat: "I'm still here", no reply
    control(this, m);
  }
}

// ------------------------------------------------------------------ rooms

function newCode() {
  for (;;) {
    let c = '';
    const r = crypto.randomBytes(5);
    for (let i = 0; i < 5; i++) c += CODE_CHARS[r[i] % CODE_CHARS.length];
    if (!rooms.has(c)) return c;
  }
}

function cleanName(n) {
  return String(n || 'Player').replace(/[\u0000-\u001f]/g, '').slice(0, 20) || 'Player';
}

function control(c, m) {
  switch (m.t) {
    case 'create': {
      if (c.room) return c.json({ t: 'error', why: 'already in a room' });
      if (String(m.ver) !== VERSION) return c.json({ t: 'error', why: 'version' });
      if (rooms.size >= MAX_ROOMS) return c.json({ t: 'error', why: 'the server is full' });
      const st = ipStat(c.ip);
      if (st.rooms >= MAX_ROOMS_PER_IP) return c.json({ t: 'error', why: 'too many rooms' });
      const room = { code: newCode(), host: 1, peers: new Map(), locked: false, next: 2, ip: c.ip,
        max: Math.max(2, Math.min(MAX_PLAYERS, parseInt(m.max || MAX_PLAYERS, 10) || MAX_PLAYERS)) };
      rooms.set(room.code, room);
      st.rooms++;
      c.room = room; c.id = 1; c.name = cleanName(m.name);
      room.peers.set(1, c);
      log('room', room.code, 'created by', c.name);
      return c.json({ t: 'created', code: room.code, id: 1 });
    }
    case 'join': {
      if (c.room) return c.json({ t: 'error', why: 'already in a room' });
      if (String(m.ver) !== VERSION) return c.json({ t: 'error', why: 'version' });
      const room = rooms.get(String(m.code || '').toUpperCase().trim());
      if (!room) return c.json({ t: 'error', why: 'no such room' });
      if (room.locked) return c.json({ t: 'error', why: 'the game already started' });
      if (room.peers.size >= room.max) return c.json({ t: 'error', why: 'the room is full' });
      let id = room.next;
      while (room.peers.has(id) || id > 250) id = id > 250 ? 2 : id + 1;
      room.next = id + 1;
      c.room = room; c.id = id; c.name = cleanName(m.name);
      const peers = [...room.peers.values()].map((p) => ({ id: p.id, name: p.name }));
      for (const p of room.peers.values()) p.json({ t: 'peer_in', id, name: c.name });
      room.peers.set(id, c);
      log('room', room.code, c.name, 'joined as', id);
      return c.json({ t: 'joined', code: room.code, id, host: room.host, peers });
    }
    case 'lock':
      if (c.room && c.id === c.room.host) c.room.locked = true;
      return;
    case 'kick': {
      if (!c.room || c.id !== c.room.host) return;
      const p = c.room.peers.get(parseInt(m.id, 10));
      if (p && p !== c) { p.json({ t: 'closed', why: 'removed by the host' }); leaveRoom(p); p.room = null; }
      return;
    }
    case 'leave':
      leaveRoom(c);
      c.room = null;
      if (!c.closed) c.grace = setTimeout(() => { if (!c.room) c.kill(1008); }, JOIN_GRACE_MS);
      return;
    default:
      return c.json({ t: 'error', why: 'unknown message' });
  }
}

function leaveRoom(c) {
  const room = c.room;
  if (!room || !room.peers.has(c.id) || room.peers.get(c.id) !== c) return;
  room.peers.delete(c.id);
  if (c.id === room.host) {
    // no host, no game: everyone else is told and the room goes
    for (const p of room.peers.values()) { p.json({ t: 'closed', why: 'the host left' }); p.room = null; }
    room.peers.clear();
    rooms.delete(room.code);
    const st = perIp.get(room.ip);
    if (st) { st.rooms--; ipDrop(room.ip); }
    log('room', room.code, 'closed');
  } else {
    for (const p of room.peers.values()) p.json({ t: 'peer_out', id: c.id });
    log('room', room.code, c.name, 'left');
  }
}

function relayData(c, data) {
  const room = c.room;
  if (!room || data.length < 1) return;
  const to = data[0];
  const out = Buffer.from(data);
  out[0] = c.id;                         // the receiver sees who sent it
  if (to === 0) {
    for (const p of room.peers.values()) if (p !== c) p.send(0x2, out);
  } else {
    const p = room.peers.get(to);
    if (p) p.send(0x2, out);
  }
}

// ------------------------------------------------------------------ server

function clientIp(req, sock) {
  if (TRUST_PROXY) {
    const xff = (req.headers['x-forwarded-for'] || '').split(',')[0].trim();
    if (xff) return xff;
  }
  return sock.remoteAddress || '?';
}

const server = http.createServer((req, res) => {
  // plain HTTP: a health check for hosting services
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end(`tempest relay ok, ${rooms.size} rooms\n`);
});
server.maxConnections = MAX_CONNS;                 // Node refuses accepts past this
server.headersTimeout = HANDSHAKE_MS;              // a socket must send its request headers in time
server.requestTimeout = HANDSHAKE_MS;

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  // browsers send Origin; native app builds don't. Only restrict when an allowlist is configured.
  if (ALLOWED_ORIGINS.length && req.headers.origin && !ALLOWED_ORIGINS.includes(req.headers.origin)) {
    sock.destroy(); return;
  }
  const ip = clientIp(req, sock);
  const st = ipStat(ip);
  if (conns.size >= MAX_CONNS || st.conns >= MAX_CONNS_PER_IP) { sock.destroy(); ipDrop(ip); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  sock.setTimeout(0);                              // we run our own ping/grace timers from here
  st.conns++;
  const c = new Conn(sock, ip);
  conns.add(c);
});

// drop connections that go silent (a host whose game crashed, a phone that lost signal). Hosting
// proxies like Render's don't pass a dropped connection on to the relay, so this check is how the
// other players in the room find out: within PING_MS to 2 x PING_MS (5-10 s by default).
setInterval(() => {
  for (const c of conns) {
    try {
      if (!c.alive) { c.kill(1001); continue; }
      // the proxy in front of a hosted relay (Render) answers pings itself, so a frozen or vanished
      // game still looks alive at the socket level. Games that send heartbeats are dropped when those
      // stop; older games that never sent one keep the plain ping check above.
      if (c.hb && Date.now() - c.lastApp > HB_TIMEOUT_MS) { c.kill(1001); continue; }
      c.alive = false;
      c.send(0x9, Buffer.alloc(0));
    } catch (e) { try { c.onClose(); } catch (e2) { /* gone */ } }
  }
}, PING_MS).unref();

// ---------------------------------------------------------------- stay up (don't let the relay "go down")
// 1) One unhandled error must never take the whole server (and every room on it) down. Log and keep serving.
process.on('uncaughtException', (e) => log('uncaughtException (kept running):', (e && e.stack) || e));
process.on('unhandledRejection', (e) => log('unhandledRejection (kept running):', (e && e.stack) || e));

// 2) Free hosting (Render's free plan) spins a web service down after ~15 min without inbound traffic; the
//    next player then hits a cold start — the relay was effectively "down" between sessions. Ping our own
//    public URL every few minutes to stay awake. Render sets RENDER_EXTERNAL_URL automatically; or set
//    KEEPALIVE_URL yourself. (A paid always-on plan doesn't need this.)
const SELF_URL = (process.env.KEEPALIVE_URL || process.env.RENDER_EXTERNAL_URL || '').trim();
if (SELF_URL) {
  const getter = SELF_URL.startsWith('https') ? require('https') : http;
  setInterval(() => {
    try { getter.get(SELF_URL, (r) => r.resume()).on('error', () => {}); } catch (e) { /* ignore */ }
  }, num('KEEPALIVE_MS', 10 * 60 * 1000)).unref();
  log('keepalive: pinging', SELF_URL, 'to stay awake');
}

// 3) When the host restarts us (a deploy, or Render recycling the instance), tell every client with a
//    clean close frame instead of dropping them cold, so their game can show "relay restarting" not a hang.
function shutdown() {
  for (const c of conns) { try { c.kill(1012); } catch (e) { /* gone */ } }  // 1012 = service restart
  try { server.close(); } catch (e) { /* ignore */ }
  setTimeout(() => process.exit(0), 300).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.listen(PORT, () => log(`relay listening on :${PORT}`));
module.exports = { server };
