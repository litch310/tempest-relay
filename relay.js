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
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '8787', 10);
const VERSION = '1';
const MAX_PLAYERS = 4;
const MAX_FRAME = 1 << 20;            // 1 MiB per message
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';   // no 0/O, 1/I
const rooms = new Map();              // code -> {code, host, peers: Map id -> conn, locked, next}

function log(...a) { console.log(new Date().toISOString(), ...a); }

// ------------------------------------------------------------------ WebSocket (RFC 6455) by hand

class Conn {
  constructor(sock) {
    this.sock = sock;
    this.buf = Buffer.alloc(0);
    this.frags = [];
    this.fragOp = 0;
    this.room = null;
    this.id = 0;
    this.name = '';
    this.alive = true;
    this.closed = false;
    sock.setNoDelay(true);
    sock.on('data', (d) => this.onData(d));
    sock.on('close', () => this.onClose());
    sock.on('error', () => this.onClose());
  }

  onData(d) {
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
    if (op === 0x9) return this.send(0xA, data);           // ping -> pong
    if (op === 0xA) { this.alive = true; return; }
    if (op === 0x1 || op === 0x2) {
      if (fin) return this.onMessage(op, data);
      this.fragOp = op; this.frags = [data]; return;
    }
    if (op === 0x0) {
      this.frags.push(data);
      if (this.frags.reduce((n, f) => n + f.length, 0) > MAX_FRAME) return this.kill(1009);
      if (fin) { const all = Buffer.concat(this.frags); this.frags = []; this.onMessage(this.fragOp, all); }
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
    leaveRoom(this);
  }

  onMessage(op, data) {
    if (op === 0x2) return relayData(this, data);
    let m;
    try { m = JSON.parse(data.toString('utf8')); } catch (e) { return this.json({ t: 'error', why: 'bad message' }); }
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
      const room = { code: newCode(), host: 1, peers: new Map(), locked: false, next: 2,
        max: Math.max(2, Math.min(MAX_PLAYERS, parseInt(m.max || MAX_PLAYERS, 10) || MAX_PLAYERS)) };
      rooms.set(room.code, room);
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

const server = http.createServer((req, res) => {
  // plain HTTP: a health check for hosting services
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end(`tempest relay ok, ${rooms.size} rooms\n`);
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  const c = new Conn(sock);
  conns.add(c);
  sock.on('close', () => conns.delete(c));
});

// drop connections that stop answering pings (phones that lost signal)
const conns = new Set();
setInterval(() => {
  for (const c of conns) {
    if (!c.alive) { c.kill(1001); continue; }
    c.alive = false;
    c.send(0x9, Buffer.alloc(0));
  }
}, 15000).unref();

server.listen(PORT, () => log(`relay listening on :${PORT}`));
module.exports = { server };
