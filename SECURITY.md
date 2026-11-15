# Relay server — threat model

The relay (`relay.js`) is a dumb message forwarder: rooms with short join codes, and it passes bytes
between the players of a room. **The host's game is authoritative** — it validates every order a guest
sends (see `rts/netgame.gd`: orders are checked, rate-limited, and only for the sender's own team).
So the relay is not where cheating is stopped; the relay's job is to **stay up and route honestly** no
matter what a malicious client sends. These are the attacks it defends against and how.

| # | Attack | Defense in `relay.js` |
|---|--------|------------------------|
| 1 | **Connection flood** (open thousands of sockets to exhaust memory/FDs) | `MAX_CONNS` global cap + `MAX_CONNS_PER_IP`; Node's own `server.maxConnections` refuses accepts past the cap |
| 2 | **Room-table exhaustion** (spam `create` to grow the room map) | `MAX_ROOMS` global cap + `MAX_ROOMS_PER_IP`; a host's room count is tracked and released on close |
| 3 | **Message / broadcast flood + amplification** (blast large frames the relay re-sends to every peer) | Per-connection byte-rate token bucket `DATA_BYTES_PER_S`/`DATA_BURST`; offenders are closed, not buffered |
| 4 | **Slow-reader memory DoS** (connect, never read, let the relay's outbox grow) | `send()` checks `sock.writableLength`; past `SEND_BUF_MAX` the client is killed (1009) |
| 5 | **Fragment bomb** (one message split into huge numbers of tiny continuation frames → CPU) | `MAX_FRAGMENTS` cap, running `fragBytes` counter (no per-frame `reduce`), total still capped at `MAX_FRAME` |
| 6 | **Idle / Slowloris hold** (hold sockets open, answer pings, never join a room) | `JOIN_GRACE_MS` timer drops any socket that isn't in a room in time; ping flood spends the control bucket |
| 7 | **Cross-site WebSocket hijack** (a random web page opens a socket and spams rooms) | Optional `ALLOWED_ORIGINS` allowlist (browsers send `Origin`; native builds don't, so it's opt-in) |
| 8 | **Join-code brute force** (guess codes to crash or grief private rooms) | 5 chars × 32-symbol alphabet = 33.5M crypto-random codes; guesses spend the control-message bucket, so a flood of `join` attempts is killed |
| 9 | **Peer impersonation / order spoofing** (claim to be the host or another player) | The relay stamps the real sender id on every forwarded frame; `lock`/`kick`/room-close are host-only and checked server-side |
| 10 | **Plaintext tampering / eavesdropping** (MITM a `ws://` deployment) | Deploy behind TLS: Render gives `wss://` for free; on a VPS put Caddy/Nginx in front and use `wss://`. The game's `DEFAULT_RELAY` is already `wss://`. |

What the relay deliberately does **not** try to do: validate game logic, detect map-hacks, or judge
whether an order is legal. That is the authoritative host's job and is already enforced there. Pushing
that into the relay would make it slow and would not help — a cheating host is caught by its guests'
own authoritative checks, not by the relay.

## Limits (all env-tunable)

Defaults sit far above a real 4-player match (host ≈ 45 frames/s, a few MiB/s; guests ≈ 12–30 orders/s),
so legit play never trips them. Lower them only if you see abuse; raise them for larger free-for-alls.

| Env | Default | Meaning |
|-----|---------|---------|
| `MAX_CONNS` | 4000 | total live sockets |
| `MAX_CONNS_PER_IP` | 24 | sockets from one address |
| `MAX_ROOMS` | 2000 | open rooms |
| `MAX_ROOMS_PER_IP` | 20 | rooms hosted from one address |
| `JOIN_GRACE_MS` | 30000 | drop a socket that never joins a room |
| `CTRL_RATE` / `CTRL_BURST` | 15 / 30 | control messages per second / burst (also caps ping & join-guess floods) |
| `DATA_BYTES_PER_S` / `DATA_BURST` | 8 MiB / 4 MiB | game-data byte budget per connection |
| `SEND_BUF_MAX` | 8 MiB | kill a client whose outbox backs up past this |
| `MAX_FRAGMENTS` | 256 | continuation frames allowed per message |
| `HANDSHAKE_MS` | 10000 | time allowed to finish the HTTP upgrade |
| `TRUST_PROXY` | off | read `X-Forwarded-For` for the real client IP — **turn on when behind a proxy/CDN** (Render, Caddy, Cloudflare), otherwise the per-IP caps see only the proxy's IP. Leave off for a direct VPS, where `X-Forwarded-For` would be attacker-spoofable. |
| `ALLOWED_ORIGINS` | empty (all) | comma-separated `Origin` allowlist for browser/web builds |

## Staying up (so the relay doesn't "go down" and disconnect players)

- **Never crash on one bad event.** A `process.on('uncaughtException')` / `unhandledRejection` handler
  logs the error and keeps the server running, so a single malformed client or edge case can't take
  down every room at once.
- **Beat free-tier idle sleep.** Render's free plan spins the service down after ~15 min without
  traffic; the next player then hits a ~1 min cold start. The relay pings its own public URL every 10
  min (`RENDER_EXTERNAL_URL`, set by Render automatically, or `KEEPALIVE_URL`) to stay awake.
- **Clean restarts.** On `SIGTERM`/`SIGINT` (a deploy or instance recycle) the relay sends every client
  a 1012 "service restart" close frame instead of dropping them cold.
- **Fast dead-player detection.** Every socket is pinged every 5 s (`PING_MS`); one that sends nothing
  (no game data, no ping reply) until the next ping is dropped, so the rest of the room hears within
  5–10 s when a host crashes or a phone loses signal. This matters on Render, whose proxy doesn't pass a
  dropped connection on to the relay. Any message from a player counts as a sign of life, so busy
  players are never dropped.

| Env | Default | Meaning |
|-----|---------|---------|
| `KEEPALIVE_URL` | `RENDER_EXTERNAL_URL` | public URL the relay pings to stay awake (empty = off) |
| `KEEPALIVE_MS` | 600000 | how often to self-ping (10 min) |
| `PING_MS` | 5000 | how often to ping players; silent for one interval = dropped (5–10 s) |

## Still on you (not something code can do for you)

- **TLS**: keep the public relay on `wss://` (Render does this; a VPS needs Caddy/Nginx in front).
- **If you ever add accounts or private data to the relay**, revisit this — right now there is nothing
  secret in a room, which is why no per-user auth is needed.
