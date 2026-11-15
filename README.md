# Tempest Chronicles relay

Passes messages between the players of an online room. Plain Node.js, no packages: `node relay.js` (PORT env, default 8787).

It is hardened against abuse and against going down: per-IP/room caps, rate limits, slow-reader and
fragment-bomb guards, it survives unexpected errors instead of crashing, and it keeps itself awake on
free hosting by pinging its own URL (Render sets `RENDER_EXTERNAL_URL` automatically; a GitHub Actions
cron in `.github/workflows/keep-awake.yml` also pings it). Set `TRUST_PROXY=1` when behind a proxy
(Render does this via `render.yaml`). See [SECURITY.md](SECURITY.md) for the threat model and all env knobs.
