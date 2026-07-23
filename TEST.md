# Testing dbfailsim

This guide covers testing at three levels, from cheapest to most realistic:

1. [Automated tests](#1-automated-tests) — no database needed, seconds
2. [Smoke test without a database](#2-smoke-test-without-a-database) — any TCP service as a stand-in
3. [End-to-end with real databases](#3-end-to-end-with-real-databases) — locally
   via Docker, or hosted on [Neon](#3c-neon-hosted-postgres) / [a VPS](#3d-postgres-on-a-vps)

Plus a [per-fault verification checklist](#4-what-each-fault-should-look-like)
and [troubleshooting](#5-troubleshooting).

---

## 1. Automated tests

Every package has a test suite. The proxy tests spin up a real TCP echo
server and assert forwarding, latency timing, partition severing/refusal,
and heal recovery; the control tests exercise every HTTP endpoint.

```bash
go test ./...              # full suite
go test -race -count=1 ./...   # with the race detector (recommended — the proxy is concurrent)
go vet ./...
```

Expected: all packages `ok`, no vet findings. This is the fastest signal
after any code change and needs no database.

## 2. Smoke test without a database

The proxy is protocol-agnostic, so *any* TCP listener works as an upstream.
Useful for verifying the serve/inject/heal loop without touching a database:

```bash
# Terminal 1: a fake "database" that echoes bytes back
ncat -l -k 127.0.0.1 5555 --exec /bin/cat    # or: socat TCP-LISTEN:5555,fork,reuseaddr EXEC:/bin/cat

# Terminal 2: minimal config + serve
cat > smoke.json <<'EOF'
{
  "control_addr": "127.0.0.1:8080",
  "nodes": [
    {"name": "fake", "listen_addr": "127.0.0.1:6555", "upstream_addr": "127.0.0.1:5555"}
  ]
}
EOF
go build -o dbfailsim ./cmd/dbfailsim
./dbfailsim serve --config smoke.json

# Terminal 3: talk through the proxy and inject faults
ncat 127.0.0.1 6555                    # type a line, see it echoed
./dbfailsim fault --config smoke.json --node fake --kind latency --value 2000
# ... typed lines now echo back ~2s late
./dbfailsim fault --config smoke.json --node fake --kind partition
# ... the ncat session is severed; reconnecting fails
./dbfailsim heal --config smoke.json
```

## 3. End-to-end with real databases

### 3a. The included Docker harness (recommended first stop)

`docker/` runs a real Postgres 16 primary, a real streaming replica (built
with `pg_basebackup`, not simulated), and dbfailsim fronting both.

```bash
cd docker
docker compose up --build
```

Endpoints once it's up:

| What | Address |
|---|---|
| Dashboard + control API | http://localhost:8080 |
| Primary **via proxy** (point apps here) | `localhost:6432` |
| Replica **via proxy** | `localhost:6433` |
| Primary directly (setup/debug only) | `localhost:15432` |
| Replica directly | `localhost:5433` |

Walkthrough:

```bash
# 1. Confirm both nodes are reachable through the proxies
psql "postgresql://appuser:apppass@localhost:6432/appdb" -c "SELECT 1"
psql "postgresql://appuser:apppass@localhost:6433/appdb" -c "SELECT 1"

# 2. Confirm replication is live: write via the primary proxy, read via the replica proxy
psql "postgresql://appuser:apppass@localhost:6432/appdb" \
  -c "UPDATE accounts SET balance = balance + 1 WHERE id = 1"
psql "postgresql://appuser:apppass@localhost:6433/appdb" \
  -c "SELECT balance FROM accounts WHERE id = 1"

# 3. Inject faults (dashboard buttons, or curl — the harness config sets
#    control_token "chaos-demo-token"; the dashboard prompts for it once)
AUTH='Authorization: Bearer chaos-demo-token'
curl -H "$AUTH" -X POST localhost:8080/nodes/replica/fault \
  -d '{"kind":"latency","latency_ms":2000}'
curl -H "$AUTH" -X POST localhost:8080/scenarios/replica-lag/run

# 4. Consistency check across both nodes
curl -H "$AUTH" "localhost:8080/check?query=SELECT%20balance%20FROM%20accounts%20WHERE%20id=1"

# 5. Recover
curl -H "$AUTH" -X POST localhost:8080/heal
```

**Important caveat — what the faults do and don't touch here.** The proxies
sit on the *client* path. The replication stream (replica → primary) runs
directly between the Postgres containers and does **not** pass through a
proxy. So `replica-lag` makes clients of the replica slow/lossy; it does
not literally delay WAL apply. Divergence in step 4 is therefore timing
luck, not guaranteed.

**To force deterministic divergence** (the tool's payoff, on demand), pause
WAL replay on the replica directly, write to the primary, and check:

```bash
psql "postgresql://appuser:apppass@localhost:5433/appdb" -c "SELECT pg_wal_replay_pause()"
psql "postgresql://appuser:apppass@localhost:6432/appdb" \
  -c "UPDATE accounts SET balance = 999 WHERE id = 1"
curl -H "$AUTH" "localhost:8080/check?query=SELECT%20balance%20FROM%20accounts%20WHERE%20id=1"
# => "agree": false — replica still serves the old balance

psql "postgresql://appuser:apppass@localhost:5433/appdb" -c "SELECT pg_wal_replay_resume()"
```

(If you want proxy faults to affect replication itself, point the replica's
`primary_conninfo` at a dbfailsim proxy in front of the primary instead of
at `postgres-primary` directly — then latency/drop genuinely delays WAL.)

### 3b. Your own local Postgres (no Docker)

Any two local Postgres instances work; without replication between them you
can still test every fault kind, just not replica-lag divergence.

```bash
# Two throwaway instances is easiest via Docker even if you skip the harness:
docker run -d --name pg1 -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres:16
docker run -d --name pg2 -e POSTGRES_PASSWORD=pass -p 5433:5432 postgres:16
```

Then adapt `config.example.json`: `upstream_addr` `127.0.0.1:5432` /
`127.0.0.1:5433`, and `check_command` like:

```
psql "postgresql://postgres:pass@127.0.0.1:5432/postgres" -t -c "{query}"
```

Run `./dbfailsim serve --config config.json` and point `psql` at the
proxy ports (`6432`/`6433`).

### 3c. Neon-hosted Postgres

Works, with one important wrinkle. Neon's endpoints require TLS **and use
SNI (the hostname inside the TLS handshake) to route your connection**.
TLS itself passes through the proxy untouched — it's just bytes — but when
your client connects to `127.0.0.1:6432`, it no longer sends the Neon
hostname via SNI, and Neon can't route the connection. Neon's documented
workaround for SNI-less clients is passing the endpoint id in `options`.

Config:

```json
{
  "control_addr": "127.0.0.1:8080",
  "nodes": [
    {
      "name": "neon-primary",
      "listen_addr": "127.0.0.1:6432",
      "upstream_addr": "ep-your-endpoint-123456.us-east-2.aws.neon.tech:5432",
      "check_command": "psql \"postgresql://user:pass@ep-your-endpoint-123456.us-east-2.aws.neon.tech/dbname?sslmode=require\" -t -c \"{query}\""
    }
  ]
}
```

Connecting **through the proxy** (note the `options=endpoint` addition):

```bash
psql "postgresql://user:pass@127.0.0.1:6432/dbname?sslmode=require&options=endpoint%3Dep-your-endpoint-123456"
```

Notes:

- `check_command` connects to Neon *directly* (that's the point — it
  reports true database state), so it needs no workaround.
- Latency, drop, partition, and crash all behave normally — the proxy
  doesn't care that the stream is TLS.
- For divergence testing, create a **Neon read replica** and add it as a
  second node with its own endpoint; Neon replicas can lag under write
  load, and `check` will surface it. You cannot pause WAL replay on Neon
  (managed service), so divergence is opportunistic rather than forced.
- Expect the injected latency to stack on top of real network latency to
  Neon; use larger values (2000ms+) so the effect is unambiguous.

### 3d. Postgres on a VPS

Two options for reaching the database:

**Option A — SSH tunnel (recommended).** No firewall changes, credentials
never cross the network un-tunneled:

```bash
ssh -N -L 5432:localhost:5432 you@your-vps &
```

Then treat it exactly like a local database: `upstream_addr` is
`127.0.0.1:5432`, `check_command` uses `127.0.0.1:5432`. Add a second
tunnel (`-L 5433:localhost:5433`) if the VPS also runs a replica.

**Option B — direct connection.** Set `listen_addresses = '*'` in
`postgresql.conf`, add your IP to `pg_hba.conf` (use `scram-sha-256`, never
`trust`), and firewall port 5432 to your IP only. Then `upstream_addr` is
`your-vps-ip:5432`.

For real replica-lag testing, set up streaming replication between two VPS
instances (`pg_basebackup -R` — see `docker/postgres/replica-entrypoint.sh`
for a working recipe) and add both as nodes. Since you control the replica,
the `pg_wal_replay_pause()` trick from §3a works here too.

**Security notes (read before testing against anything remote):**

- Set `control_token` in the config (or `DBFAILSIM_CONTROL_TOKEN`) so the
  control API requires `Authorization: Bearer <token>`. Without it the API
  is open — keep `control_addr` on `127.0.0.1` and never expose port 8080
  beyond your machine, since anyone who can reach it can inject faults and
  `/check` executes shell commands from your config. The Docker harness
  ships with token `chaos-demo-token`.
- The proxy `listen_addr`s should also stay on `127.0.0.1` when the
  upstream is a real hosted database, or anyone on your network can reach
  the database through your proxy.
- Only ever point dbfailsim at **test databases**. `partition` and `crash`
  sever live client connections by design.

## 4. What each fault should look like

Inject each fault against a node, then verify the observable behavior:

| Fault | How to verify | Expected observation |
|---|---|---|
| `latency` 2000ms | `time psql ... -c "SELECT 1"` through the proxy | Completes, but takes ~2s+ (delay applies per chunk, per direction) |
| `drop` 20% | Run a query loop through the proxy | Intermittent stalls/errors; some connections hang mid-protocol. Messy on purpose — a lossy link corrupts the stream, it doesn't fail cleanly |
| `partition` | New `psql` connection + one already open | New: connection refused/immediately closed. Existing: severed instantly (`server closed the connection unexpectedly`) |
| `crash` | Same as partition | Same behavior; reported as `crashed` in `/status` — distinct label, same mechanics |
| `heal` | Reconnect after any of the above | New connections succeed immediately; severed clients must reconnect themselves |
| scenario | `./dbfailsim inject --scenario primary-crash` then watch dashboard | Steps land at their `after_ms` offsets (e.g. crash fires 3s after start); `/status` reflects each step as it applies |

A query loop for observing faults live:

```bash
while true; do
  psql "postgresql://appuser:apppass@localhost:6432/appdb" -t -c "SELECT now()" || echo FAILED
  sleep 0.5
done
```

## 5. Troubleshooting

- **`connection refused` on a proxy port** — check `/status`: the node may
  be partitioned/crashed from an earlier test. `POST /heal` first.
- **Queries hang forever under `drop`** — expected: a dropped chunk stalls
  the wire protocol and the client waits. Set a client-side
  `statement_timeout`/`connect_timeout` in your test loop.
- **`check` returns errors for every node** — `check_command` runs via
  `sh -c` on the machine running `dbfailsim serve` (inside the container
  for the Docker harness). The client binary (`psql`, `mysql`) must exist
  *there*, and addresses must be resolvable *from there*.
- **Neon: `ERROR: Endpoint ID is not specified`** — you connected through
  the proxy without the `options=endpoint%3D...` parameter (see §3c).
- **Docker harness replica never comes up** — the harness is documented as
  not yet run end-to-end (see README). Check
  `docker compose logs postgres-replica`; the replica clones via
  `pg_basebackup` on first boot, and a stale `replica-data` volume from a
  failed attempt can wedge it (`docker compose down -v` resets).
- **Ports already in use** — the harness ports (15432/5433/6432/6433/8080)
  may collide with a locally running Postgres or another dev server; adjust
  the compose file / config, they're all arbitrary.
