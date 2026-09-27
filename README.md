# Universal Lead & Worker Dispatch Automation (ULWDA) v2.0

Lead extraction (OpenStreetMap/Nominatim), worker management, distance-based
job matching, and WhatsApp/SMTP email outreach — backed by a local SQLite
database. CLI-first; no servers, no HTTP endpoints.

## 🚀 Quick start (fresh clone)

```bash
git clone https://github.com/Shivay00001/lead-dispatch-system.git
cd lead-dispatch-system
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Configure (copy the example, fill in your values)
cp .env.example .env

# 2. Verify the install
python lead_dispatch_system.py --help
python lead_dispatch_system.py stats
```

That's it — no tribal knowledge. The SQLite database (`ulwda_production.db`
by default) is created automatically on first run.

## ⚙️ Configuration — environment variables

All secrets and deployment settings come from environment variables or a `.env`
file in the working directory. **Nothing is hardcoded** — SMTP credentials
especially. A legacy `--smtp-config <file.json>` override is still accepted
for `send-email`.

| Variable               | Required | Default                  | Purpose                                          |
|------------------------|----------|--------------------------|--------------------------------------------------|
| `LEAD_DISPATCH_DB`     | no       | `ulwda_production.db`    | Path to the SQLite database file                 |
| `SMTP_HOST`            | yes¹     | `smtp.gmail.com`         | SMTP server hostname                             |
| `SMTP_PORT`            | no       | `587`                    | SMTP port (STARTTLS)                             |
| `SMTP_USER`            | yes¹     | —                        | SMTP login (e.g. your Gmail address)             |
| `SMTP_PASS`            | yes¹     | —                        | SMTP password — **use a Gmail App Password**, never your real login password |
| `SMTP_MIN_GAP_SECONDS` | no       | `30`                     | Min seconds between outbound emails in one run (abuse throttle; `0` disables) |

¹ required only for `send-email`. Gmail: enable 2-Step Verification, then
create an App Password at https://myaccount.google.com/apppasswords.

> `.env` is git-ignored — never commit it. `.env.example` shows every variable
> with dummy placeholder values.

## 💻 Usage

```bash
# Collect leads for a service in a city
python lead_dispatch_system.py collect --city "Mumbai" --service "hotel" --limit 20

# Import workers (CSV: name,skills,phone,email,lat,lon)
python lead_dispatch_system.py import-workers workers.csv

# Manually add one worker
python lead_dispatch_system.py add-worker --name "Ravi" --skills "plumbing" --phone "+919876543210"

# Auto-match nearest workers to new leads
python lead_dispatch_system.py match --service "plumbing"

# Send outreach (email needs SMTP configured; WhatsApp opens WhatsApp Web)
python lead_dispatch_system.py send-email 1 --city "Mumbai" --service "plumbing"
python lead_dispatch_system.py send-whatsapp 1 --city "Mumbai" --service "plumbing"

# Inspect
python lead_dispatch_system.py list-leads
python lead_dispatch_system.py list-workers
python lead_dispatch_system.py list-jobs
python lead_dispatch_system.py stats        # alias: status
python lead_dispatch_system.py cleanup

# Export to CSV
python lead_dispatch_system.py export --type leads --output leads.csv
```

Exit codes: `0` on success, non-zero on failure (e.g. `1` when an email can't
be sent). Errors print as `❌ ERROR: <what happened>` — stack traces are
never shown to end users; internal events are logged to the `system_logs`
table in the database.

## 🐳 Docker

```bash
cp .env.example .env   # fill in SMTP creds
docker compose build
docker compose run --rm dispatch stats
docker compose run --rm dispatch collect --city "Mumbai" --service hotel --limit 20
```

The database persists in the `lead_data` volume (mounted at `/data`, with
`LEAD_DISPATCH_DB=/data/ulwda_production.db` set in compose).

## ☁️ Deploying on Render (cron worker)

This repo is CLI-only (no web service), so deploy it as a **Render Cron Job**
or **Background Worker**:

1. Push the repo to GitHub, create a new **Cron Job** on Render from it.
2. Build command: `pip install -r requirements.txt`
3. Command (example — collect + match daily at 6:00 UTC):
   `python lead_dispatch_system.py collect --city "Mumbai" --service "hotel" --limit 50 && python lead_dispatch_system.py match --service hotel`
4. Set a schedule (cron expression) in the Render dashboard.
5. Add environment variables in the Render dashboard (don't commit `.env`):
   `SMTP_HOST`, `SMTP_USER`, `SMTP_PASS`, `SMTP_MIN_GAP_SECONDS`,
   and optionally `LEAD_DISPATCH_DB=/var/data/ulwda_production.db`.
6. Attach a **Persistent Disk** mounted at `/var/data` so the SQLite database
   survives between cron runs.

For outbound email in a worker: keep `SMTP_MIN_GAP_SECONDS` ≥ 30 so one run
can't blast the inbox provider's limits.

## ⏱️ Rate limits & abuse safety

- **Nominatim (lead collection):** hard-capped at 1 request per
  `API_RATE_LIMIT_SECONDS` (1.2s) — complies with the OpenStreetMap usage
  policy (max 1 req/sec). Results are cached 24h in the `api_cache` table.
- **Email:** at most one send per `SMTP_MIN_GAP_SECONDS` (default 30s) inside
  a single process run; failed sends are never retried automatically.
- **WhatsApp:** via `pywhatkit`, which opens a real WhatsApp Web session in
  your browser — each send is manual by design. On headless servers (no
  `DISPLAY`) WhatsApp is cleanly disabled with a plain-English message;
  every other command still works.

## ✅ Tests

```bash
bash tests/smoke_test.sh          # 11 CLI checks on a throwaway DB (help, stats/status, validation, creds error, no-traceback)
python tests/test_smtp_unit.py    # SMTP send path + throttle + DB logging, fake in-process SMTP server (no network)
```

## 🔒 Notes

- **No HTTP endpoint** — this tool exposes nothing to the network, so no API-key
  auth is needed. Input validation (email/phone/coordinate formats, length
  caps, parameterized SQL) applies to all CLI inputs and imported CSV/JSON.
- Outreach templates are plain-text Hindi/English business messages — no AI,
  no mock/fake sending: `send-email` really delivers via SMTP, `send-whatsapp`
  really opens WhatsApp Web.
- Ethical use: uses the free, legal OpenStreetMap API, respects its rate
  limits, stores data locally, and only contacts leads you have consent to
  contact. Follow your jurisdiction's anti-spam laws.

## License

MIT (see LICENSE).
