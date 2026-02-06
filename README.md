# Universal Lead & Worker Dispatch Automation (ULWDA)

A production-ready, zero-cost automation system for lead extraction, worker management, and job dispatch across various industries.

## Features

- **Lead Collection**: Automated extraction of business leads from OpenStreetMap (via Nominatim API).
- **Worker Management**: Import and manage worker profiles with skills, location, and contact details.
- **Auto-Matching**: Intelligent matching of nearest qualified workers to leads.
- **Outreach Automation**:
  - **WhatsApp**: Automated messaging via `pywhatkit`.
  - **Email**: Automated email dispatch.
- **Security**: SQL injection prevention, input sanitization, and secure credential handling.
- **Zero Cost**: Uses free APIs and local resources (SQLite, Gmail SMTP).

## Installation

1. **Dependencies**:

    ```bash
    pip install requests pywhatkit
    ```

2. **Database**: The system automatically initializes `ulwda_production.db` on first run.

## Usage

### Collect Leads

```bash
python lead_dispatch_system.py collect --city "Mumbai" --service "hotel" --limit 20
```

### Import Workers

Create a CSV (`workers.csv`) with columns: `name,skills,phone,email,lat,lon`

```bash
python lead_dispatch_system.py import-workers workers.csv
```

### Match and Dispatch

```bash
python lead_dispatch_system.py match --service "plumbing"
```

### Send Outreach

```bash
python lead_dispatch_system.py send-whatsapp 1 --city "Mumbai" --service "plumbing"
```

## Architecture

- **LeadCollector**: Handles interaction with OpenStreetMap API using caching and rate limiting.
- **WorkerManager**: Manages worker data and imports.
- **JobDispatcher**: core logic for matching workers to leads based on location and skills.
- **OutreachManager**: Handles communication via WhatsApp and Email.
- **DatabaseManager**: Secure SQLite wrapper.

## License

MIT
