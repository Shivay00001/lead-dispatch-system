# Universal Lead & Worker Dispatch Automation (ULWDA) v2.0

A production-ready, zero-cost automation system for lead extraction, worker management, and job dispatch across any industry (hotels, plumbing, cleaning, maintenance, logistics, delivery, etc.).

## 🚀 Key Features

- **Lead Collection**: Automatically extract business leads from OpenStreetMap using the Nominatim API.
- **Worker Management**: Import and register service workers with their specific skills and geographic coordinates via CSV.
- **Auto-Matching**: Automatically matches collected business leads with the nearest available workers based on Haversine distance.
- **Automated Outreach**: Send WhatsApp messages (via `pywhatkit`) and emails directly to matched leads.
- **Local Secure Storage**: Uses a secure local SQLite database for data persistence and caching to avoid API rate limits.
- **Multi-lingual Templates**: Built-in support for Hindi and English professional outreach messages.

## ⚙️ Prerequisites

- Python 3.9+
- A modern terminal/command prompt supporting UTF-8 (set `PYTHONIOENCODING=utf-8` if on Windows)
- `requests` and `pywhatkit` packages

## 📦 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Shivay00001/lead-dispatch-system.git
   cd lead-dispatch-system
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## 💻 Usage

Run the ULWDA system through the command-line interface `lead_dispatch_system.py`.

### 1. Collect Leads
Extract leads for a specific service in a specific city:
```bash
python lead_dispatch_system.py collect --city "Mumbai" --service "hotel" --limit 20
```

### 2. Import Workers
Import your worker database from a CSV file (format: `name,skills,phone,email,lat,lon`):
```bash
python lead_dispatch_system.py import-workers workers.csv
```

### 3. Auto-Match Jobs
Assign the nearest registered workers to your collected leads:
```bash
python lead_dispatch_system.py match --service "plumbing"
```

### 4. Send Outreach
Message the matched leads via WhatsApp or Email:
```bash
python lead_dispatch_system.py send-whatsapp 1 --city "Mumbai" --service "plumbing"
```

### View Stats & Help
```bash
python lead_dispatch_system.py stats
python lead_dispatch_system.py --help
```

## ⚖️ Ethical & Legal Use
This tool is designed to be ethical and GDPR-friendly. It exclusively uses free, legal, and open-source APIs like OpenStreetMap, avoids scraping proprietary maps, implements strict API rate limiting, and securely hashes sensitive metrics. Always ensure you comply with local anti-spam and telemarketing regulations before contacting leads.

## License
This project is licensed under the terms provided in the LICENSE file.
