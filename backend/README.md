# Human Data Operations Dashboard

Real-time annotator performance dashboard powered by Encord SDK.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure .env (already done if you have the file)
#    Make sure ENCORD_SSH_KEY_PATH points to your Encord private key
#    Make sure ENCORD_PROJECT_HASHES has your project hash

# 3. Run the server
uvicorn main:app --reload --port 8000

# 4. Open the dashboard
open http://localhost:8000
```

## .env Configuration

```env
ENCORD_SSH_KEY_PATH=/path/to/your/encord-private-key.ed25519
ENCORD_DOMAIN=https://api.encord.com
ENCORD_PROJECT_HASHES=your-project-hash-here
PROJECT_CLIENT=ClientName
PROJECT_MODALITY=Audio
```

## What It Shows

- **KPI Strip**: Active projects, critical flags, warnings, annotator count
- **Overview Tab**: Project card with progress, rejection rate, TPT, status
- **Annotators Tab**: Per-annotator cards with rejection rate, TPT, throughput, and outlier flags
- **Agent Flags Tab**: Placeholder for Phase 2 automated QA agents

## Data Safety

⛔ **READ-ONLY** — No data is modified on Encord. No DELETE queries anywhere.
