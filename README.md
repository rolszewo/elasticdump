# elasticdump.py

## Description

Script to dump elasicsearch indices to a compressed ndjson file

## key features

- ✅ Auto-calculated optimal slices based on document count
- ✅ Pattern matching for bulk exports (logs-*, prod-*, etc.)
- ✅ Environment variable support (`ES_URL, ES_USERNAME, ES_PASSWORD`)
- ✅ Fast binary gzip concatenation (10-100x faster)
- ✅ Handles small indices (no slice API errors)
- ✅ Per-index combined files with `--combine`
- ✅ Comprehensive statistics and verification

## HowTo

```bash
# prepare (only once)
python3 -m venv elastic_venv
source elastic_venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
chmod 755 elasticdump.py

# use
source elastic_venv/bin/activate
export ES_URL=http://localhost:9200
export ES_USERNAME=elastic
export ES_PASSWORD="secret"
python3 elasticdump.py --index myindex-104-prod-2025.10.18 --combine 
```
