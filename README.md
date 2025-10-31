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

## prepare

```bash
# prepare (only once)
python3 -m venv elastic_venv
source elastic_venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
chmod 755 elasticdump.py
```

## use

```bash
source elastic_venv/bin/activate
export ES_URL=http://localhost:9200
export ES_USERNAME=elastic
export ES_PASSWORD="secret"

./elasticdump.py --index myindex-104-prod-2025.10.18 --combine

./elasticdump.py --index "logs-2024-*" --list-only
./elasticdump.py --index "logs-2024-*" --combine
```

## help

```bash
usage: elasticdump.py [-h] [--url URL] --index INDEX [--slices SLICES] [--output OUTPUT] [--combine] [--username USERNAME] [--password PASSWORD] [--list-only]

Export Elasticsearch index(es) in parallel

options:
  -h, --help           show this help message and exit
  --url URL            Elasticsearch URL (env: ES_URL, default: http://localhost:9200)
  --index INDEX        Index name or pattern (use * for wildcard matching)
  --slices SLICES      Number of parallel slices (default: auto-calculate based on doc count)
  --output OUTPUT      Output directory (default: export/)
  --combine            Combine slices into single INDEXNAME.ndjson.gz file per index
  --username USERNAME  ES username (env: ES_USERNAME)
  --password PASSWORD  ES password (env: ES_PASSWORD)
  --list-only          Only list matching indices without exporting

Environment Variables:
  ES_URL       Elasticsearch URL (default: http://localhost:9200)
  ES_USERNAME  Elasticsearch username (if auth required)
  ES_PASSWORD  Elasticsearch password (if auth required)

Index Patterns:
  Without '*' - exact match:
    --index my-index              → exports only "my-index"
  
  With '*' - wildcard match:
    --index "logs-*"              → exports logs-2024-01, logs-2024-02, etc.
    --index "prod-*"              → exports all indices starting with "prod-"
    --index "*-archive"           → exports all indices ending with "-archive"

Examples:
  # Export single index
  elasticdump.py --index my-index

  # Export all matching indices with pattern
  elasticdump.py --index "logs-2024-*" --combine

  # List what would be exported (dry-run)
  elasticdump.py --index "prod-*" --list-only

  # With authentication
  export ES_USERNAME=elastic ES_PASSWORD=secret
  elasticdump.py --index "myapp-*" --combine

  # Custom output directory
  elasticdump.py --index "logs-*" --output /backup/es-export
  ```