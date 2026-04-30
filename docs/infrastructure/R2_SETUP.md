# Cloudflare R2 Setup

Axiom Corpus uses Cloudflare R2 for storing raw source files (PDFs, XML, ZIPs).

## Bucket Configuration

| Setting | Value |
|---------|-------|
| Bucket name | `axiom-corpus` |
| Region | Auto (global) |
| Storage class | Standard |

## Directory Structure

The current corpus pipeline maps the local `data/corpus` artifact layout
directly into the R2 bucket:

```text
axiom-corpus (R2 bucket)/
├── sources/{jurisdiction}/{document_class}/{run_id}/...
├── inventory/{jurisdiction}/{document_class}/{run_id}.json
├── provisions/{jurisdiction}/{document_class}/{version}.jsonl
├── coverage/{jurisdiction}/{document_class}/{version}.json
├── exports/{format}/{jurisdiction}/{document_class}/{version}/...
├── analytics/{version}.json
└── snapshots/...
```

## Status

✅ **Bucket created**: 2024-12-28
✅ **API credentials configured**: R2 API token
✅ **Initial data loaded**: 11 objects, 61.5 MB

## API Credentials

Credentials are stored locally at `~/.config/axiom-foundation/r2-credentials.json`

Environment variables for scripts:

```bash
# Load from config file
export R2_ACCOUNT_ID="010d8d7f3b423be5ce36c7a5a49e91e4"
export R2_ENDPOINT="https://010d8d7f3b423be5ce36c7a5a49e91e4.r2.cloudflarestorage.com"
export R2_BUCKET="axiom-corpus"
# Access key and secret from ~/.config/axiom-foundation/r2-credentials.json
```

For CI/CD, add secrets:
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`

## Wrangler CLI

For bucket management, use the configured Cloudflare API credentials:

```bash
export CLOUDFLARE_API_TOKEN="<cloudflare-api-token>"
wrangler r2 bucket list
```

## Python Client

Use `boto3` with S3-compatible endpoint:

```python
import boto3
import os

s3 = boto3.client(
    's3',
    endpoint_url=os.environ['R2_ENDPOINT'],
    aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],
)

# Upload a file
s3.upload_file(
    'local-file.pdf',
    'axiom-corpus',
    'sources/guidance/irs/rev-proc/rev-proc-2024-01.pdf'
)

# Download a file
s3.download_file(
    'axiom-corpus',
    'sources/statutes/us/usc/26/32.xml',
    'local-copy.xml'
)

# List files
response = s3.list_objects_v2(
    Bucket='axiom-corpus',
    Prefix='sources/guidance/irs/'
)
for obj in response.get('Contents', []):
    print(obj['Key'])
```

## Integration with Axiom Corpus

Corpus R2 operations are dry-run first. Use `--apply` to upload:

```bash
# Plan uploads for all corpus artifact prefixes
axiom-corpus-ingest sync-r2 --base data/corpus

# Upload one or more prefixes
axiom-corpus-ingest sync-r2 --base data/corpus --prefix sources --prefix inventory --apply

# Compare local artifacts, R2 objects, coverage, and Supabase counts
axiom-corpus-ingest artifact-report \
  --base data/corpus \
  --version 2026-04-30 \
  --supabase-counts data/corpus/snapshots/provision-counts-2026-04-30.json \
  --include-r2
```

## Related Documentation

- [Source Organization](./architecture/source-organization.md) - Document structure
- [PostgreSQL Schema](../../schema/) - Metadata storage
