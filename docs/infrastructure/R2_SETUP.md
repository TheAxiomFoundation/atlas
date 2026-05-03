# Cloudflare R2 Setup

Axiom Corpus uses Cloudflare R2 for storing raw source files (PDFs, XML, ZIPs).

## Bucket Configuration

| Setting | Value |
|---------|-------|
| Bucket name | From `R2_BUCKET` or `~/.config/axiom-foundation/r2-credentials.json` |
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
├── snapshots/...
└── releases/{release}.artifacts.json
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
export R2_ACCOUNT_ID="011fb8d44f0e4d9832265ac9f748bc6b"
export R2_ENDPOINT="https://011fb8d44f0e4d9832265ac9f748bc6b.r2.cloudflarestorage.com"
export R2_BUCKET="<configured-corpus-bucket>"
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
    os.environ['R2_BUCKET'],
    'sources/guidance/irs/rev-proc/rev-proc-2024-01.pdf'
)

# Download a file
s3.download_file(
    os.environ['R2_BUCKET'],
    'sources/statutes/us/usc/26/32.xml',
    'local-copy.xml'
)

# List files
response = s3.list_objects_v2(
    Bucket=os.environ['R2_BUCKET'],
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

# Upload a scoped release safely
axiom-corpus-ingest sync-r2 \
  --base data/corpus \
  --jurisdiction us-co \
  --document-class policy \
  --version 2026-04-30 \
  --apply

# Use bounded concurrency for large source trees with many small files
axiom-corpus-ingest sync-r2 \
  --base data/corpus \
  --prefix sources \
  --jurisdiction us-dc \
  --document-class statute \
  --version 2026-04-29 \
  --workers 16 \
  --apply

# Compare local artifacts, R2 objects, coverage, and Supabase counts
axiom-corpus-ingest artifact-report \
  --base data/corpus \
  --supabase-counts data/corpus/snapshots/provision-counts-2026-04-30.json \
  --include-r2

# Write an immutable digest manifest for release artifacts
axiom-corpus-ingest release-artifact-manifest \
  --base data/corpus \
  --release current \
  --output data/corpus/releases/current.artifacts.json

# Validate a release before promotion/publication
axiom-corpus-ingest validate-release \
  --base data/corpus \
  --release current \
  --supabase-counts data/corpus/snapshots/provision-counts-2026-05-02.json \
  --include-r2
```

Unscoped artifact reports auto-use `manifests/releases/current.json` when it
exists. Add `--all-scopes` when you need an exhaustive diagnostic report that
includes old probes and superseded snapshots.

## Related Documentation

- [Source Organization](./architecture/source-organization.md) - Document structure
- [PostgreSQL Schema](../../schema/) - Metadata storage
