# Cloudflare R2 Setup

Arch uses Cloudflare R2 for storing raw source files (PDFs, XML, ZIPs).

## Bucket Configuration

| Setting | Value |
|---------|-------|
| Bucket name | `arch` |
| Region | Auto (global) |
| Storage class | Standard |

## Directory Structure

```
arch (R2 bucket)/
├── sources/
│   ├── statutes/
│   │   ├── us/
│   │   │   ├── usc/            # US Code USLM XML
│   │   │   │   ├── 26/         # Title 26 (IRC)
│   │   │   │   ├── 7/          # Title 7 (Agriculture)
│   │   │   │   └── ...
│   │   │   └── cfr/            # Code of Federal Regulations
│   │   └── states/
│   │       ├── ny/             # New York statutes
│   │       ├── ca/             # California statutes
│   │       └── ...
│   │
│   ├── guidance/
│   │   ├── irs/
│   │   │   ├── rev-proc/       # Revenue Procedures
│   │   │   ├── rev-rul/        # Revenue Rulings
│   │   │   ├── notices/        # IRS Notices
│   │   │   └── publications/   # IRS Publications
│   │   └── usda/
│   │       └── fns/            # Food & Nutrition Service
│   │
│   ├── microdata/
│   │   ├── cps-asec/           # Current Population Survey ASEC
│   │   ├── acs/                # American Community Survey
│   │   └── scf/                # Survey of Consumer Finances
│   │
│   └── crosstabs/
│       ├── soi/                # IRS Statistics of Income
│       └── census/             # Census Bureau tables
```

## Status

✅ **Bucket created**: 2024-12-28
✅ **API credentials configured**: `arch-s3-api` token
✅ **Data migrated** from legacy bucket (11 objects, 61.5 MB)

## API Credentials

Credentials are stored locally at `~/.config/axiom-foundation/r2-credentials.json`

Environment variables for scripts:

```bash
# Load from config file
export R2_ACCOUNT_ID="010d8d7f3b423be5ce36c7a5a49e91e4"
export R2_ENDPOINT="https://010d8d7f3b423be5ce36c7a5a49e91e4.r2.cloudflarestorage.com"
export R2_BUCKET="arch"
# Access key and secret from ~/.config/axiom-foundation/r2-credentials.json
```

For CI/CD, add secrets:
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`

## Wrangler CLI

For bucket management, use the `arch-r2` Cloudflare API token:

```bash
export CLOUDFLARE_API_TOKEN="<arch-r2-token>"
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
    'arch',
    'sources/guidance/irs/rev-proc/rev-proc-2024-01.pdf'
)

# Download a file
s3.download_file(
    'arch',
    'sources/statutes/us/usc/26/32.xml',
    'local-copy.xml'
)

# List files
response = s3.list_objects_v2(
    Bucket='arch',
    Prefix='sources/guidance/irs/'
)
for obj in response.get('Contents', []):
    print(obj['Key'])
```

## Integration with Arch

The `arch` CLI will support R2 operations:

```bash
# Upload local data to R2
arch sync --to-r2

# Download from R2 to local
arch sync --from-r2

# Upload specific source type
arch sync --to-r2 --type=guidance
```

## Related Documentation

- [Source Organization](./architecture/source-organization.md) - Document structure
- [PostgreSQL Schema](../../schema/) - Metadata storage
