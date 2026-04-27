"""Cloudflare R2 storage backend for raw document archiving.

This module handles uploading raw source files (HTML, XML, PDF) to R2 before conversion.
The raw files are preserved for provenance and re-processing.

Structure in R2:
    us/statutes/states/{state}/{title}/{section}.html
    us/statutes/federal/{title}/{section}.xml
    us/guidance/irs/{type}/{doc_id}.pdf
    us/regulations/cfr/{title}/{part}.xml
"""

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError


class R2Storage:
    """Cloudflare R2 storage for raw document archiving.

    Example:
        >>> r2 = R2Storage.from_config()
        >>> r2.upload_raw("us/statutes/states/ak/43/05/010.html", html_content)
        >>> r2.upload_raw("us/guidance/irs/notices/n-24-01.pdf", pdf_bytes)
    """

    def __init__(
        self,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
        bucket: str = "atlas",
    ):
        """Initialize R2 storage.

        Args:
            endpoint_url: R2 S3-compatible endpoint
            access_key_id: R2 access key
            secret_access_key: R2 secret key
            bucket: Bucket name (default: atlas)
        """
        self.bucket = bucket
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    @classmethod
    def from_config(cls, config_path: str | Path | None = None) -> "R2Storage":
        """Create R2Storage from config file.

        Args:
            config_path: Path to credentials JSON. Defaults to ~/.config/axiom-foundation/r2-credentials.json

        Returns:
            Configured R2Storage instance
        """
        if config_path is None:
            config_path = Path.home() / ".config" / "axiom-foundation" / "r2-credentials.json"

        with open(config_path) as f:
            creds = json.load(f)

        return cls(
            endpoint_url=creds["endpoint_url"],
            access_key_id=creds["access_key_id"],
            secret_access_key=creds["secret_access_key"],
            bucket=creds.get("bucket", "atlas"),
        )

    def upload_raw(
        self,
        key: str,
        content: bytes | str,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Upload raw content to R2.

        Args:
            key: Object key (path in bucket)
            content: Raw content bytes or string
            content_type: MIME type (auto-detected if not provided)
            metadata: Optional metadata dict

        Returns:
            The object key
        """
        if isinstance(content, str):
            content = content.encode('utf-8')

        # Auto-detect content type
        if content_type is None:
            if key.endswith('.html'):
                content_type = 'text/html; charset=utf-8'
            elif key.endswith('.xml'):
                content_type = 'application/xml'
            elif key.endswith('.pdf'):
                content_type = 'application/pdf'
            elif key.endswith('.json'):
                content_type = 'application/json'
            else:
                content_type = 'application/octet-stream'

        # Build metadata
        upload_metadata = {
            'uploaded-at': datetime.now(timezone.utc).isoformat(),
            'content-hash': hashlib.sha256(content).hexdigest()[:16],
        }
        if metadata:
            upload_metadata.update(metadata)

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
            Metadata=upload_metadata,
        )

        return key

    def upload_state_statute(
        self,
        state: str,
        section_id: str,
        html: str,
        source_url: str,
        fetched_at: datetime | None = None,
    ) -> str:
        """Upload a state statute's raw HTML.

        Args:
            state: State code (e.g., 'ak', 'ny')
            section_id: Section identifier (e.g., '43.05.010', '12-41')
            html: Raw HTML content
            source_url: URL the content was fetched from
            fetched_at: When the content was fetched (default: now)

        Returns:
            The R2 object key
        """
        # Normalize section_id for path safety
        safe_id = section_id.replace('/', '-').replace('.', '-')
        key = f"us/statutes/states/{state.lower()}/{safe_id}.html"

        metadata = {
            'source-url': source_url[:256],  # R2 metadata value limit
            'state': state.lower(),
            'section-id': section_id,
            'fetched-at': (fetched_at or datetime.now(timezone.utc)).isoformat(),
        }

        return self.upload_raw(key, html, metadata=metadata)

    def upload_federal_statute(
        self,
        title: int,
        section: str,
        xml: str,
        source_url: str,
    ) -> str:
        """Upload US Code section XML.

        Args:
            title: USC title number
            section: Section number
            xml: Raw XML content
            source_url: Source URL

        Returns:
            The R2 object key
        """
        safe_section = section.replace('/', '-')
        key = f"us/statutes/federal/{title}/{safe_section}.xml"

        metadata = {
            'source-url': source_url[:256],
            'title': str(title),
            'section': section,
        }

        return self.upload_raw(key, xml, metadata=metadata)

    def upload_guidance(
        self,
        doc_type: str,
        doc_id: str,
        content: bytes,
        source_url: str,
    ) -> str:
        """Upload IRS guidance document.

        Args:
            doc_type: Document type ('notices', 'rev-proc', 'rev-rul', 'publications')
            doc_id: Document ID (e.g., 'n-24-01', 'rev-proc-2024-1')
            content: PDF or HTML content
            source_url: Source URL

        Returns:
            The R2 object key
        """
        ext = 'pdf' if content[:4] == b'%PDF' else 'html'
        key = f"us/guidance/irs/{doc_type}/{doc_id}.{ext}"

        metadata = {
            'source-url': source_url[:256],
            'doc-type': doc_type,
            'doc-id': doc_id,
        }

        return self.upload_raw(key, content, metadata=metadata)

    def exists(self, key: str) -> bool:
        """Check if an object exists in R2.

        Args:
            key: Object key

        Returns:
            True if object exists
        """
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def get(self, key: str) -> bytes:
        """Get object content from R2.

        Args:
            key: Object key

        Returns:
            Object content as bytes
        """
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response['Body'].read()

    def list_prefix(self, prefix: str, max_keys: int = 1000) -> list[dict[str, Any]]:
        """List objects with a given prefix.

        Args:
            prefix: Key prefix to filter by
            max_keys: Maximum objects to return

        Returns:
            List of object info dicts
        """
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix,
            MaxKeys=max_keys,
        )
        return response.get('Contents', [])

    def get_state_stats(self, state: str) -> dict[str, int]:
        """Get statistics for a state's archived content.

        Args:
            state: State code

        Returns:
            Dict with 'count' and 'total_bytes'
        """
        prefix = f"us/statutes/states/{state.lower()}/"
        objects = self.list_prefix(prefix)
        return {
            'count': len(objects),
            'total_bytes': sum(obj['Size'] for obj in objects),
        }


# Convenience functions for scripts
def get_r2() -> R2Storage:
    """Get R2Storage for atlas bucket (legal documents)."""
    return R2Storage.from_config()


def get_r2_atlas() -> R2Storage:
    """Get R2Storage for atlas bucket (legal documents - statutes, guidance, regulations)."""
    return R2Storage.from_config()
