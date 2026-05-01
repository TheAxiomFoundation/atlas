"""R2 artifact sync and inventory reporting for the corpus store."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

import boto3  # type: ignore[import-untyped]

from axiom_corpus.corpus.analytics import load_provision_count_snapshot
from axiom_corpus.corpus.releases import ScopeKey

DEFAULT_R2_BUCKET = "axiom-corpus"
DEFAULT_R2_ACCOUNT_ID = "010d8d7f3b423be5ce36c7a5a49e91e4"
DEFAULT_R2_CREDENTIAL_PATH = Path.home() / ".config" / "axiom-foundation" / "r2-credentials.json"
DEFAULT_ARTIFACT_PREFIXES = (
    "sources",
    "inventory",
    "provisions",
    "coverage",
    "exports",
    "analytics",
    "snapshots",
)


@dataclass(frozen=True)
class R2Config:
    bucket: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str


@dataclass(frozen=True)
class LocalArtifact:
    key: str
    path: Path
    size: int


@dataclass(frozen=True)
class RemoteArtifact:
    key: str
    size: int
    etag: str | None = None


@dataclass(frozen=True)
class R2SyncReport:
    dry_run: bool
    bucket: str
    endpoint_url: str
    prefixes: tuple[str, ...]
    local_count: int
    remote_count: int
    skipped_count: int
    candidate_upload_count: int
    planned_upload_count: int
    limited_upload_count: int
    uploaded_count: int
    bytes_planned: int
    bytes_uploaded: int
    uploaded_keys: tuple[str, ...]

    def to_mapping(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "bucket": self.bucket,
            "endpoint_url": self.endpoint_url,
            "prefixes": list(self.prefixes),
            "local_count": self.local_count,
            "remote_count": self.remote_count,
            "skipped_count": self.skipped_count,
            "candidate_upload_count": self.candidate_upload_count,
            "planned_upload_count": self.planned_upload_count,
            "limited_upload_count": self.limited_upload_count,
            "uploaded_count": self.uploaded_count,
            "bytes_planned": self.bytes_planned,
            "bytes_uploaded": self.bytes_uploaded,
            "uploaded_keys": list(self.uploaded_keys),
        }


@dataclass(frozen=True)
class ArtifactScopeRow:
    jurisdiction: str
    document_class: str
    version: str
    local_inventory: bool = False
    local_provisions: bool = False
    local_coverage: bool = False
    local_source_files: int = 0
    local_source_bytes: int = 0
    remote_inventory: bool | None = None
    remote_provisions: bool | None = None
    remote_coverage: bool | None = None
    remote_source_files: int | None = None
    remote_source_bytes: int | None = None
    coverage_complete: bool | None = None
    source_count: int | None = None
    provision_count: int | None = None
    matched_count: int | None = None
    missing_count: int | None = None
    extra_count: int | None = None
    supabase_count: int | None = None

    def to_mapping(self) -> dict[str, Any]:
        local_complete = self.local_inventory and self.local_provisions and self.local_coverage
        r2_complete = None
        if self.remote_inventory is not None:
            r2_complete = (
                self.remote_inventory
                and self.remote_provisions
                and self.remote_coverage
                and (self.local_source_files == 0 or bool(self.remote_source_files))
            )
        supabase_matches = None
        if self.supabase_count is not None and self.provision_count is not None:
            supabase_matches = self.supabase_count == self.provision_count
        return {
            "jurisdiction": self.jurisdiction,
            "document_class": self.document_class,
            "version": self.version,
            "local_inventory": self.local_inventory,
            "local_provisions": self.local_provisions,
            "local_coverage": self.local_coverage,
            "local_source_files": self.local_source_files,
            "local_source_bytes": self.local_source_bytes,
            "remote_inventory": self.remote_inventory,
            "remote_provisions": self.remote_provisions,
            "remote_coverage": self.remote_coverage,
            "remote_source_files": self.remote_source_files,
            "remote_source_bytes": self.remote_source_bytes,
            "coverage_complete": self.coverage_complete,
            "source_count": self.source_count,
            "provision_count": self.provision_count,
            "matched_count": self.matched_count,
            "missing_count": self.missing_count,
            "extra_count": self.extra_count,
            "supabase_count": self.supabase_count,
            "local_complete": local_complete,
            "r2_complete": r2_complete,
            "supabase_matches_provisions": supabase_matches,
            "mismatch_reasons": self.mismatch_reasons(),
        }

    def mismatch_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if not self.local_inventory:
            reasons.append("missing_local_inventory")
        if not self.local_provisions:
            reasons.append("missing_local_provisions")
        if not self.local_coverage:
            reasons.append("missing_local_coverage")
        if self.coverage_complete is False:
            reasons.append("coverage_incomplete")
        if (
            self.supabase_count is not None
            and self.provision_count is not None
            and self.supabase_count != self.provision_count
        ):
            reasons.append("supabase_count_mismatch")
        if self.remote_inventory is False:
            reasons.append("missing_r2_inventory")
        if self.remote_provisions is False:
            reasons.append("missing_r2_provisions")
        if self.remote_coverage is False:
            reasons.append("missing_r2_coverage")
        if self.local_source_files and self.remote_source_files == 0:
            reasons.append("missing_r2_sources")
        return tuple(reasons)


@dataclass(frozen=True)
class ArtifactReport:
    local_root: Path
    prefixes: tuple[str, ...]
    local_count: int
    local_bytes: int
    local_by_prefix: dict[str, dict[str, int]]
    remote_count: int | None
    remote_bytes: int | None
    remote_by_prefix: dict[str, dict[str, int]] | None
    rows: tuple[ArtifactScopeRow, ...]
    release_name: str | None = None
    release_scope_count: int | None = None

    def to_mapping(self) -> dict[str, Any]:
        mismatch_rows = [row for row in self.rows if row.mismatch_reasons()]
        payload = {
            "local_root": str(self.local_root),
            "prefixes": list(self.prefixes),
            "local_count": self.local_count,
            "local_bytes": self.local_bytes,
            "local_by_prefix": self.local_by_prefix,
            "remote_count": self.remote_count,
            "remote_bytes": self.remote_bytes,
            "remote_by_prefix": self.remote_by_prefix,
            "scope_count": len(self.rows),
            "mismatch_count": len(mismatch_rows),
            "mismatches": [row.to_mapping() for row in mismatch_rows],
            "rows": [row.to_mapping() for row in self.rows],
        }
        if self.release_name is not None:
            payload["release"] = self.release_name
            payload["release_scope_count"] = self.release_scope_count
        return payload


def load_r2_config(
    *,
    environ: Mapping[str, str] = os.environ,
    credential_path: str | Path | None = None,
    bucket: str | None = None,
    endpoint_url: str | None = None,
) -> R2Config:
    """Load R2 S3 credentials from env or the local Axiom credentials file."""
    credentials = _load_credential_file(credential_path)
    account_id = (
        environ.get("R2_ACCOUNT_ID")
        or _credential_value(credentials, "account_id", "accountId")
        or DEFAULT_R2_ACCOUNT_ID
    )
    resolved_endpoint = (
        endpoint_url
        or environ.get("R2_ENDPOINT")
        or _credential_value(credentials, "endpoint", "endpoint_url", "endpointUrl")
        or f"https://{account_id}.r2.cloudflarestorage.com"
    )
    access_key_id = (
        environ.get("R2_ACCESS_KEY_ID")
        or environ.get("AWS_ACCESS_KEY_ID")
        or _credential_value(credentials, "access_key_id", "accessKeyId", "accessKey")
    )
    secret_access_key = (
        environ.get("R2_SECRET_ACCESS_KEY")
        or environ.get("AWS_SECRET_ACCESS_KEY")
        or _credential_value(credentials, "secret_access_key", "secretAccessKey", "secretKey")
    )
    if not access_key_id or not secret_access_key:
        raise RuntimeError(
            "R2 credentials not found. Set R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY "
            "or configure ~/.config/axiom-foundation/r2-credentials.json."
        )
    return R2Config(
        bucket=bucket
        or environ.get("R2_BUCKET")
        or _credential_value(credentials, "bucket", "bucket_name", "bucketName")
        or DEFAULT_R2_BUCKET,
        endpoint_url=resolved_endpoint,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


def make_r2_client(config: R2Config) -> Any:
    """Create a boto3 S3-compatible client for Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        region_name="auto",
    )


def iter_local_artifacts(
    root: str | Path,
    *,
    prefixes: Iterable[str] = DEFAULT_ARTIFACT_PREFIXES,
) -> tuple[LocalArtifact, ...]:
    root_path = Path(root)
    rows: list[LocalArtifact] = []
    for prefix in _normalize_prefixes(prefixes):
        base = root_path / prefix
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            key = path.relative_to(root_path).as_posix()
            rows.append(LocalArtifact(key=key, path=path, size=path.stat().st_size))
    return tuple(rows)


def list_r2_artifacts(
    client: Any,
    *,
    bucket: str,
    prefixes: Iterable[str] = DEFAULT_ARTIFACT_PREFIXES,
) -> dict[str, RemoteArtifact]:
    artifacts: dict[str, RemoteArtifact] = {}
    paginator = client.get_paginator("list_objects_v2")
    for prefix in _normalize_prefixes(prefixes):
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            for obj in page.get("Contents", []):
                key = str(obj["Key"])
                artifacts[key] = RemoteArtifact(
                    key=key,
                    size=int(obj.get("Size", 0)),
                    etag=obj.get("ETag"),
                )
    return artifacts


def sync_artifacts_to_r2(
    root: str | Path,
    *,
    config: R2Config,
    client: Any | None = None,
    prefixes: Iterable[str] = DEFAULT_ARTIFACT_PREFIXES,
    jurisdiction: str | None = None,
    document_class: str | None = None,
    version: str | None = None,
    dry_run: bool = True,
    limit: int | None = None,
    workers: int = 1,
    force: bool = False,
    progress_stream: TextIO | None = None,
) -> R2SyncReport:
    """Upload missing or size-different corpus artifacts to R2."""
    prefix_tuple = _normalize_prefixes(prefixes)
    r2 = client or make_r2_client(config)
    local = tuple(
        artifact
        for artifact in iter_local_artifacts(root, prefixes=prefix_tuple)
        if _artifact_matches_scope(
            artifact.key,
            jurisdiction=jurisdiction,
            document_class=document_class,
            version=version,
        )
    )
    remote = _filter_remote_artifacts(
        list_r2_artifacts(r2, bucket=config.bucket, prefixes=prefix_tuple),
        jurisdiction=jurisdiction,
        document_class=document_class,
        version=version,
    )
    assert remote is not None
    upload_candidates = tuple(
        artifact
        for artifact in local
        if force or artifact.key not in remote or remote[artifact.key].size != artifact.size
    )
    planned = upload_candidates
    if limit is not None:
        planned = planned[:limit]
    uploaded: list[str] = []
    uploaded_bytes = 0
    if not dry_run:
        completed = _upload_artifacts(
            r2,
            bucket=config.bucket,
            artifacts=planned,
            workers=workers,
            progress_stream=progress_stream,
        )
        uploaded = [artifact.key for artifact in completed]
        uploaded_bytes = sum(artifact.size for artifact in completed)
    return R2SyncReport(
        dry_run=dry_run,
        bucket=config.bucket,
        endpoint_url=config.endpoint_url,
        prefixes=prefix_tuple,
        local_count=len(local),
        remote_count=len(remote),
        skipped_count=len(local) - len(upload_candidates),
        candidate_upload_count=len(upload_candidates),
        planned_upload_count=len(planned),
        limited_upload_count=len(upload_candidates) - len(planned),
        uploaded_count=len(uploaded),
        bytes_planned=sum(artifact.size for artifact in planned),
        bytes_uploaded=uploaded_bytes,
        uploaded_keys=tuple(uploaded),
    )


def build_artifact_report(
    root: str | Path,
    *,
    prefixes: Iterable[str] = DEFAULT_ARTIFACT_PREFIXES,
    version: str | None = None,
    jurisdiction: str | None = None,
    document_class: str | None = None,
    supabase_counts_path: str | Path | None = None,
    remote: Mapping[str, RemoteArtifact] | None = None,
    release_name: str | None = None,
    release_scopes: Iterable[ScopeKey] | None = None,
) -> ArtifactReport:
    prefix_tuple = _normalize_prefixes(prefixes)
    root_path = Path(root)
    release_scope_keys = _normalize_release_scopes(release_scopes)
    local = tuple(
        artifact
        for artifact in iter_local_artifacts(root_path, prefixes=prefix_tuple)
        if _artifact_matches_scope(
            artifact.key,
            jurisdiction=jurisdiction,
            document_class=document_class,
            version=version,
            release_scopes=release_scope_keys,
        )
    )
    scoped_remote = _filter_remote_artifacts(
        remote,
        jurisdiction=jurisdiction,
        document_class=document_class,
        version=version,
        release_scopes=release_scope_keys,
    )
    supabase_counts = load_provision_count_snapshot(supabase_counts_path)
    rows = _build_scope_rows(
        root_path,
        local,
        scoped_remote,
        version=version,
        jurisdiction=jurisdiction,
        document_class=document_class,
        release_scopes=release_scope_keys,
        supabase_counts=supabase_counts,
    )
    return ArtifactReport(
        local_root=root_path,
        prefixes=prefix_tuple,
        local_count=len(local),
        local_bytes=sum(artifact.size for artifact in local),
        local_by_prefix=_summarize_by_prefix(local),
        remote_count=len(scoped_remote) if scoped_remote is not None else None,
        remote_bytes=sum(artifact.size for artifact in scoped_remote.values())
        if scoped_remote is not None
        else None,
        remote_by_prefix=(
            _summarize_remote_by_prefix(scoped_remote) if scoped_remote is not None else None
        ),
        rows=rows,
        release_name=release_name,
        release_scope_count=len(release_scope_keys) if release_scope_keys is not None else None,
    )


def build_artifact_report_with_r2(
    root: str | Path,
    *,
    config: R2Config,
    client: Any | None = None,
    prefixes: Iterable[str] = DEFAULT_ARTIFACT_PREFIXES,
    version: str | None = None,
    jurisdiction: str | None = None,
    document_class: str | None = None,
    supabase_counts_path: str | Path | None = None,
    release_name: str | None = None,
    release_scopes: Iterable[ScopeKey] | None = None,
) -> ArtifactReport:
    prefix_tuple = _normalize_prefixes(prefixes)
    r2 = client or make_r2_client(config)
    remote = list_r2_artifacts(r2, bucket=config.bucket, prefixes=prefix_tuple)
    return build_artifact_report(
        root,
        prefixes=prefix_tuple,
        version=version,
        jurisdiction=jurisdiction,
        document_class=document_class,
        supabase_counts_path=supabase_counts_path,
        remote=remote,
        release_name=release_name,
        release_scopes=release_scopes,
    )


def _build_scope_rows(
    root: Path,
    local: tuple[LocalArtifact, ...],
    remote: Mapping[str, RemoteArtifact] | None,
    *,
    version: str | None,
    jurisdiction: str | None,
    document_class: str | None,
    release_scopes: frozenset[ScopeKey] | None,
    supabase_counts: Mapping[tuple[str, str], int],
) -> tuple[ArtifactScopeRow, ...]:
    builders: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(dict)
    if release_scopes is not None:
        for key in release_scopes:
            builders[key]
    for local_artifact in local:
        _merge_local_scope(builders, root, local_artifact)
    if remote is not None:
        for remote_artifact in remote.values():
            _merge_remote_scope(builders, remote_artifact)
    rows: list[ArtifactScopeRow] = []
    for key in sorted(builders):
        row_jurisdiction, row_document_class, row_version = key
        if version is not None and row_version != version:
            continue
        if jurisdiction is not None and row_jurisdiction != jurisdiction:
            continue
        if document_class is not None and row_document_class != document_class:
            continue
        if release_scopes is not None and key not in release_scopes:
            continue
        data = builders[key]
        rows.append(
            ArtifactScopeRow(
                jurisdiction=row_jurisdiction,
                document_class=row_document_class,
                version=row_version,
                local_inventory=bool(data.get("local_inventory")),
                local_provisions=bool(data.get("local_provisions")),
                local_coverage=bool(data.get("local_coverage")),
                local_source_files=int(data.get("local_source_files", 0)),
                local_source_bytes=int(data.get("local_source_bytes", 0)),
                remote_inventory=bool(data.get("remote_inventory")) if remote is not None else None,
                remote_provisions=(
                    bool(data.get("remote_provisions")) if remote is not None else None
                ),
                remote_coverage=bool(data.get("remote_coverage")) if remote is not None else None,
                remote_source_files=(
                    int(data.get("remote_source_files", 0)) if remote is not None else None
                ),
                remote_source_bytes=(
                    int(data.get("remote_source_bytes", 0)) if remote is not None else None
                ),
                coverage_complete=data.get("coverage_complete"),
                source_count=data.get("source_count"),
                provision_count=data.get("provision_count"),
                matched_count=data.get("matched_count"),
                missing_count=data.get("missing_count"),
                extra_count=data.get("extra_count"),
                supabase_count=supabase_counts.get((row_jurisdiction, row_document_class)),
            )
        )
    return tuple(rows)


def _merge_local_scope(
    builders: dict[tuple[str, str, str], dict[str, Any]],
    root: Path,
    artifact: LocalArtifact,
) -> None:
    parts = artifact.key.split("/")
    parsed = _parse_scope(parts)
    if parsed is None:
        return
    artifact_type, jurisdiction, document_class, row_version = parsed
    data = builders[(jurisdiction, document_class, row_version)]
    if artifact_type == "inventory":
        data["local_inventory"] = True
    elif artifact_type == "provisions":
        data["local_provisions"] = True
    elif artifact_type == "coverage":
        data["local_coverage"] = True
        _merge_coverage(data, root / artifact.key)
    elif artifact_type == "sources":
        data["local_source_files"] = int(data.get("local_source_files", 0)) + 1
        data["local_source_bytes"] = int(data.get("local_source_bytes", 0)) + artifact.size


def _merge_remote_scope(
    builders: dict[tuple[str, str, str], dict[str, Any]],
    artifact: RemoteArtifact,
) -> None:
    parts = artifact.key.split("/")
    parsed = _parse_scope(parts)
    if parsed is None:
        return
    artifact_type, jurisdiction, document_class, row_version = parsed
    data = builders[(jurisdiction, document_class, row_version)]
    if artifact_type == "inventory":
        data["remote_inventory"] = True
    elif artifact_type == "provisions":
        data["remote_provisions"] = True
    elif artifact_type == "coverage":
        data["remote_coverage"] = True
    elif artifact_type == "sources":
        data["remote_source_files"] = int(data.get("remote_source_files", 0)) + 1
        data["remote_source_bytes"] = int(data.get("remote_source_bytes", 0)) + artifact.size


def _parse_scope(parts: list[str]) -> tuple[str, str, str, str] | None:
    if len(parts) < 4 or parts[0] not in {"sources", "inventory", "provisions", "coverage"}:
        return None
    artifact_type, jurisdiction, document_class = parts[0], parts[1], parts[2]
    row_version = parts[3] if artifact_type == "sources" else Path(parts[3]).stem
    return artifact_type, jurisdiction, document_class, row_version


def _artifact_matches_scope(
    key: str,
    *,
    jurisdiction: str | None,
    document_class: str | None,
    version: str | None,
    release_scopes: frozenset[ScopeKey] | None = None,
) -> bool:
    if (
        jurisdiction is None
        and document_class is None
        and version is None
        and release_scopes is None
    ):
        return True
    parsed = _parse_scope(key.split("/"))
    if parsed is None:
        return False
    _, artifact_jurisdiction, artifact_document_class, artifact_version = parsed
    scope_key = (artifact_jurisdiction, artifact_document_class, artifact_version)
    return (
        (release_scopes is None or scope_key in release_scopes)
        and (jurisdiction is None or artifact_jurisdiction == jurisdiction)
        and (document_class is None or artifact_document_class == document_class)
        and (version is None or artifact_version == version)
    )


def _filter_remote_artifacts(
    remote: Mapping[str, RemoteArtifact] | None,
    *,
    jurisdiction: str | None,
    document_class: str | None,
    version: str | None,
    release_scopes: frozenset[ScopeKey] | None = None,
) -> dict[str, RemoteArtifact] | None:
    if remote is None:
        return None
    return {
        key: artifact
        for key, artifact in remote.items()
        if _artifact_matches_scope(
            key,
            jurisdiction=jurisdiction,
            document_class=document_class,
            version=version,
            release_scopes=release_scopes,
        )
    }


def _normalize_release_scopes(
    release_scopes: Iterable[ScopeKey] | None,
) -> frozenset[ScopeKey] | None:
    if release_scopes is None:
        return None
    return frozenset(
        (str(jurisdiction), str(document_class), str(version))
        for jurisdiction, document_class, version in release_scopes
    )


def _merge_coverage(data: dict[str, Any], path: Path) -> None:
    try:
        coverage = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return
    data["coverage_complete"] = bool(coverage.get("complete"))
    data["source_count"] = int(coverage.get("source_count", 0))
    data["provision_count"] = int(coverage.get("provision_count", 0))
    data["matched_count"] = int(coverage.get("matched_count", 0))
    data["missing_count"] = len(coverage.get("missing_from_provisions", ()))
    data["extra_count"] = len(coverage.get("extra_provisions", ()))


def _summarize_by_prefix(artifacts: Iterable[LocalArtifact]) -> dict[str, dict[str, int]]:
    buckets: dict[str, dict[str, int]] = {}
    for artifact in artifacts:
        prefix = artifact.key.split("/", 1)[0]
        bucket = buckets.setdefault(prefix, {"count": 0, "bytes": 0})
        bucket["count"] += 1
        bucket["bytes"] += artifact.size
    return dict(sorted(buckets.items()))


def _summarize_remote_by_prefix(
    artifacts: Mapping[str, RemoteArtifact] | None,
) -> dict[str, dict[str, int]]:
    if artifacts is None:
        return {}
    buckets: dict[str, dict[str, int]] = {}
    for artifact in artifacts.values():
        prefix = artifact.key.split("/", 1)[0]
        bucket = buckets.setdefault(prefix, {"count": 0, "bytes": 0})
        bucket["count"] += 1
        bucket["bytes"] += artifact.size
    return dict(sorted(buckets.items()))


def _upload_artifact(client: Any, *, bucket: str, artifact: LocalArtifact) -> None:
    extra_args: dict[str, Any] = {"Metadata": {"sha256": _sha256_file(artifact.path)}}
    content_type = mimetypes.guess_type(artifact.path.name)[0]
    if content_type:
        extra_args["ContentType"] = content_type
    client.upload_file(
        str(artifact.path),
        bucket,
        artifact.key,
        ExtraArgs=extra_args,
    )


def _upload_artifacts(
    client: Any,
    *,
    bucket: str,
    artifacts: tuple[LocalArtifact, ...],
    workers: int,
    progress_stream: TextIO | None,
) -> tuple[LocalArtifact, ...]:
    if not artifacts:
        return ()
    worker_count = max(1, workers)
    if worker_count == 1:
        uploaded: list[LocalArtifact] = []
        for index, artifact in enumerate(artifacts, start=1):
            _progress(
                progress_stream,
                f"uploading {index}/{len(artifacts)} {artifact.key} ({artifact.size} bytes)",
            )
            _upload_artifact(client, bucket=bucket, artifact=artifact)
            uploaded.append(artifact)
        return tuple(uploaded)

    completed: list[tuple[int, LocalArtifact]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {}
        for index, artifact in enumerate(artifacts, start=1):
            _progress(
                progress_stream,
                f"uploading {index}/{len(artifacts)} {artifact.key} ({artifact.size} bytes)",
            )
            future = executor.submit(_upload_artifact, client, bucket=bucket, artifact=artifact)
            futures[future] = (index, artifact)
        for future in as_completed(futures):
            index, artifact = futures[future]
            future.result()
            completed.append((index, artifact))
    return tuple(artifact for _, artifact in sorted(completed, key=lambda item: item[0]))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_credential_file(path: str | Path | None) -> dict[str, Any]:
    resolved = Path(path) if path is not None else DEFAULT_R2_CREDENTIAL_PATH
    if not resolved.exists():
        return {}
    try:
        data = json.loads(resolved.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"failed to read R2 credential file: {resolved}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"R2 credential file must be a JSON object: {resolved}")
    return data


def _credential_value(credentials: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = credentials.get(key)
        if value:
            return str(value)
    return None


def _normalize_prefixes(prefixes: Iterable[str]) -> tuple[str, ...]:
    normalized = []
    for prefix in prefixes:
        cleaned = prefix.strip().strip("/")
        if not cleaned:
            continue
        if "/" in cleaned or cleaned in {".", ".."}:
            raise ValueError(f"artifact prefix must be a top-level directory: {prefix!r}")
        normalized.append(cleaned)
    return tuple(dict.fromkeys(normalized))


def _progress(stream: TextIO | None, message: str) -> None:
    if stream is None:
        return
    print(message, file=stream)
    stream.flush()


if __name__ == "__main__":
    print("Use `axiom-corpus-ingest sync-r2` for R2 artifact sync.", file=sys.stderr)
