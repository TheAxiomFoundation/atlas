"""Tests for the R2 storage backend.

Tests cover R2Storage class including upload, fetch, list, and stats operations.
All boto3 calls are mocked.
"""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from atlas.storage.r2 import R2Storage, get_r2, get_r2_atlas


class TestR2StorageInit:
    @patch("atlas.storage.r2.boto3")
    def test_init(self, mock_boto3):
        r2 = R2Storage(
            endpoint_url="https://example.r2.cloudflarestorage.com",
            access_key_id="key123",
            secret_access_key="secret456",
        )
        assert r2.bucket == "atlas"
        mock_boto3.client.assert_called_once()

    @patch("atlas.storage.r2.boto3")
    def test_init_custom_bucket(self, mock_boto3):
        r2 = R2Storage(
            endpoint_url="https://example.r2.cloudflarestorage.com",
            access_key_id="key",
            secret_access_key="secret",
            bucket="custom-bucket",
        )
        assert r2.bucket == "custom-bucket"


class TestR2StorageFromConfig:
    @patch("atlas.storage.r2.boto3")
    @patch("builtins.open", mock_open(read_data='{"endpoint_url": "https://r2.example.com", "access_key_id": "key", "secret_access_key": "secret", "bucket": "atlas"}'))
    def test_from_config_default_path(self, mock_boto3):
        r2 = R2Storage.from_config()
        assert r2.bucket == "atlas"

    @patch("atlas.storage.r2.boto3")
    @patch("builtins.open", mock_open(read_data='{"endpoint_url": "https://r2.example.com", "access_key_id": "key", "secret_access_key": "secret"}'))
    def test_from_config_custom_path(self, mock_boto3):
        r2 = R2Storage.from_config(config_path="/custom/path.json")
        assert r2.bucket == "atlas"  # default when not in config

    @patch("atlas.storage.r2.boto3")
    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_from_config_missing_file(self, mock_open, mock_boto3):
        with pytest.raises(FileNotFoundError):
            R2Storage.from_config("/nonexistent/path.json")


class TestR2StorageUpload:
    @patch("atlas.storage.r2.boto3")
    def test_upload_raw_string(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        key = r2.upload_raw("test/file.html", "<html>test</html>")

        assert key == "test/file.html"
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "atlas"
        assert call_kwargs["Key"] == "test/file.html"
        assert call_kwargs["ContentType"] == "text/html; charset=utf-8"

    @patch("atlas.storage.r2.boto3")
    def test_upload_raw_bytes(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        r2.upload_raw("test/file.xml", b"<xml>test</xml>")

        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "application/xml"

    @patch("atlas.storage.r2.boto3")
    def test_upload_raw_pdf(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        r2.upload_raw("test/doc.pdf", b"PDF content")

        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "application/pdf"

    @patch("atlas.storage.r2.boto3")
    def test_upload_raw_json(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        r2.upload_raw("test/data.json", '{"key": "value"}')

        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "application/json"

    @patch("atlas.storage.r2.boto3")
    def test_upload_raw_unknown_type(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        r2.upload_raw("test/file.bin", b"\x00\x01")

        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "application/octet-stream"

    @patch("atlas.storage.r2.boto3")
    def test_upload_raw_custom_content_type(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        r2.upload_raw("test/file.html", "data", content_type="text/plain")

        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["ContentType"] == "text/plain"

    @patch("atlas.storage.r2.boto3")
    def test_upload_raw_with_metadata(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        r2.upload_raw("test/file.html", "data", metadata={"source": "test"})

        call_kwargs = mock_client.put_object.call_args[1]
        assert "source" in call_kwargs["Metadata"]
        assert call_kwargs["Metadata"]["source"] == "test"


class TestR2StorageUploadStatute:
    @patch("atlas.storage.r2.boto3")
    def test_upload_state_statute(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        key = r2.upload_state_statute(
            state="ak",
            section_id="43.05.010",
            html="<html>statute</html>",
            source_url="https://example.com/statute",
        )

        assert key == "us/statutes/states/ak/43-05-010.html"

    @patch("atlas.storage.r2.boto3")
    def test_upload_federal_statute(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        key = r2.upload_federal_statute(
            title=26,
            section="32",
            xml="<xml>statute</xml>",
            source_url="https://uscode.house.gov",
        )

        assert key == "us/statutes/federal/26/32.xml"

    @patch("atlas.storage.r2.boto3")
    def test_upload_guidance_pdf(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        key = r2.upload_guidance(
            doc_type="notices",
            doc_id="n-24-01",
            content=b"%PDF-1.4 content",
            source_url="https://irs.gov",
        )

        assert key == "us/guidance/irs/notices/n-24-01.pdf"

    @patch("atlas.storage.r2.boto3")
    def test_upload_guidance_html(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        key = r2.upload_guidance(
            doc_type="rev-proc",
            doc_id="rp-2024-1",
            content=b"<html>content</html>",
            source_url="https://irs.gov",
        )

        assert key == "us/guidance/irs/rev-proc/rp-2024-1.html"


class TestR2StorageQuery:
    @patch("atlas.storage.r2.boto3")
    def test_exists_true(self, mock_boto3):
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        assert r2.exists("test/file.html") is True
        mock_client.head_object.assert_called_once()

    @patch("atlas.storage.r2.boto3")
    def test_exists_false(self, mock_boto3):
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject"
        )
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        assert r2.exists("nonexistent.html") is False

    @patch("atlas.storage.r2.boto3")
    def test_get(self, mock_boto3):
        mock_client = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = b"file content"
        mock_client.get_object.return_value = {"Body": mock_body}
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        content = r2.get("test/file.html")
        assert content == b"file content"

    @patch("atlas.storage.r2.boto3")
    def test_list_prefix(self, mock_boto3):
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "us/statutes/states/ak/001.html", "Size": 1000},
                {"Key": "us/statutes/states/ak/002.html", "Size": 2000},
            ]
        }
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        result = r2.list_prefix("us/statutes/states/ak/")
        assert len(result) == 2

    @patch("atlas.storage.r2.boto3")
    def test_list_prefix_empty(self, mock_boto3):
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        result = r2.list_prefix("nonexistent/")
        assert result == []


class TestR2StorageStats:
    @patch("atlas.storage.r2.boto3")
    def test_get_state_stats(self, mock_boto3):
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "us/statutes/states/ak/001.html", "Size": 1000},
                {"Key": "us/statutes/states/ak/002.html", "Size": 2500},
            ]
        }
        mock_boto3.client.return_value = mock_client

        r2 = R2Storage("https://r2.example.com", "key", "secret")
        stats = r2.get_state_stats("AK")
        assert stats["count"] == 2
        assert stats["total_bytes"] == 3500


class TestConvenienceFunctions:
    @patch("atlas.storage.r2.R2Storage.from_config")
    def test_get_r2(self, mock_from_config):
        mock_r2 = MagicMock()
        mock_from_config.return_value = mock_r2
        result = get_r2()
        assert result is mock_r2

    @patch("atlas.storage.r2.R2Storage.from_config")
    def test_get_r2_atlas(self, mock_from_config):
        mock_r2 = MagicMock()
        mock_from_config.return_value = mock_r2
        result = get_r2_atlas()
        assert result is mock_r2
