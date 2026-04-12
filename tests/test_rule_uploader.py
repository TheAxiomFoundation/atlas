"""Tests for rule_uploader — batched Supabase upsert with retry."""
import pytest
from unittest.mock import patch, MagicMock
import httpx
from atlas.ingest.rule_uploader import RuleUploader

@pytest.fixture
def uploader():
    return RuleUploader(url="https://test.supabase.co", key="test-service-key")

@pytest.fixture
def sample_rules():
    return [
        {"id": f"uuid-{i}", "jurisdiction": "us", "doc_type": "statute",
         "parent_id": None, "level": 0, "ordinal": i, "heading": f"Section {i}",
         "body": f"Body text for section {i}\nLine 2", "effective_date": None,
         "source_url": None, "source_path": None, "citation_path": f"us/statute/26/{i}",
         "rac_path": None, "has_rac": False}
        for i in range(1, 6)
    ]

class TestUpsertBatchHeaders:
    def test_sends_correct_headers(self, uploader, sample_rules):
        with patch("httpx.Client") as MockClient:
            mock_client = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_client.post.return_value = mock_response
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            MockClient.return_value = mock_client
            uploader.upsert_batch(sample_rules[:1])
            call_kwargs = mock_client.post.call_args
            headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
            assert headers["apikey"] == "test-service-key"
            assert headers["Authorization"] == "Bearer test-service-key"
            assert headers["Content-Profile"] == "arch"
            assert headers["Prefer"] == "resolution=ignore-duplicates,return=minimal"

class TestUpsertBatchLineCount:
    def test_adds_line_count(self, uploader, sample_rules):
        with patch("httpx.Client") as MockClient:
            mock_client = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_client.post.return_value = mock_response
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            MockClient.return_value = mock_client
            rules = [sample_rules[0].copy()]
            rules[0]["body"] = "Line 1\nLine 2\nLine 3"
            uploader.upsert_batch(rules)
            call_kwargs = mock_client.post.call_args
            json_data = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
            assert json_data[0]["line_count"] == 3

    def test_empty_body_line_count(self, uploader):
        with patch("httpx.Client") as MockClient:
            mock_client = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_client.post.return_value = mock_response
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            MockClient.return_value = mock_client
            rules = [{"id": "1", "body": "", "citation_path": "test"}]
            uploader.upsert_batch(rules)
            call_kwargs = mock_client.post.call_args
            json_data = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
            assert json_data[0]["line_count"] == 1

class TestUpsertAllBatches:
    def test_batches_correctly(self, uploader):
        rules = [{"id": str(i), "body": "text", "citation_path": f"path/{i}"} for i in range(120)]
        with patch.object(uploader, "upsert_batch", return_value=50) as mock_batch:
            total = uploader.upsert_all(rules, batch_size=50)
            assert mock_batch.call_count == 3
            batch_sizes = [len(call.args[0]) for call in mock_batch.call_args_list]
            assert batch_sizes == [50, 50, 20]

    def test_empty_input(self, uploader):
        assert uploader.upsert_all([], batch_size=50) == 0

    def test_deduplicates_by_citation_path(self, uploader):
        rules = [
            {"id": "1", "body": "old", "citation_path": "path/1"},
            {"id": "2", "body": "new", "citation_path": "path/1"},
            {"id": "3", "body": "unique", "citation_path": "path/2"},
        ]
        with patch.object(uploader, "upsert_batch", return_value=2) as mock_batch:
            uploader.upsert_all(rules, batch_size=50)
            all_rules = mock_batch.call_args_list[0].args[0]
            paths = [r["citation_path"] for r in all_rules]
            assert len(paths) == 2
            assert set(paths) == {"path/1", "path/2"}

class TestRetryOnTimeout:
    def test_retries_on_timeout(self, uploader, sample_rules):
        with patch("httpx.Client") as MockClient, patch("time.sleep") as mock_sleep:
            mock_client = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status = MagicMock()
            mock_client.post.side_effect = [httpx.ReadTimeout("timeout"), httpx.ReadTimeout("timeout"), mock_response]
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            MockClient.return_value = mock_client
            result = uploader.upsert_batch(sample_rules[:1])
            assert result == 1
            assert mock_sleep.call_count == 2

class TestRetryOnServerError:
    def test_retries_on_5xx(self, uploader, sample_rules):
        with patch("httpx.Client") as MockClient, patch("time.sleep"):
            mock_client = MagicMock()
            error_response = MagicMock()
            error_response.status_code = 502
            error_response.raise_for_status.side_effect = httpx.HTTPStatusError("502", request=MagicMock(), response=error_response)
            ok_response = MagicMock()
            ok_response.status_code = 200
            ok_response.raise_for_status = MagicMock()
            mock_client.post.side_effect = [error_response, ok_response]
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            MockClient.return_value = mock_client
            result = uploader.upsert_batch(sample_rules[:1])
            assert result == 1

class TestNoRetryOnClientError:
    def test_raises_on_4xx(self, uploader, sample_rules):
        with patch("httpx.Client") as MockClient:
            mock_client = MagicMock()
            error_response = MagicMock()
            error_response.status_code = 400
            error_response.raise_for_status.side_effect = httpx.HTTPStatusError("400", request=MagicMock(), response=error_response)
            mock_client.post.return_value = error_response
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            MockClient.return_value = mock_client
            with pytest.raises(httpx.HTTPStatusError):
                uploader.upsert_batch(sample_rules[:1])
            assert mock_client.post.call_count == 1

class TestGetServiceKey:
    def test_get_service_key_from_env(self):
        with patch.dict("os.environ", {"SUPABASE_ACCESS_TOKEN": "test-token"}):
            with patch("httpx.Client") as MockClient:
                mock_client = MagicMock()
                mock_response = MagicMock()
                mock_response.json.return_value = [
                    {"name": "anon", "api_key": "anon-key"},
                    {"name": "service_role", "api_key": "service-key-123"},
                ]
                mock_response.raise_for_status = MagicMock()
                mock_client.get.return_value = mock_response
                mock_client.__enter__ = MagicMock(return_value=mock_client)
                mock_client.__exit__ = MagicMock(return_value=False)
                MockClient.return_value = mock_client
                uploader = RuleUploader(url="https://test.supabase.co")
                assert uploader.key == "service-key-123"

    def test_raises_without_access_token(self):
        import os
        old = os.environ.pop("SUPABASE_ACCESS_TOKEN", None)
        try:
            with pytest.raises(ValueError, match="SUPABASE_ACCESS_TOKEN"):
                RuleUploader(url="https://test.supabase.co")
        finally:
            if old:
                os.environ["SUPABASE_ACCESS_TOKEN"] = old
