"""Tests for the pipeline runner module."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from atlas.models import Citation, Section
from atlas.pipeline.runner import STATE_CONVERTERS, StatePipeline


def _make_section(section_id="AK-43.05.010", **kwargs):
    defaults = {
        "citation": Citation(title=0, section=section_id),
        "title_name": "Alaska Statutes",
        "section_title": "Test section",
        "text": "Test text content.",
        "subsections": [],
        "source_url": "https://example.com",
        "retrieved_at": date.today(),
    }
    defaults.update(kwargs)
    return Section(**defaults)


class TestStateConvertersRegistry:
    def test_has_many_states(self):
        assert len(STATE_CONVERTERS) >= 40

    def test_ak_in_registry(self):
        assert "ak" in STATE_CONVERTERS

    def test_ny_in_registry(self):
        assert "ny" in STATE_CONVERTERS

    def test_all_are_atlas_module_paths(self):
        for state, path in STATE_CONVERTERS.items():
            assert "atlas.converters.us_states" in path, f"{state}: {path}"

    def test_all_states_two_letter(self):
        for state in STATE_CONVERTERS:
            assert len(state) == 2


class TestStatePipelineInit:
    @patch("atlas.pipeline.runner.get_r2_atlas")
    def test_init_defaults(self, mock_atlas):
        mock_atlas.return_value = MagicMock()
        pipeline = StatePipeline("ak")
        assert pipeline.state == "ak"
        assert pipeline.dry_run is False

    @patch("atlas.pipeline.runner.get_r2_atlas")
    def test_init_dry_run(self, mock_atlas):
        mock_atlas.return_value = MagicMock()
        pipeline = StatePipeline("ny", dry_run=True)
        assert pipeline.state == "ny"
        assert pipeline.dry_run is True

    def test_init_with_custom_r2(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        assert pipeline.r2_atlas is mock_atlas

    def test_init_stats(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        assert pipeline.stats["sections_found"] == 0
        assert pipeline.stats["raw_uploaded"] == 0
        assert pipeline.stats["xml_generated"] == 0
        assert pipeline.stats["errors"] == 0

    def test_state_normalized_to_lowercase(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("AK", r2_atlas=mock_atlas)
        assert pipeline.state == "ak"


class TestStatePipelineLoadConverter:
    def test_load_converter_invalid_state(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("xx", r2_atlas=mock_atlas)
        with pytest.raises(ValueError, match="No converter for state"):
            pipeline._load_converter()

    @patch("atlas.pipeline.runner.importlib")
    def test_load_converter_success(self, mock_importlib):
        mock_module = MagicMock()
        mock_converter_cls = MagicMock()
        mock_module.AKConverter = mock_converter_cls
        mock_importlib.import_module.return_value = mock_module

        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        pipeline._load_converter()

        mock_converter_cls.assert_called_once()

    @patch("atlas.pipeline.runner.importlib")
    def test_load_converter_alternate_naming(self, mock_importlib):
        mock_module = MagicMock(spec=[])
        # No AKConverter attribute, but has a SomethingConverter
        mock_module.SomeConverter = MagicMock()
        mock_importlib.import_module.return_value = mock_module

        # Need to make hasattr work: spec=[] means no attrs
        # We need to set up dir() to return the converter name
        type(mock_module).__dir__ = lambda self: ["SomeConverter", "__name__"]

        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        result = pipeline._load_converter()
        assert result is not None


class TestStatePipelineGetChapterUrl:
    def test_get_chapter_url_with_build_method_2_params(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        mock_converter = MagicMock()
        mock_converter._build_chapter_url.return_value = "https://example.com/ch1"
        # Mock inspect.signature to return 2 params
        pipeline.converter = mock_converter

        import inspect
        sig = inspect.Signature(parameters=[
            inspect.Parameter("title", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            inspect.Parameter("chapter", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ])
        with patch("atlas.pipeline.runner.inspect.signature", return_value=sig):
            url = pipeline._get_chapter_url("05", 43)
            assert url == "https://example.com/ch1"

    def test_get_chapter_url_fallback(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        mock_converter = MagicMock(spec=[])  # no _build_chapter_url or base_url
        pipeline.converter = mock_converter

        url = pipeline._get_chapter_url("05", None)
        assert "ak.gov" in url

    def test_get_chapter_url_with_base_url(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("oh", r2_atlas=mock_atlas)
        mock_converter = MagicMock(spec=["base_url"])
        mock_converter.base_url = "https://codes.ohio.gov"
        pipeline.converter = mock_converter

        url = pipeline._get_chapter_url("5747")
        assert "codes.ohio.gov" in url


class TestStatePipelineFetchRawHtml:
    def test_fetch_with_get_method(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        mock_converter = MagicMock()
        mock_converter._get.return_value = "<html>content</html>"
        pipeline.converter = mock_converter

        result = pipeline._fetch_raw_html("https://example.com")
        assert result == "<html>content</html>"

    def test_fetch_with_client(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        mock_converter = MagicMock(spec=["client"])
        mock_converter.client.get.return_value.text = "<html>test</html>"
        pipeline.converter = mock_converter

        result = pipeline._fetch_raw_html("https://example.com")
        assert result == "<html>test</html>"

    @patch("httpx.get")
    def test_fetch_with_httpx_fallback(self, mock_httpx_get):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        mock_converter = MagicMock(spec=[])  # No _get or client
        pipeline.converter = mock_converter
        mock_httpx_get.return_value.text = "<html>fallback</html>"

        result = pipeline._fetch_raw_html("https://example.com")
        assert result == "<html>fallback</html>"

    def test_fetch_returns_none_on_error(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        mock_converter = MagicMock()
        mock_converter._get.side_effect = Exception("Connection failed")
        pipeline.converter = mock_converter

        result = pipeline._fetch_raw_html("https://example.com")
        assert result is None


class TestStatePipelineRun:
    def test_run_dry_run(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", dry_run=True, r2_atlas=mock_atlas)

        mock_converter = MagicMock()
        mock_converter.__class__.__name__ = "AKConverter"
        mock_converter.__class__.__module__ = "atlas.converters.us_states.ak"

        sections = [_make_section()]
        mock_converter.iter_chapter.return_value = sections
        mock_converter._get.return_value = "<html>raw</html>"

        with patch.object(pipeline, "_load_converter", return_value=mock_converter):
            with patch.object(pipeline, "_get_chapters", return_value=[("05", 43)]):
                with patch.object(pipeline, "_get_chapter_url", return_value="https://example.com"):
                    stats = pipeline.run()

        assert stats["xml_generated"] == 1
        # Dry run - no actual uploads
        mock_atlas.upload_raw.assert_not_called()

    def test_run_with_upload(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", dry_run=False, r2_atlas=mock_atlas)

        mock_converter = MagicMock()
        mock_converter.__class__.__name__ = "AKConverter"

        sections = [_make_section()]
        mock_converter._get.return_value = "<html>raw</html>"

        with patch.object(pipeline, "_load_converter", return_value=mock_converter):
            with patch.object(pipeline, "_get_chapters", return_value=[("05", 43)]):
                with patch.object(pipeline, "_get_chapter_url", return_value="https://example.com"):
                    with patch.object(pipeline, "_get_sections", return_value=sections):
                        with patch("atlas.pipeline.runner.time.sleep"):
                            stats = pipeline.run()

        assert stats["sections_found"] == 1
        assert stats["raw_uploaded"] == 1
        assert stats["xml_generated"] == 1
        assert stats["errors"] == 0

    def test_run_converter_load_failure(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("xx", r2_atlas=mock_atlas)

        with patch.object(pipeline, "_load_converter", side_effect=ValueError("bad")):
            stats = pipeline.run()

        assert stats["sections_found"] == 0
        assert stats["errors"] == 0  # Load failure doesn't increment errors

    def test_run_no_chapters(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)

        mock_converter = MagicMock()
        mock_converter.__class__.__name__ = "AKConverter"

        with patch.object(pipeline, "_load_converter", return_value=mock_converter):
            with patch.object(pipeline, "_get_chapters", return_value=[]):
                stats = pipeline.run()

        assert stats["sections_found"] == 0


class TestStatePipelineGetSections:
    def test_get_sections_ak(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("ak", r2_atlas=mock_atlas)
        mock_converter = MagicMock()
        sections = [_make_section()]
        mock_converter.iter_chapter.return_value = sections
        pipeline.converter = mock_converter

        result = pipeline._get_sections("05", 43)
        assert len(result) == 1
        mock_converter.iter_chapter.assert_called_once_with(43, "05")

    def test_get_sections_tx(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("tx", r2_atlas=mock_atlas)
        mock_converter = MagicMock()
        sections = [_make_section()]
        mock_converter.iter_chapter.return_value = sections
        pipeline.converter = mock_converter

        result = pipeline._get_sections("151", "TX")
        assert len(result) == 1

    def test_get_sections_generic_iter_chapter(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("oh", r2_atlas=mock_atlas)
        mock_converter = MagicMock()
        sections = [_make_section()]
        mock_converter.iter_chapter.return_value = sections
        pipeline.converter = mock_converter

        result = pipeline._get_sections("5747", None)
        assert len(result) == 1

    def test_get_sections_fetch_chapter(self):
        mock_atlas = MagicMock()
        pipeline = StatePipeline("oh", r2_atlas=mock_atlas)
        mock_converter = MagicMock(spec=["fetch_chapter"])
        sections = [_make_section()]
        mock_converter.fetch_chapter.return_value = sections
        pipeline.converter = mock_converter

        result = pipeline._get_sections("5747", None)
        assert len(result) == 1
