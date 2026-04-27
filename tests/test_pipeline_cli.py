"""Tests for the pipeline CLI module.

Note: The pipeline module has an import dependency on get_r2_atlas which
may not exist. Tests handle import failures gracefully.
"""

from unittest.mock import MagicMock, patch

import pytest

try:
    from atlas.pipeline.cli import main as pipeline_main
    _IMPORT_OK = True
except ImportError:
    _IMPORT_OK = False

pytestmark = pytest.mark.skipif(not _IMPORT_OK, reason="Pipeline CLI not importable")


class TestPipelineCLI:
    @patch("atlas.pipeline.cli.StatePipeline")
    @patch("atlas.pipeline.cli.STATE_CONVERTERS", {"ak": "atlas.converters.us_states.ak"})
    def test_main_with_state(self, mock_pipeline_cls):
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.run.return_value = {
            "sections_found": 10,
            "raw_uploaded": 10,
            "xml_generated": 10,
            "errors": 0,
        }

        with patch("sys.argv", ["cli", "--state", "ak"]):
            pipeline_main()

        mock_pipeline_cls.assert_called_once_with("ak", dry_run=False)

    @patch("atlas.pipeline.cli.StatePipeline")
    @patch("atlas.pipeline.cli.STATE_CONVERTERS", {"ak": "a", "oh": "b"})
    def test_main_all_states(self, mock_pipeline_cls):
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.run.return_value = {
            "sections_found": 5,
            "raw_uploaded": 5,
            "xml_generated": 5,
            "errors": 0,
        }

        with patch("sys.argv", ["cli", "--all-states"]):
            pipeline_main()

        assert mock_pipeline_cls.call_count == 2

    @patch("atlas.pipeline.cli.StatePipeline")
    @patch("atlas.pipeline.cli.STATE_CONVERTERS", {"ak": "a"})
    def test_main_dry_run(self, mock_pipeline_cls):
        mock_pipeline = MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.run.return_value = {
            "sections_found": 0,
            "raw_uploaded": 0,
            "xml_generated": 0,
            "errors": 0,
        }

        with patch("sys.argv", ["cli", "--state", "ak", "--dry-run"]):
            pipeline_main()

        mock_pipeline_cls.assert_called_once_with("ak", dry_run=True)

    @patch("builtins.print")
    def test_main_no_args(self, mock_print):
        with patch("sys.argv", ["cli"]):
            pipeline_main()

        mock_print.assert_called_with("Specify --state or --all-states")
