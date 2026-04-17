"""Tests for the archive module (main Arch class)."""

from datetime import date
from unittest.mock import MagicMock, patch

from atlas.archive import Arch
from atlas.models import Citation, SearchResult, Section, TitleInfo


def _make_section(**kwargs):
    defaults = {
        "citation": Citation(title=26, section="32"),
        "title_name": "Internal Revenue Code",
        "section_title": "Earned income tax credit",
        "text": "Tax credit is allowed...",
        "subsections": [],
        "source_url": "https://uscode.house.gov",
        "retrieved_at": date(2024, 1, 1),
    }
    defaults.update(kwargs)
    return Section(**defaults)


class TestArchInit:
    @patch("atlas.archive.SQLiteStorage")
    def test_init_default(self, mock_sqlite):
        Arch()
        mock_sqlite.assert_called_once_with("atlas.db")

    @patch("atlas.archive.SQLiteStorage")
    def test_init_custom_path(self, mock_sqlite):
        Arch(db_path="custom.db")
        mock_sqlite.assert_called_once_with("custom.db")

    def test_init_custom_storage(self):
        mock_storage = MagicMock()
        arch = Arch(storage=mock_storage)
        assert arch.storage is mock_storage


class TestArchGet:
    def test_get_by_string(self):
        mock_storage = MagicMock()
        section = _make_section()
        mock_storage.get_section.return_value = section

        arch = Arch(storage=mock_storage)
        result = arch.get("26 USC 32")

        assert result is section
        mock_storage.get_section.assert_called_once_with(
            title=26, section="32", subsection=None, as_of=None
        )

    def test_get_by_citation(self):
        mock_storage = MagicMock()
        section = _make_section()
        mock_storage.get_section.return_value = section

        arch = Arch(storage=mock_storage)
        cite = Citation(title=26, section="32")
        result = arch.get(cite)

        assert result is section

    def test_get_with_subsection(self):
        mock_storage = MagicMock()
        mock_storage.get_section.return_value = _make_section()

        arch = Arch(storage=mock_storage)
        arch.get("26 USC 32(a)(1)")

        call_args = mock_storage.get_section.call_args
        assert call_args[1]["subsection"] == "a/1"

    def test_get_with_as_of(self):
        mock_storage = MagicMock()
        mock_storage.get_section.return_value = None

        arch = Arch(storage=mock_storage)
        result = arch.get("26 USC 32", as_of=date(2020, 1, 1))

        assert result is None
        mock_storage.get_section.assert_called_once_with(
            title=26, section="32", subsection=None, as_of=date(2020, 1, 1)
        )

    def test_get_not_found(self):
        mock_storage = MagicMock()
        mock_storage.get_section.return_value = None

        arch = Arch(storage=mock_storage)
        result = arch.get("99 USC 999")
        assert result is None


class TestArchSearch:
    def test_search(self):
        mock_storage = MagicMock()
        results = [
            SearchResult(
                citation=Citation(title=26, section="32"),
                section_title="EITC",
                snippet="earned income...",
                score=0.9,
            )
        ]
        mock_storage.search.return_value = results

        arch = Arch(storage=mock_storage)
        result = arch.search("earned income")

        assert len(result) == 1
        mock_storage.search.assert_called_once_with(
            "earned income", title=None, limit=20
        )

    def test_search_with_title(self):
        mock_storage = MagicMock()
        mock_storage.search.return_value = []

        arch = Arch(storage=mock_storage)
        arch.search("credit", title=26, limit=5)

        mock_storage.search.assert_called_once_with("credit", title=26, limit=5)


class TestArchListTitles:
    def test_list_titles(self):
        mock_storage = MagicMock()
        mock_storage.list_titles.return_value = [
            TitleInfo(
                number=26, name="IRC", section_count=100,
                last_updated=date(2024, 1, 1), is_positive_law=True,
            )
        ]

        arch = Arch(storage=mock_storage)
        result = arch.list_titles()

        assert len(result) == 1
        assert result[0].number == 26


class TestArchGetReferences:
    def test_get_references_string(self):
        mock_storage = MagicMock()
        mock_storage.get_references_to.return_value = ["26 USC 24"]
        mock_storage.get_referenced_by.return_value = ["26 USC 1"]

        arch = Arch(storage=mock_storage)
        refs = arch.get_references("26 USC 32")

        assert refs["references_to"] == ["26 USC 24"]
        assert refs["referenced_by"] == ["26 USC 1"]

    def test_get_references_citation(self):
        mock_storage = MagicMock()
        mock_storage.get_references_to.return_value = []
        mock_storage.get_referenced_by.return_value = []

        arch = Arch(storage=mock_storage)
        cite = Citation(title=26, section="32")
        refs = arch.get_references(cite)

        assert refs["references_to"] == []
        assert refs["referenced_by"] == []


class TestArchIngestTitle:
    @patch("atlas.parsers.us.statutes.USLMParser")
    def test_ingest_title(self, mock_parser_cls):
        mock_storage = MagicMock()
        mock_parser = MagicMock()
        mock_parser_cls.return_value = mock_parser
        mock_parser.get_title_number.return_value = 26
        mock_parser.get_title_name.return_value = "Internal Revenue Code"
        mock_parser.iter_sections.return_value = [
            _make_section(),
            _make_section(citation=Citation(title=26, section="32A")),
        ]

        arch = Arch(storage=mock_storage)
        count = arch.ingest_title("data/uscode/usc26.xml")

        assert count == 2
        assert mock_storage.store_section.call_count == 2
