"""Orchestrate state statute ingestion into Supabase."""

import importlib
import inspect
import re
from pathlib import Path

from atlas.ingest.rule_converter import section_to_rules
from atlas.ingest.rule_uploader import RuleUploader

STATE_CONVERTER_MODULES = {
    "ak": "atlas.converters.us_states.ak",
    "al": "atlas.converters.us_states.al",
    "ar": "atlas.converters.us_states.ar",
    "az": "atlas.converters.us_states.az",
    "ca": "atlas.converters.us_states.ca",
    "co": "atlas.converters.us_states.co",
    "ct": "atlas.converters.us_states.ct",
    "dc": "atlas.converters.us_states.dc",
    "de": "atlas.converters.us_states.de",
    "fl": "atlas.converters.us_states.fl",
    "ga": "atlas.converters.us_states.ga",
    "hi": "atlas.converters.us_states.hi",
    "ia": "atlas.converters.us_states.ia",
    "id": "atlas.converters.us_states.id_",
    "il": "atlas.converters.us_states.il",
    "in": "atlas.converters.us_states.in_",
    "ks": "atlas.converters.us_states.ks",
    "ky": "atlas.converters.us_states.ky",
    "la": "atlas.converters.us_states.la",
    "ma": "atlas.converters.us_states.ma",
    "md": "atlas.converters.us_states.md",
    "me": "atlas.converters.us_states.me",
    "mi": "atlas.converters.us_states.mi",
    "mn": "atlas.converters.us_states.mn",
    "mo": "atlas.converters.us_states.mo",
    "ms": "atlas.converters.us_states.ms",
    "mt": "atlas.converters.us_states.mt",
    "nc": "atlas.converters.us_states.nc",
    "nd": "atlas.converters.us_states.nd",
    "ne": "atlas.converters.us_states.ne",
    "nh": "atlas.converters.us_states.nh",
    "nj": "atlas.converters.us_states.nj",
    "nm": "atlas.converters.us_states.nm",
    "nv": "atlas.converters.us_states.nv",
    "ny": "atlas.converters.us_states.ny",
    "oh": "atlas.converters.us_states.oh",
    "ok": "atlas.converters.us_states.ok",
    "or": "atlas.converters.us_states.or_",
    "pa": "atlas.converters.us_states.pa",
    "ri": "atlas.converters.us_states.ri",
    "sc": "atlas.converters.us_states.sc",
    "sd": "atlas.converters.us_states.sd",
    "tn": "atlas.converters.us_states.tn",
    "tx": "atlas.converters.us_states.tx",
    "ut": "atlas.converters.us_states.ut",
    "va": "atlas.converters.us_states.va",
    "vt": "atlas.converters.us_states.vt",
    "wa": "atlas.converters.us_states.wa",
    "wi": "atlas.converters.us_states.wi",
    "wv": "atlas.converters.us_states.wv",
    "wy": "atlas.converters.us_states.wy",
}


class StateOrchestrator:
    """Coordinate ingestion of state statutes from local HTML files."""

    def __init__(
        self,
        data_dir: Path | None = None,
        uploader: RuleUploader | None = None,
    ):
        self.data_dir = data_dir or (
            Path(__file__).parent.parent.parent.parent / "data"
        )
        self._uploader = uploader

    @property
    def uploader(self) -> RuleUploader:
        if self._uploader is None:
            self._uploader = RuleUploader()
        return self._uploader

    def get_available_states(self) -> list[str]:
        """List state codes that have local data directories."""
        statutes_dir = self.data_dir / "statutes"
        if not statutes_dir.exists():
            return []
        return sorted(
            d.name.replace("us-", "")
            for d in statutes_dir.iterdir()
            if d.is_dir() and d.name.startswith("us-") and len(d.name) == 5
        )

    def _get_converter(self, state_code: str):
        """Import and instantiate the converter for a state."""
        module_path = STATE_CONVERTER_MODULES.get(state_code)
        if not module_path:
            raise ValueError(f"No converter for state: {state_code}")
        module = importlib.import_module(module_path)
        class_name = f"{state_code.upper()}Converter"
        if hasattr(module, class_name):
            return getattr(module, class_name)()
        for name in dir(module):
            if name.endswith("Converter") and name != "Converter":
                return getattr(module, name)()
        raise ValueError(f"No converter class found in {module_path}")

    def _extract_section_number(self, filename: str, state: str) -> str:
        """Extract section number from an HTML filename.

        Each state has unique URL-derived filenames. This method handles
        all known patterns.
        """
        # OH: ohio-revised-code_section-5747.01.html → 5747.01
        # AL: code-of-alabama_section-1-1-1.1.html → 1-1-1.1
        if "section-" in filename:
            match = re.search(r"section-(.+?)\.html$", filename)
            if match:
                return match.group(1)

        # AK: statutes.asp_01-05-006.html → 01.05.006
        if "statutes.asp_" in filename:
            match = re.search(r"statutes\.asp_(.+?)\.html$", filename)
            if match:
                return match.group(1).replace("-", ".")

        # AZ: viewdocument_docName-www.azleg.gov_ars_20_00259.htm.html → 20-00259
        if "_ars_" in filename:
            match = re.search(r"_ars_(\d+_\d+[-\d]*)\.htm", filename)
            if match:
                return match.group(1).replace("_", "-")

        # MN: statutes_cite_105.63.html → 105.63
        if "statutes_cite_" in filename:
            match = re.search(r"statutes_cite_(.+?)\.html$", filename)
            if match:
                return match.group(1)

        # WA: RCW_default.aspx_cite-10.89.html → 10.89
        if "_cite-" in filename:
            match = re.search(r"_cite-(.+?)\.html$", filename)
            if match:
                return match.group(1)

        # WI: document_statutes_100.52.html → 100.52
        if "document_statutes_" in filename:
            match = re.search(r"document_statutes_(.+?)\.html$", filename)
            if match:
                return match.group(1)

        # NE: laws_statutes.php_statute-13-2501.html → 13-2501
        #     laws_statutes.php_statute-44-911_print-true.html → 44-911
        if "statutes.php_statute-" in filename:
            match = re.search(r"statute-(.+?)(?:_print-true)?\.html$", filename)
            if match:
                return match.group(1)

        # TX: Docs_AG_htm_AG.1.htm_1-001.html → AG/1.001
        #     Pattern: Docs_{code}_htm_{code}.{ch}.htm_{ch}-{sec}.html
        if filename.startswith("Docs_") and "_htm_" in filename:
            match = re.search(
                r"_htm_([A-Z]+)\.(\d+)\.htm_\d+-(.+?)\.html$", filename
            )
            if match:
                code = match.group(1)
                chapter = match.group(2)
                section = match.group(3)
                return f"{code}/{chapter}.{section}"

        # ME: statutes_1_title1ch0sec0.html.html → 1-0
        #     statutes_13-A_title13-Ach0sec0.html.html → 13-A-0
        if "title" in filename and "sec" in filename:
            match = re.search(r"title([\d]+(?:-[A-Z])?)ch\d+sec(\d+)", filename)
            if match:
                return f"{match.group(1)}-{match.group(2)}"

        # RI: Statutes_TITLE1_INDEX.HTM.html → TITLE1
        if "Statutes_TITLE" in filename:
            match = re.search(r"Statutes_(TITLE\d+)", filename)
            if match:
                return match.group(1)

        # SC: code_t02c003.php.html → 2-3
        if filename.startswith("code_t") and "c" in filename:
            match = re.search(r"code_t(\d+)c(\d+)", filename)
            if match:
                return f"{int(match.group(1))}-{int(match.group(2))}"

        # LA: Legis_Laws_Toc.aspx_folder-1.html → 1
        if "_folder-" in filename:
            match = re.search(r"_folder-(\d+)\.html$", filename)
            if match:
                return match.group(1)

        # NH: rsa_html_NHTOC_NHTOC-I.htm.html → I
        if "NHTOC-" in filename:
            match = re.search(r"NHTOC-([^.]+)\.htm", filename)
            if match:
                return match.group(1)

        # NV: NRS_NRS-000.html.html → 000
        if "NRS_NRS-" in filename:
            match = re.search(r"NRS-([^.]+)\.html", filename)
            if match:
                return match.group(1)

        # PA: statutes_consolidated_view-statute_txtType-HTM_ttl-10.html → 10
        if "_ttl-" in filename:
            match = re.search(r"_ttl-(\d+)\.html$", filename)
            if match:
                return match.group(1)

        # Generic fallback
        stem = Path(filename).stem
        for prefix in ["section", "sec", "statute"]:
            stem = re.sub(rf"^{prefix}[-_]?", "", stem, flags=re.IGNORECASE)
        return stem

    def _split_section_for_converter(
        self, section_num: str, state: str
    ) -> tuple:
        """Split an extracted section number for converters with extra args.

        TX: "AG.1-1-001" → ("AG", "1-1-001")  — code, section_number
        ME: "1-0" → (1, "0")  — title (int), section_number
        """
        if state == "tx":
            # TX format: "CODE/chapter.section" e.g. "AG/1.001"
            # Split at slash: code="AG", section_number="1.001"
            if "/" in section_num:
                code, section = section_num.split("/", 1)
                return (code, section)
            return (section_num, "")
        elif state == "me":
            # ME format: "title-section" e.g. "1-0" or "13-A-0"
            parts = section_num.split("-", 1)
            if len(parts) == 2:
                try:
                    title = int(parts[0])
                except ValueError:
                    title = parts[0]
                return (title, parts[1])
            try:
                return (int(section_num), "0")
            except ValueError:
                return (section_num, "0")
        return (section_num,)

    def _parse_local_file(self, html_path: Path, state: str):
        """Parse a single local HTML file into a Section."""
        converter = self._get_converter(state)

        # Skip converters without _parse_section_html
        if not hasattr(converter, "_parse_section_html"):
            print(f"  Skipping {html_path.name}: {type(converter).__name__} has no _parse_section_html")
            return None

        section_num = self._extract_section_number(html_path.name, state)
        html = html_path.read_text(errors="replace")
        url = f"file://{html_path}"
        try:
            # Check how many params the converter expects (excluding self)
            sig = inspect.signature(converter._parse_section_html)
            param_count = len(sig.parameters)

            if param_count == 4:
                # Non-standard: (html, extra_arg, section_number, url)
                # e.g. TX: (html, code, section_number, url)
                # e.g. ME: (html, title, section_number, url)
                parts = self._split_section_for_converter(section_num, state)
                parsed = converter._parse_section_html(
                    html, parts[0], parts[1], url
                )
            else:
                # Standard: (html, section_number, url)
                parsed = converter._parse_section_html(
                    html, section_num, url
                )
            return converter._to_section(parsed)
        except Exception as e:
            print(f"  Warning: Could not parse {html_path.name}: {e}")
            return None

    def ingest_state(self, state_code: str, mode: str = "local") -> int:
        """Ingest all HTML files for a single state.

        Returns the number of rules upserted.
        """
        state_dir = self.data_dir / "statutes" / f"us-{state_code}"
        if not state_dir.exists():
            return 0
        html_files = sorted(state_dir.glob("*.html"))
        if not html_files:
            return 0

        all_rules = []
        for html_path in html_files:
            section = self._parse_local_file(html_path, state_code)
            if section:
                rules = list(
                    section_to_rules(section, jurisdiction=f"us-{state_code}")
                )
                all_rules.extend(rules)

        if not all_rules:
            return 0

        print(
            f"  {state_code.upper()}: {len(all_rules)} rules"
            f" from {len(html_files)} files"
        )
        return self.uploader.upsert_all(all_rules)

    def ingest_all_states(self, mode: str = "local") -> dict[str, int]:
        """Ingest all states that have local data.

        Returns a dict mapping state code to rules upserted.
        Errors in one state do not stop others.
        """
        states = self.get_available_states()
        results = {}
        for state in states:
            try:
                count = self.ingest_state(state, mode=mode)
                results[state] = count
            except Exception as e:
                print(f"Error ingesting {state}: {e}")
                results[state] = 0
        return results
