# State Statute Scraper Research

Research on building state statute scrapers for the top 10 US states by population, focusing on tax code sections (state income tax, credits like state EITC).

## Legal Background

**Government edicts are in the public domain.** Per [Georgia v. Public.Resource.Org, Inc.](https://www.supremecourt.gov/opinions/19pdf/18-1150_7m58.pdf) (2020), the Supreme Court confirmed that statutes and official codes authored by legislators are not copyrightable. This applies to:
- Statutory text
- Code annotations created by legislative arms
- All materials created by officials "in the course of their official duties"

**Best practices for scraping:**
1. Respect `robots.txt` as a good-faith signal (not legally binding)
2. Review Terms of Service but note that public data access has been upheld (see hiQ Labs v. LinkedIn)
3. Use bulk download/API options when available
4. Rate-limit requests to avoid server strain
5. Cache aggressively to minimize requests

## Source Summary Table

| State | Population Rank | URL | Format | API/Bulk | Structure | Difficulty | State EITC? |
|-------|-----------------|-----|--------|----------|-----------|------------|-------------|
| California | 1 | [leginfo.legislature.ca.gov](https://leginfo.legislature.ca.gov) | ZIP (XML/HTML) | **Yes - Bulk Downloads** | Code/Division/Part/Chapter/Section | Easy | Yes (RTC 17052) |
| Texas | 2 | [statutes.capitol.texas.gov](https://statutes.capitol.texas.gov) | PDF, RTF | **Yes - FTP** | Code/Title/Subtitle/Chapter/Section | Easy | No (no income tax) |
| Florida | 3 | [leg.state.fl.us/statutes](https://www.leg.state.fl.us/statutes/) | HTML | No | Title/Chapter/Section | Medium | No (no income tax) |
| New York | 4 | [legislation.nysenate.gov](https://legislation.nysenate.gov) | JSON | **Yes - REST API** | Law/Article/Section | Easy | Yes (TAX 606) |
| Pennsylvania | 5 | [palegis.us/statutes](https://www.palegis.us/statutes/consolidated) | HTML | No | Title/Chapter/Section | Medium | No |
| Illinois | 6 | [ilga.gov](https://www.ilga.gov/legislation/ILCS/Chapters) | HTML | No | Chapter/Act/Section | Hard | Yes |
| Ohio | 7 | [codes.ohio.gov](https://codes.ohio.gov/) | HTML, PDF | No | Title/Chapter/Section | Medium | Yes |
| Georgia | 8 | [legis.ga.gov](https://www.legis.ga.gov) | HTML, WSDL | **Yes - SOAP API** | Title/Chapter/Article/Section | Medium | No |
| North Carolina | 9 | [ncleg.gov](https://www.ncleg.gov/Laws/GeneralStatutes) | HTML | No (Archive.org bulk) | Chapter/Article/Section | Medium | Yes |
| Michigan | 10 | [legislature.mi.gov](https://www.legislature.mi.gov) | XML, PDF | **Yes - XML Archive** | Chapter/Act/Section | Easy | Yes |

## Detailed State Analysis

### 1. California (Easiest - Bulk Downloads)

**URL:** https://leginfo.legislature.ca.gov / https://downloads.leginfo.legislature.ca.gov

**Format:** ZIP archives containing statute files

**Bulk Access:**
- Official downloads at `downloads.leginfo.legislature.ca.gov`
- Archive files: `pubinfo_YYYY.zip` (1989-2025)
- Daily/weekly snapshots available
- Documentation: `pubinfo_Readme.pdf`
- Public domain per CA Gov Code 10248.5

**Structure:**
```
California Codes (29 total):
  Revenue and Taxation Code (RTC)
    Division 2 - Other Taxes
      Part 10 - Personal Income Tax
        Chapter 2 - Imposition of Tax
          Section 17052 - California EITC (CalEITC)
```

**Tax-Relevant Sections:**
- CalEITC: RTC 17052
- Young Child Tax Credit: RTC 17052.1
- Personal Income Tax: RTC Part 10

**Difficulty:** Easy - official bulk downloads in predictable format

---

### 2. Texas (Easy - FTP Access)

**URL:** https://statutes.capitol.texas.gov

**Format:** PDF (per-code), RTF available via FTP

**Bulk Access:**
- Download page: `statutes.capitol.texas.gov/Download.aspx`
- FTP: `ftp://ftp.legis.state.tx.us`
- Note: Texas Legislative Council monitors for excessive scraping

**Structure:**
```
Texas Codes (27+ codes):
  Tax Code
    Title 2 - State Taxation
      Subtitle A - General Provisions
      Subtitle B - Franchise Tax (no personal income tax)
```

**Tax Note:** Texas has no state income tax, so no state EITC exists. Primary tax codes are Franchise Tax and property tax provisions.

**Difficulty:** Easy - official FTP with bulk files

---

### 3. New York (Easiest - Full REST API)

**URL:** https://legislation.nysenate.gov

**Format:** JSON via REST API

**API Access:**
- Full documentation: [legislation.nysenate.gov/static/docs/html/laws.html](https://legislation.nysenate.gov/static/docs/html/laws.html)
- Free API key required (sign up at legislation.nysenate.gov)
- Open source: [github.com/nysenate/OpenLegislation](https://github.com/nysenate/OpenLegislation)

**Key API Endpoints:**
```
GET /api/3/laws                           # List all law IDs
GET /api/3/laws/{lawId}                   # Law structure (e.g., TAX)
GET /api/3/laws/{lawId}?full=true         # Full text included
GET /api/3/laws/{lawId}/{locationId}      # Specific section
GET /api/3/laws/search?term=...           # Full-text search
GET /api/3/laws/updates/{from}/{to}       # Change tracking
```

**Law Codes (3-letter):**
- TAX - Tax Law
- EDN - Education Law
- PEN - Penal Law

**Tax-Relevant Sections:**
- NY EITC: TAX 606(d)
- NYC EITC (local): TAX 606(d) references
- Empire State Child Credit: TAX 606(c-1)

**Structure:**
```
Tax Law (TAX)
  Article 22 - Personal Income Tax
    Section 606 - Credits Against Tax
      (d) Earned income credit
      (d-1) Enhanced earned income tax credit
```

**Difficulty:** Easiest - well-documented REST API with JSON responses

---

### 4. Michigan (Easy - XML Archive)

**URL:** https://legislature.mi.gov

**Format:** XML, PDF

**Bulk Access:**
- Archive: `legislature.mi.gov/documents/mcl/archive/`
- XML directory: `legislature.mi.gov/documents/mcl/archive/xml/`
- Date-stamped snapshots (2007-2024)
- Complete MCL through current session

**Structure:**
```
Michigan Compiled Laws (MCL)
  Chapter 206 - Income Tax Act
    206.260 - Credits and exemptions
    206.272 - Earned income tax credit
```

**Tax-Relevant Sections:**
- MI EITC: MCL 206.272

**Difficulty:** Easy - official XML bulk downloads

---

### 5. Florida (Medium - HTML Scraping)

**URL:** https://leg.state.fl.us/statutes/ (Online Sunshine)

**Format:** HTML only

**Bulk Access:** None official. LegiScan offers third-party bulk downloads.

**Structure:**
```
Florida Statutes
  Title XIV - Taxation and Finance
    Chapter 220 - Income Tax Code
```

**Tax Note:** Florida has no state personal income tax.

**Difficulty:** Medium - HTML scraping required, no API

---

### 6. Pennsylvania (Medium - HTML Scraping)

**URL:** https://www.palegis.us/statutes/consolidated

**Format:** HTML

**Bulk Access:** None identified

**Structure:**
```
Pennsylvania Consolidated Statutes (Pa.C.S.)
  Title 72 - Taxation and Fiscal Affairs
    Chapter 73 - Personal Income Tax
```

**Notes:**
- Statutes split between Consolidated and Unconsolidated
- No official bulk download or API
- Legislative Reference Bureau publishes 79 titles

**Difficulty:** Medium - HTML scraping, split statutory structure

---

### 7. Illinois (Hard - HTML Scraping, No Official Source)

**URL:** https://www.ilga.gov/legislation/ILCS/Chapters

**Format:** HTML (unofficial)

**Bulk Access:** None. Database is "NOT official" per ILGA disclaimer.

**Structure:**
```
Illinois Compiled Statutes (ILCS)
  Chapter 35 - Revenue
    Act 5 - Illinois Income Tax Act
      Section 212 - Tax credits
```

**Tax-Relevant Sections:**
- IL EITC: 35 ILCS 5/212

**Challenges:**
- No official ILCS publication exists
- Website explicitly states it's not authoritative
- HTML structure requires parsing

**Difficulty:** Hard - unofficial source, HTML scraping, legal authority unclear

---

### 8. Georgia (Medium - SOAP API)

**URL:** https://www.legis.ga.gov

**Format:** HTML, SOAP Web Service

**API Access:**
- WSDL: `http://webservices.legis.ga.gov/Legislation/Service.svc?wsdl`
- SOAP-based (more complex than REST)

**Bulk Alternative:**
- Internet Archive: `archive.org/details/gov.ga.ocga.2024`
- Obtained via Open Records Act request
- Public domain per SCOTUS ruling

**Structure:**
```
Official Code of Georgia Annotated (OCGA)
  Title 48 - Revenue and Taxation
    Chapter 7 - Income Taxes
      Article 2 - Imposition, Rate, Computation
```

**Tax Note:** Georgia has no state EITC.

**Difficulty:** Medium - SOAP API adds complexity, Archive.org bulk available

---

### 9. North Carolina (Medium - HTML + Archive.org)

**URL:** https://www.ncleg.gov/Laws/GeneralStatutes

**Format:** HTML

**Bulk Access:**
- None from official site
- Archive.org: `archive.org/details/gov.nc.code` (quarterly releases)
- Statutes are public domain per state law

**Structure:**
```
North Carolina General Statutes
  Chapter 105 - Taxation
    Article 4 - Individual Income Tax
      105-151.31 - Earned income tax credit
```

**Tax-Relevant Sections:**
- NC EITC: GS 105-151.31

**Difficulty:** Medium - Archive.org bulk available, HTML scraping for current

---

### 10. Ohio (Medium - HTML + Certified PDFs)

**URL:** https://codes.ohio.gov

**Format:** HTML, authenticated PDFs

**Bulk Access:** None identified. Individual certified PDF downloads available.

**Structure:**
```
Ohio Revised Code (ORC)
  Title [57] - Taxation
    Chapter 5747 - Income Tax
      5747.71 - Earned income credit
```

**Tax-Relevant Sections:**
- OH EITC: ORC 5747.71

**Special Feature:** LSC provides authenticated PDF stamps for legal certification.

**Difficulty:** Medium - HTML scraping, no bulk download

---

## Priority Ranking for Implementation

### Tier 1: Easy (Start Here)
1. **New York** - Full REST API, JSON, excellent documentation
2. **California** - Official bulk downloads, public domain
3. **Michigan** - XML archive with historical snapshots

### Tier 2: Medium Effort
4. **Texas** - FTP access (though no income tax)
5. **Georgia** - Archive.org bulk + SOAP API
6. **North Carolina** - Archive.org bulk available

### Tier 3: HTML Scraping Required
7. **Ohio** - Clean HTML structure
8. **Florida** - Online Sunshine HTML
9. **Pennsylvania** - Split statutory structure

### Tier 4: Avoid Initially
10. **Illinois** - Unofficial source, unclear authority

---

## Code Sketches

### New York API Client

```python
"""
New York Open Legislation API client.
Requires free API key from legislation.nysenate.gov
"""
import httpx
from dataclasses import dataclass
from typing import Optional

BASE_URL = "https://legislation.nysenate.gov/api/3"

@dataclass
class NYLawSection:
    law_id: str
    location_id: str
    title: str
    text: str
    doc_level_id: str

class NYLegislationClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.Client(
            base_url=BASE_URL,
            params={"key": api_key}
        )

    def get_law_ids(self) -> list[str]:
        """List all available law codes (TAX, EDN, PEN, etc.)"""
        resp = self.client.get("/laws")
        resp.raise_for_status()
        return [item["lawId"] for item in resp.json()["result"]["items"]]

    def get_law_tree(self, law_id: str) -> dict:
        """Get structure of a law (articles, sections, etc.)"""
        resp = self.client.get(f"/laws/{law_id}")
        resp.raise_for_status()
        return resp.json()["result"]

    def get_section(
        self,
        law_id: str,
        location_id: str,
        date: Optional[str] = None
    ) -> NYLawSection:
        """
        Get specific section text.

        Args:
            law_id: Law code (e.g., "TAX")
            location_id: Section location (e.g., "606")
            date: ISO date for historical version (optional)
        """
        params = {"full": "true"}
        if date:
            params["date"] = date

        resp = self.client.get(f"/laws/{law_id}/{location_id}", params=params)
        resp.raise_for_status()
        data = resp.json()["result"]

        return NYLawSection(
            law_id=data["lawId"],
            location_id=data["locationId"],
            title=data.get("title", ""),
            text=data.get("text", ""),
            doc_level_id=data.get("docLevelId", "")
        )

    def search(self, term: str, law_id: Optional[str] = None) -> list[dict]:
        """Full-text search across laws."""
        endpoint = f"/laws/{law_id}/search" if law_id else "/laws/search"
        resp = self.client.get(endpoint, params={"term": term, "limit": 100})
        resp.raise_for_status()
        return resp.json()["result"]["items"]


# Example usage
if __name__ == "__main__":
    import os

    client = NYLegislationClient(api_key=os.environ["NY_LEGISLATION_API_KEY"])

    # Get NY EITC section
    eitc = client.get_section("TAX", "606")
    print(f"Section: {eitc.title}")
    print(f"Text length: {len(eitc.text)} chars")

    # Search for earned income credit
    results = client.search("earned income credit", law_id="TAX")
    print(f"Found {len(results)} results")
```

### California Bulk Downloader

```python
"""
California statute bulk download and extraction.
Downloads from official leginfo.legislature.ca.gov
"""
import httpx
import zipfile
from pathlib import Path
from datetime import datetime

DOWNLOAD_BASE = "https://downloads.leginfo.legislature.ca.gov"

def download_california_statutes(
    year: int = None,
    output_dir: Path = Path("data/california")
) -> Path:
    """
    Download California statute archive.

    Args:
        year: Specific year (default: current year)
        output_dir: Where to save files

    Returns:
        Path to extracted directory
    """
    if year is None:
        year = datetime.now().year

    output_dir.mkdir(parents=True, exist_ok=True)

    # Download the yearly archive
    archive_name = f"pubinfo_{year}.zip"
    archive_url = f"{DOWNLOAD_BASE}/{archive_name}"
    archive_path = output_dir / archive_name

    print(f"Downloading {archive_url}...")
    with httpx.stream("GET", archive_url) as resp:
        resp.raise_for_status()
        with open(archive_path, "wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)

    # Extract
    extract_dir = output_dir / f"pubinfo_{year}"
    print(f"Extracting to {extract_dir}...")
    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(extract_dir)

    return extract_dir


def find_revenue_taxation_code(extracted_dir: Path) -> list[Path]:
    """Find Revenue and Taxation Code files in extracted archive."""
    rtc_files = list(extracted_dir.rglob("*rtc*"))
    rtc_files.extend(extracted_dir.rglob("*revenue*"))
    return rtc_files


def extract_section(file_path: Path, section_num: str) -> str:
    """Extract specific section from a code file."""
    # Implementation depends on actual file format
    # Would parse XML/HTML structure
    pass


# Example usage
if __name__ == "__main__":
    extracted = download_california_statutes(2024)
    rtc_files = find_revenue_taxation_code(extracted)
    print(f"Found {len(rtc_files)} RTC files")

    # Look for CalEITC (Section 17052)
    for f in rtc_files:
        if "17052" in f.name:
            print(f"Found CalEITC file: {f}")
```

### Michigan XML Scraper

```python
"""
Michigan Compiled Laws XML archive scraper.
Downloads from official legislature.mi.gov XML archive.
"""
import httpx
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET

ARCHIVE_BASE = "https://www.legislature.mi.gov/documents/mcl/archive/xml"

def list_available_snapshots() -> list[str]:
    """List available XML snapshot dates."""
    resp = httpx.get(f"{ARCHIVE_BASE}/")
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    dates = []

    for link in soup.find_all("a"):
        href = link.get("href", "")
        # Format: YYYY-MM-DD/
        if href.startswith("20") and href.endswith("/"):
            dates.append(href.rstrip("/"))

    return sorted(dates, reverse=True)


def download_mcl_xml(
    snapshot_date: str = None,
    output_dir: Path = Path("data/michigan")
) -> Path:
    """
    Download Michigan MCL XML for a specific snapshot.

    Args:
        snapshot_date: YYYY-MM-DD format (default: latest)
        output_dir: Where to save files

    Returns:
        Path to downloaded XML directory
    """
    if snapshot_date is None:
        available = list_available_snapshots()
        snapshot_date = available[0]  # Latest

    snapshot_url = f"{ARCHIVE_BASE}/{snapshot_date}/"
    output_path = output_dir / snapshot_date
    output_path.mkdir(parents=True, exist_ok=True)

    # List files in snapshot directory
    resp = httpx.get(snapshot_url)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    for link in soup.find_all("a"):
        href = link.get("href", "")
        if href.endswith(".xml"):
            file_url = f"{snapshot_url}{href}"
            file_path = output_path / href

            print(f"Downloading {href}...")
            file_resp = httpx.get(file_url)
            file_resp.raise_for_status()
            file_path.write_bytes(file_resp.content)

    return output_path


def parse_mcl_section(xml_path: Path, chapter: str, section: str) -> dict:
    """
    Parse a specific MCL section from XML.

    Args:
        xml_path: Path to MCL XML file
        chapter: Chapter number (e.g., "206")
        section: Section number (e.g., "272")

    Returns:
        Dict with section metadata and text
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Navigate to chapter/section (structure depends on actual XML schema)
    # This is a placeholder - actual implementation would match MI's schema
    target_id = f"{chapter}.{section}"

    for section_elem in root.iter("section"):
        if section_elem.get("number") == target_id:
            return {
                "chapter": chapter,
                "section": section,
                "title": section_elem.find("title").text,
                "text": section_elem.find("text").text,
            }

    return None


# Example usage
if __name__ == "__main__":
    # List available snapshots
    snapshots = list_available_snapshots()
    print(f"Available snapshots: {snapshots[:5]}...")

    # Download latest
    xml_dir = download_mcl_xml()
    print(f"Downloaded to {xml_dir}")

    # Parse EITC section (MCL 206.272)
    for xml_file in xml_dir.glob("*.xml"):
        if "206" in xml_file.name:
            eitc = parse_mcl_section(xml_file, "206", "272")
            if eitc:
                print(f"Found MI EITC: {eitc['title']}")
```

---

## State EITC Statute References

For states with EITC programs, key statute citations:

| State | Citation | Description |
|-------|----------|-------------|
| California | RTC 17052 | CalEITC (47% of federal, different phaseout) |
| New York | TAX 606(d) | State EITC (30% of federal) |
| New York City | TAX 606(d) | City EITC (10-30% additional) |
| Michigan | MCL 206.272 | MI EITC (6% of federal) |
| Illinois | 35 ILCS 5/212 | IL EITC (20% of federal) |
| Ohio | ORC 5747.71 | OH EITC (30% of federal, nonrefundable) |
| North Carolina | GS 105-151.31 | NC EITC (historic, currently unfunded) |

**States without EITC:**
- Texas (no income tax)
- Florida (no income tax)
- Pennsylvania (flat tax, no credits)
- Georgia (no EITC enacted)

---

## Next Steps

1. **Register for NY API key** at legislation.nysenate.gov
2. **Download California pubinfo archive** and explore structure
3. **Probe Michigan XML archive** to understand schema
4. **Build unified scraper interface** supporting multiple backends
5. **Store in Axiom Corpus catalog format** per existing conventions
