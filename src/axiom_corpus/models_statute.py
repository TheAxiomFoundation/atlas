"""Unified statute model for all jurisdictions.

Supports federal (US Code) and state statutes with consistent structure.

Jurisdiction IDs match rules repo naming:
- us          -> rulespec-us (federal)
- us-ca       -> rulespec-us-ca (California)
- us-ny       -> rulespec-us-ny (New York)
- uk          -> rulespec-uk (United Kingdom)

Citation paths use slashes like RuleSpec engine:
- rulespec-us/statute/26/32/a.yaml
- rulespec-us-ca/statute/RTC/17041/a.yaml
"""

from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator


class JurisdictionType(StrEnum):
    """Type of jurisdiction."""

    FEDERAL = "federal"
    STATE = "state"
    TERRITORY = "territory"
    LOCAL = "local"


class CodeType(StrEnum):
    """Type of legal code."""

    STATUTE = "statute"
    REGULATION = "regulation"
    CONSTITUTION = "constitution"
    RULE = "rule"


# Jurisdiction registry - maps ID to metadata
JURISDICTIONS: dict[str, dict] = {
    # Federal
    "us": {
        "name": "United States",
        "type": JurisdictionType.FEDERAL,
        "codes_url": "https://uscode.house.gov",
    },
    # States (alphabetical)
    "us-al": {"name": "Alabama", "type": JurisdictionType.STATE},
    "us-ak": {"name": "Alaska", "type": JurisdictionType.STATE},
    "us-az": {"name": "Arizona", "type": JurisdictionType.STATE},
    "us-ar": {"name": "Arkansas", "type": JurisdictionType.STATE},
    "us-ca": {
        "name": "California",
        "type": JurisdictionType.STATE,
        "codes_url": "https://leginfo.legislature.ca.gov",
    },
    "us-co": {"name": "Colorado", "type": JurisdictionType.STATE},
    "us-ct": {"name": "Connecticut", "type": JurisdictionType.STATE},
    "us-de": {"name": "Delaware", "type": JurisdictionType.STATE},
    "us-fl": {
        "name": "Florida",
        "type": JurisdictionType.STATE,
        "codes_url": "https://leg.state.fl.us",
    },
    "us-ga": {"name": "Georgia", "type": JurisdictionType.STATE},
    "us-hi": {"name": "Hawaii", "type": JurisdictionType.STATE},
    "us-id": {"name": "Idaho", "type": JurisdictionType.STATE},
    "us-il": {"name": "Illinois", "type": JurisdictionType.STATE},
    "us-in": {"name": "Indiana", "type": JurisdictionType.STATE},
    "us-ia": {"name": "Iowa", "type": JurisdictionType.STATE},
    "us-ks": {"name": "Kansas", "type": JurisdictionType.STATE},
    "us-ky": {"name": "Kentucky", "type": JurisdictionType.STATE},
    "us-la": {"name": "Louisiana", "type": JurisdictionType.STATE},
    "us-me": {"name": "Maine", "type": JurisdictionType.STATE},
    "us-md": {"name": "Maryland", "type": JurisdictionType.STATE},
    "us-ma": {"name": "Massachusetts", "type": JurisdictionType.STATE},
    "us-mi": {"name": "Michigan", "type": JurisdictionType.STATE},
    "us-mn": {"name": "Minnesota", "type": JurisdictionType.STATE},
    "us-ms": {"name": "Mississippi", "type": JurisdictionType.STATE},
    "us-mo": {"name": "Missouri", "type": JurisdictionType.STATE},
    "us-mt": {"name": "Montana", "type": JurisdictionType.STATE},
    "us-ne": {"name": "Nebraska", "type": JurisdictionType.STATE},
    "us-nv": {"name": "Nevada", "type": JurisdictionType.STATE},
    "us-nh": {"name": "New Hampshire", "type": JurisdictionType.STATE},
    "us-nj": {"name": "New Jersey", "type": JurisdictionType.STATE},
    "us-nm": {"name": "New Mexico", "type": JurisdictionType.STATE},
    "us-ny": {
        "name": "New York",
        "type": JurisdictionType.STATE,
        "codes_url": "https://legislation.nysenate.gov",
    },
    "us-nc": {
        "name": "North Carolina",
        "type": JurisdictionType.STATE,
        "codes_url": "https://www.ncleg.gov",
    },
    "us-nd": {"name": "North Dakota", "type": JurisdictionType.STATE},
    "us-oh": {
        "name": "Ohio",
        "type": JurisdictionType.STATE,
        "codes_url": "https://codes.ohio.gov",
    },
    "us-ok": {"name": "Oklahoma", "type": JurisdictionType.STATE},
    "us-or": {"name": "Oregon", "type": JurisdictionType.STATE},
    "us-pa": {
        "name": "Pennsylvania",
        "type": JurisdictionType.STATE,
        "codes_url": "https://www.palegis.us",
    },
    "us-ri": {"name": "Rhode Island", "type": JurisdictionType.STATE},
    "us-sc": {"name": "South Carolina", "type": JurisdictionType.STATE},
    "us-sd": {"name": "South Dakota", "type": JurisdictionType.STATE},
    "us-tn": {"name": "Tennessee", "type": JurisdictionType.STATE},
    "us-tx": {
        "name": "Texas",
        "type": JurisdictionType.STATE,
        "codes_url": "https://statutes.capitol.texas.gov",
    },
    "us-ut": {"name": "Utah", "type": JurisdictionType.STATE},
    "us-vt": {"name": "Vermont", "type": JurisdictionType.STATE},
    "us-va": {"name": "Virginia", "type": JurisdictionType.STATE},
    "us-wa": {"name": "Washington", "type": JurisdictionType.STATE},
    "us-wv": {"name": "West Virginia", "type": JurisdictionType.STATE},
    "us-wi": {"name": "Wisconsin", "type": JurisdictionType.STATE},
    "us-wy": {"name": "Wyoming", "type": JurisdictionType.STATE},
    # Territories
    "us-dc": {"name": "District of Columbia", "type": JurisdictionType.TERRITORY},
    "us-pr": {"name": "Puerto Rico", "type": JurisdictionType.TERRITORY},
    "us-gu": {"name": "Guam", "type": JurisdictionType.TERRITORY},
    "us-vi": {"name": "U.S. Virgin Islands", "type": JurisdictionType.TERRITORY},
    "us-as": {"name": "American Samoa", "type": JurisdictionType.TERRITORY},
    "us-mp": {"name": "Northern Mariana Islands", "type": JurisdictionType.TERRITORY},
    # International (future)
    "uk": {"name": "United Kingdom", "type": JurisdictionType.FEDERAL},
    "ca": {"name": "Canada", "type": JurisdictionType.FEDERAL},
}


class StatuteSubsection(BaseModel):
    """A subsection within a statute section."""

    identifier: str = Field(..., description="Subsection identifier (e.g., 'a', '1', 'A')")
    heading: str | None = Field(None, description="Subsection heading if present")
    text: str = Field(..., description="Text content of this subsection")
    children: list[StatuteSubsection] = Field(
        default_factory=list, description="Child subsections"
    )

    model_config = {"extra": "forbid"}


class Statute(BaseModel):
    """A statute section from any jurisdiction.

    Unified model that works for:
    - Federal (US Code): jurisdiction="us", code="26", section="32"
    - State (CA RTC): jurisdiction="us-ca", code="RTC", section="17041"
    - State (NY TAX): jurisdiction="us-ny", code="TAX", section="601"

    Citation path format matches RuleSpec engine:
    - rulespec-us/statute/26/32.yaml
    - rulespec-us-ca/statute/RTC/17041.yaml
    """

    # Core identification
    jurisdiction: str = Field(..., description="Jurisdiction ID (us, us-ca, us-ny, uk, etc.)")
    code: str = Field(
        ..., description="Code/title identifier (e.g., '26' for IRC, 'RTC' for CA Revenue)"
    )
    code_name: str = Field(..., description="Full name of the code (e.g., 'Internal Revenue Code')")
    section: str = Field(..., description="Section number (e.g., '32', '17041', '601-a')")
    subsection_path: str | None = Field(
        None, description="Subsection path using slashes (e.g., 'a/1/A')"
    )

    # Content
    title: str = Field(..., description="Section heading/title")
    text: str = Field(..., description="Full text content of the section")
    subsections: list[StatuteSubsection] = Field(
        default_factory=list, description="Hierarchical subsection structure"
    )

    # Structural hierarchy (varies by jurisdiction)
    division: str | None = Field(None, description="Division (CA) or Subtitle (USC)")
    part: str | None = Field(None, description="Part")
    chapter: str | None = Field(None, description="Chapter")
    subchapter: str | None = Field(None, description="Subchapter")
    article: str | None = Field(None, description="Article")

    # Legislative history
    history: str | None = Field(None, description="Legislative history note")
    enacted_date: date | None = Field(None, description="Date enacted")
    last_amended: date | None = Field(None, description="Date of last amendment")
    effective_date: date | None = Field(None, description="Effective date")
    public_laws: list[str] = Field(
        default_factory=list, description="Public law numbers (federal) or chapter numbers (state)"
    )

    # Cross-references
    references_to: list[str] = Field(
        default_factory=list, description="Citations this section references"
    )
    referenced_by: list[str] = Field(
        default_factory=list, description="Citations that reference this section"
    )

    # Source tracking
    source_url: str = Field(..., description="URL to official source")
    retrieved_at: datetime = Field(
        default_factory=datetime.utcnow, description="When this version was retrieved"
    )
    source_id: str | None = Field(None, description="Source-specific ID (USLM id, etc.)")

    model_config = {"extra": "forbid"}

    @field_validator("jurisdiction")
    @classmethod
    def validate_jurisdiction(cls, v: str) -> str:
        """Normalize jurisdiction ID to lowercase."""
        return v.lower()

    @property
    def jurisdiction_name(self) -> str:
        """Return human-readable jurisdiction name."""
        return JURISDICTIONS.get(self.jurisdiction, {}).get("name", self.jurisdiction)

    @property
    def jurisdiction_type(self) -> JurisdictionType | None:
        """Return jurisdiction type."""
        return JURISDICTIONS.get(self.jurisdiction, {}).get("type")

    @property
    def citation(self) -> str:
        """Return formatted citation string.

        Examples:
        - Federal: "26 USC § 32(a)(1)"
        - California: "Cal. RTC § 17041"
        - New York: "NY Tax § 601"
        """
        if self.jurisdiction == "us":
            # Federal USC format
            base = f"{self.code} USC § {self.section}"
        elif self.jurisdiction.startswith("us-"):
            # State format
            state = self.jurisdiction.split("-")[1].upper()
            base = f"{state} {self.code} § {self.section}"
        else:
            # Generic international
            base = f"{self.jurisdiction.upper()} {self.code} § {self.section}"

        if self.subsection_path:
            # Convert a/1/A to (a)(1)(A)
            parts = self.subsection_path.split("/")
            formatted = "".join(f"({p})" for p in parts)
            return f"{base}{formatted}"
        return base

    @property
    def rulespec_path(self) -> str:
        """Return RuleSpec-style path for file storage.

        Examples:
        - rulespec-us/statute/26/32.yaml
        - rulespec-us-ca/statute/RTC/17041.yaml
        - rulespec-us-ca/statute/RTC/17041/a.yaml (with subsection)
        """
        base = f"rulespec-{self.jurisdiction}/statute/{self.code}/{self.section}"
        if self.subsection_path:
            return f"{base}/{self.subsection_path}.yaml"
        return f"{base}.yaml"

    @property
    def db_path(self) -> str:
        """Return database storage path (without .yaml extension).

        Examples:
        - us/statute/26/32
        - us-ca/statute/RTC/17041
        """
        base = f"{self.jurisdiction}/statute/{self.code}/{self.section}"
        if self.subsection_path:
            return f"{base}/{self.subsection_path}"
        return base

    @classmethod
    def parse_citation(cls, cite: str) -> dict:
        """Parse a citation string into components.

        Supports formats:
        - "26 USC 32(a)(1)" -> jurisdiction="us", code="26", section="32"
        - "Cal. RTC § 17041" -> jurisdiction="us-ca", code="RTC", section="17041"
        - "NY Tax § 601" -> jurisdiction="us-ny", code="TAX", section="601"
        - "OH 5747.02" -> jurisdiction="us-oh", code="57", section="5747.02"

        Returns dict with jurisdiction, code, section, subsection_path keys.
        """
        import re

        cite = cite.strip()

        # Federal USC format: "26 USC 32(a)(1)"
        usc_pattern = r"(\d+)\s*(?:U\.?S\.?C\.?|USC)\s*(?:§\s*)?(\d+[A-Za-z]?)(?:\(([^)]+)\))?"
        match = re.match(usc_pattern, cite, re.IGNORECASE)
        if match:
            subsection = None
            remainder = cite[match.end() :]
            if remainder or match.group(3):
                sub_pattern = r"\(([^)]+)\)"
                subs = re.findall(sub_pattern, cite)
                if subs:
                    subsection = "/".join(subs)
            return {
                "jurisdiction": "us",
                "code": match.group(1),
                "section": match.group(2),
                "subsection_path": subsection,
            }

        # California: "Cal. RTC § 17041" or "CA RTC 17041"
        ca_pattern = r"(?:Cal\.?|CA)\s+([A-Z]+)\s*(?:§\s*)?(\d+(?:\.\d+)?)"
        match = re.match(ca_pattern, cite, re.IGNORECASE)
        if match:
            return {
                "jurisdiction": "us-ca",
                "code": match.group(1).upper(),
                "section": match.group(2),
                "subsection_path": None,
            }

        # State format: "NY Tax § 601" or "OH 5747.02"
        state_pattern = r"([A-Z]{2})\s+(?:([A-Za-z]+)\s+)?(?:§\s*)?(\d+(?:\.\d+)?[A-Za-z]?)"
        match = re.match(state_pattern, cite, re.IGNORECASE)
        if match:
            state = match.group(1).lower()
            code = match.group(2).upper() if match.group(2) else ""
            return {
                "jurisdiction": f"us-{state}",
                "code": code,
                "section": match.group(3),
                "subsection_path": None,
            }

        raise ValueError(f"Cannot parse citation: {cite}")


class StatuteSearchResult(BaseModel):
    """A search result from the statute archive."""

    jurisdiction: str
    code: str
    section: str
    title: str
    snippet: str = Field(..., description="Relevant text snippet with highlights")
    score: float = Field(..., description="Relevance score")
    rulespec_path: str

    model_config = {"extra": "forbid"}


class JurisdictionInfo(BaseModel):
    """Metadata about a jurisdiction's statutes in the archive."""

    jurisdiction: str
    name: str
    type: JurisdictionType
    codes: list[dict[str, str]] = Field(
        default_factory=list, description="List of codes with 'id' and 'name' keys"
    )
    section_count: int = 0
    last_updated: datetime | None = None

    model_config = {"extra": "forbid"}
