"""Converters for transforming legislation between formats.

This module provides converters for:
- State HTML to USLM XML (US state statutes)
- UK CLML to Axiom models (UK legislation from legislation.gov.uk)
- eCFR to Axiom models (US federal regulations from ecfr.gov)
- Canadian laws-lois-xml to Axiom models (Canadian federal legislation from GitHub)
"""

from axiom_corpus.converters.ca_laws import (
    BilingualContent,
    CanadaLawsConverter,
    CanadaLawsSource,
    fetch_act,
)
from axiom_corpus.converters.ecfr import (
    PRIORITY_TITLES,
    ECFRConverter,
    ECFRMetadata,
    FetchResult,
    fetch_eitc_regulations,
    fetch_regulation,
)
from axiom_corpus.converters.state_to_uslm import (
    OhioToUSLM,
    ParsedSection,
    ParsedSubsection,
    StateToUSLMConverter,
)
from axiom_corpus.converters.state_to_uslm import (
    get_converter as get_state_converter,  # Renamed to avoid conflict
)
from axiom_corpus.converters.uk_clml import UKCLMLConverter, fetch_uk_legislation
from axiom_corpus.converters.us_states.ny import (
    NY_LAW_CODES,
    NYFetchResult,
    NYLawInfo,
    NYSection,
    NYStateConverter,
)

__all__ = [
    # State converters (legacy)
    "StateToUSLMConverter",
    "OhioToUSLM",
    "ParsedSection",
    "ParsedSubsection",
    "get_state_converter",
    # UK CLML converter
    "UKCLMLConverter",
    "fetch_uk_legislation",
    # eCFR converter
    "ECFRConverter",
    "ECFRMetadata",
    "FetchResult",
    "PRIORITY_TITLES",
    "fetch_regulation",
    "fetch_eitc_regulations",
    # Canadian laws-lois-xml converter
    "CanadaLawsConverter",
    "CanadaLawsSource",
    "BilingualContent",
    "fetch_act",
    # US State converters
    "NYStateConverter",
    "NYSection",
    "NYLawInfo",
    "NYFetchResult",
    "NY_LAW_CODES",
]
