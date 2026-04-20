#!/usr/bin/env python3
"""
fetch_drug_data.py — Codex multilingual drug data pipeline
==========================================================
Fetches multilingual drug names and per-country brand names from multiple
sources, merges and deduplicates results, and outputs one Codex-format
CSV per country ready for upload.

Data sources
------------
  Generic names (global)
    PubChem   (NIH)     — InChI Key lookup by drug name
    Wikidata            — Multilingual generic names (100+ languages)

  Brand names (per country / region)
    openFDA             — US (FDA drug product database)
    DailyMed  (NIH)     — US (FDA drug label database — broader coverage)
    RxNorm    (NIH)     — US (normalized drug names, catches generics marketed as brands)
    Health Canada       — Canada
    EMA                 — All EU/EEA member states (centrally authorised products)
    MHRA                — UK (post-Brexit national authorisations)
    TGA                 — Australia
    ANVISA              — Brazil
    PMDA                — Japan

  Multiple sources per country are merged and deduplicated automatically.

Usage
-----
  python fetch_drug_data.py                              # built-in list, default countries
  python fetch_drug_data.py --countries US,UA,DE,JP,BR
  python fetch_drug_data.py --drugs "Ibuprofen,Metformin"
  python fetch_drug_data.py --file my_drugs.txt --countries US,MX,DE
  python fetch_drug_data.py --openfda-key YOUR_KEY       # higher FDA rate limit

Requirements
------------
  pip install requests
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import time
from pathlib import Path
from typing import Optional

try:
    import requests
except ImportError:
    sys.exit("Missing dependency — run:  pip install requests")


# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("codex.fetch")


# ── Country configuration ─────────────────────────────────────────────────────
#
# brand_srcs: list of source keys to use for brand name lookup.
#   - Order matters — sources listed first are preferred for display formatting
#     when duplicates are detected.
#   - Empty list means generic name only (no brand data available yet).
#   - Multiple countries can share the same source key (e.g. all EU countries
#     share "ema") — the pipeline calls each unique source only ONCE per drug.
#
# To add a country: add an entry. To add brand support: add a source key to
# brand_srcs and implement the fetcher below in BRAND_FETCHERS.

COUNTRY_CONFIG: dict[str, dict] = {
    # ── Americas ──────────────────────────────────────────────────────────────
    "US": {"language": "English",    "lang_code": "en", "brand_srcs": ["openfda", "dailymed", "rxnorm"]},
    "CA": {"language": "English",    "lang_code": "en", "brand_srcs": ["health_canada", "rxnorm"]},
    "MX": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "BR": {"language": "Portuguese", "lang_code": "pt", "brand_srcs": ["anvisa"]},
    "AR": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "CO": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "CL": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "PE": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "VE": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "EC": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "BO": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "PY": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "UY": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "GT": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    "CU": {"language": "Spanish",    "lang_code": "es", "brand_srcs": []},
    # ── Europe — EMA members (centrally authorised brand names) ───────────────
    "GB": {"language": "English",    "lang_code": "en", "brand_srcs": ["ema", "mhra"]},
    "DE": {"language": "German",     "lang_code": "de", "brand_srcs": ["ema"]},
    "FR": {"language": "French",     "lang_code": "fr", "brand_srcs": ["ema"]},
    "ES": {"language": "Spanish",    "lang_code": "es", "brand_srcs": ["ema"]},
    "IT": {"language": "Italian",    "lang_code": "it", "brand_srcs": ["ema"]},
    "PT": {"language": "Portuguese", "lang_code": "pt", "brand_srcs": ["ema"]},
    "NL": {"language": "Dutch",      "lang_code": "nl", "brand_srcs": ["ema"]},
    "BE": {"language": "French",     "lang_code": "fr", "brand_srcs": ["ema"]},
    "SE": {"language": "Swedish",    "lang_code": "sv", "brand_srcs": ["ema"]},
    "NO": {"language": "Norwegian",  "lang_code": "no", "brand_srcs": ["ema"]},
    "DK": {"language": "Danish",     "lang_code": "da", "brand_srcs": ["ema"]},
    "FI": {"language": "Finnish",    "lang_code": "fi", "brand_srcs": ["ema"]},
    "PL": {"language": "Polish",     "lang_code": "pl", "brand_srcs": ["ema"]},
    "RO": {"language": "Romanian",   "lang_code": "ro", "brand_srcs": ["ema"]},
    "GR": {"language": "Greek",      "lang_code": "el", "brand_srcs": ["ema"]},
    "CZ": {"language": "Czech",      "lang_code": "cs", "brand_srcs": ["ema"]},
    "HU": {"language": "Hungarian",  "lang_code": "hu", "brand_srcs": ["ema"]},
    "SK": {"language": "Slovak",     "lang_code": "sk", "brand_srcs": ["ema"]},
    "AT": {"language": "German",     "lang_code": "de", "brand_srcs": ["ema"]},
    "HR": {"language": "Croatian",   "lang_code": "hr", "brand_srcs": ["ema"]},
    "BG": {"language": "Bulgarian",  "lang_code": "bg", "brand_srcs": ["ema"]},
    "LT": {"language": "Lithuanian", "lang_code": "lt", "brand_srcs": ["ema"]},
    "LV": {"language": "Latvian",    "lang_code": "lv", "brand_srcs": ["ema"]},
    "EE": {"language": "Estonian",   "lang_code": "et", "brand_srcs": ["ema"]},
    "SI": {"language": "Slovenian",  "lang_code": "sl", "brand_srcs": ["ema"]},
    "IE": {"language": "English",    "lang_code": "en", "brand_srcs": ["ema"]},
    "LU": {"language": "French",     "lang_code": "fr", "brand_srcs": ["ema"]},
    "MT": {"language": "Maltese",    "lang_code": "mt", "brand_srcs": ["ema"]},
    "CY": {"language": "Greek",      "lang_code": "el", "brand_srcs": ["ema"]},
    # ── Europe — non-EMA ─────────────────────────────────────────────────────
    "UA": {"language": "Ukrainian",  "lang_code": "uk", "brand_srcs": []},
    "TR": {"language": "Turkish",    "lang_code": "tr", "brand_srcs": []},
    "RU": {"language": "Russian",    "lang_code": "ru", "brand_srcs": []},
    "RS": {"language": "Serbian",    "lang_code": "sr", "brand_srcs": []},
    "CH": {"language": "German",     "lang_code": "de", "brand_srcs": []},
    "AL": {"language": "Albanian",   "lang_code": "sq", "brand_srcs": []},
    "MK": {"language": "Macedonian", "lang_code": "mk", "brand_srcs": []},
    "BA": {"language": "Bosnian",    "lang_code": "bs", "brand_srcs": []},
    "MD": {"language": "Romanian",   "lang_code": "ro", "brand_srcs": []},
    "BY": {"language": "Belarusian", "lang_code": "be", "brand_srcs": []},
    "GE": {"language": "Georgian",   "lang_code": "ka", "brand_srcs": []},
    "AM": {"language": "Armenian",   "lang_code": "hy", "brand_srcs": []},
    "AZ": {"language": "Azerbaijani","lang_code": "az", "brand_srcs": []},
    "KZ": {"language": "Kazakh",     "lang_code": "kk", "brand_srcs": []},
    "IS": {"language": "Icelandic",  "lang_code": "is", "brand_srcs": []},
    # ── Asia / Pacific ────────────────────────────────────────────────────────
    "AU": {"language": "English",    "lang_code": "en", "brand_srcs": ["tga"]},
    "NZ": {"language": "English",    "lang_code": "en", "brand_srcs": []},
    "JP": {"language": "Japanese",   "lang_code": "ja", "brand_srcs": ["pmda"]},
    "KR": {"language": "Korean",     "lang_code": "ko", "brand_srcs": []},
    "CN": {"language": "Chinese",    "lang_code": "zh", "brand_srcs": []},
    "TW": {"language": "Chinese",    "lang_code": "zh", "brand_srcs": []},
    "HK": {"language": "Chinese",    "lang_code": "zh", "brand_srcs": []},
    "IN": {"language": "English",    "lang_code": "en", "brand_srcs": []},
    "SG": {"language": "English",    "lang_code": "en", "brand_srcs": []},
    "MY": {"language": "Malay",      "lang_code": "ms", "brand_srcs": []},
    "ID": {"language": "Indonesian", "lang_code": "id", "brand_srcs": []},
    "TH": {"language": "Thai",       "lang_code": "th", "brand_srcs": []},
    "VN": {"language": "Vietnamese", "lang_code": "vi", "brand_srcs": []},
    "PH": {"language": "Filipino",   "lang_code": "tl", "brand_srcs": []},
    "BD": {"language": "Bengali",    "lang_code": "bn", "brand_srcs": []},
    "PK": {"language": "Urdu",       "lang_code": "ur", "brand_srcs": []},
    "LK": {"language": "Sinhala",    "lang_code": "si", "brand_srcs": []},
    "MM": {"language": "Burmese",    "lang_code": "my", "brand_srcs": []},
    "KH": {"language": "Khmer",      "lang_code": "km", "brand_srcs": []},
    "MN": {"language": "Mongolian",  "lang_code": "mn", "brand_srcs": []},
    "UZ": {"language": "Uzbek",      "lang_code": "uz", "brand_srcs": []},
    # ── Middle East ───────────────────────────────────────────────────────────
    "SA": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "AE": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "EG": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "IQ": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "JO": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "LB": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "MA": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "TN": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "DZ": {"language": "Arabic",     "lang_code": "ar", "brand_srcs": []},
    "IL": {"language": "Hebrew",     "lang_code": "he", "brand_srcs": []},
    "IR": {"language": "Persian",    "lang_code": "fa", "brand_srcs": []},
    # ── Africa ────────────────────────────────────────────────────────────────
    "ZA": {"language": "English",    "lang_code": "en", "brand_srcs": []},
    "NG": {"language": "English",    "lang_code": "en", "brand_srcs": []},
    "KE": {"language": "Swahili",    "lang_code": "sw", "brand_srcs": []},
    "ET": {"language": "Amharic",    "lang_code": "am", "brand_srcs": []},
    "GH": {"language": "English",    "lang_code": "en", "brand_srcs": []},
    "TZ": {"language": "Swahili",    "lang_code": "sw", "brand_srcs": []},
    "UG": {"language": "English",    "lang_code": "en", "brand_srcs": []},
    "SN": {"language": "French",     "lang_code": "fr", "brand_srcs": []},
    "CI": {"language": "French",     "lang_code": "fr", "brand_srcs": []},
    "CM": {"language": "French",     "lang_code": "fr", "brand_srcs": []},
}

# Default country set when --countries is not specified
DEFAULT_COUNTRIES = [
    "US", "CA", "MX", "BR",
    "GB", "DE", "FR", "ES", "IT",
    "AU", "JP",
]

# Built-in starter drug list
DEFAULT_DRUGS = [
    # Analgesics / NSAIDs
    "Ibuprofen", "Acetaminophen", "Aspirin", "Naproxen", "Diclofenac",
    "Celecoxib", "Tramadol", "Morphine", "Codeine", "Oxycodone",
    # Antibiotics
    "Amoxicillin", "Azithromycin", "Ciprofloxacin", "Doxycycline",
    "Metronidazole", "Clindamycin", "Cephalexin", "Trimethoprim",
    # Cardiovascular
    "Atorvastatin", "Simvastatin", "Rosuvastatin", "Lisinopril",
    "Amlodipine", "Metoprolol", "Carvedilol", "Losartan",
    "Warfarin", "Clopidogrel", "Furosemide", "Hydrochlorothiazide",
    # Diabetes
    "Metformin", "Glipizide", "Sitagliptin", "Empagliflozin",
    # CNS / Psychiatry
    "Sertraline", "Fluoxetine", "Escitalopram", "Amitriptyline",
    "Alprazolam", "Diazepam", "Zolpidem", "Gabapentin", "Pregabalin",
    # Respiratory
    "Salbutamol", "Montelukast", "Fluticasone", "Budesonide",
    "Cetirizine", "Loratadine", "Fexofenadine",
    # GI / Other
    "Omeprazole", "Pantoprazole", "Metoclopramide", "Loperamide",
    "Prednisone", "Dexamethasone", "Levothyroxine",
    "Fluconazole", "Acyclovir", "Oseltamivir",
]

# Codex CSV column order
CSV_FIELDS = [
    "InChI Key", "Generic Name", "Brand Name",
    "Country", "Source Language", "Language Code", "Source",
]


# ── HTTP session ──────────────────────────────────────────────────────────────

def _make_session(openfda_key: str = "") -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "CodexDrugPipeline/3.0 (multilingual pharmaceutical tool)",
        "Accept":     "application/json",
    })
    if openfda_key:
        s.params = {"api_key": openfda_key}  # type: ignore[assignment]
    return s


def _get(session: requests.Session, url: str,
         params: dict | None = None,
         retries: int = 3,
         timeout: int = 15) -> Optional[dict]:
    """GET with retry and exponential backoff. Returns parsed JSON or None."""
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = 2.0 * (2 ** attempt)
                log.warning("Rate limited — waiting %.0fs", wait)
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            log.debug("Timeout attempt %d/%d: %s", attempt + 1, retries, url)
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
        except requests.RequestException as exc:
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
            else:
                log.debug("Request failed: %s  (%s)", url, exc)
    return None


# ── Deduplication ─────────────────────────────────────────────────────────────

def _norm(brand: str) -> str:
    """
    Normalise a brand name for deduplication comparison.
    Strips punctuation, whitespace, and lowercases so that
    "Advil", "ADVIL", and "Advil." all map to the same key.
    """
    return re.sub(r"[\s\-_./,;:()\[\]']", "", brand.lower())


def merge_brands(brand_lists: list[list[str]],
                 generic_name: str = "") -> list[str]:
    """
    Merge multiple brand name lists into a single deduplicated, sorted list.

    Deduplication is by normalised form — case, punctuation, and spacing
    differences are ignored.  The first occurrence (from the highest-priority
    source) is kept for display.  Entries that match the generic name are
    excluded to avoid noise.
    """
    generic_key = _norm(generic_name)
    seen:   set[str] = set()
    merged: list[str] = []

    for brands in brand_lists:
        for brand in brands:
            brand = brand.strip()
            if not brand:
                continue
            key = _norm(brand)
            if not key:
                continue
            if key == generic_key:
                continue   # brand name is the same as the generic — skip
            if key in seen:
                continue   # already have this brand from an earlier source
            seen.add(key)
            merged.append(brand)

    return sorted(merged)


# ── PubChem ───────────────────────────────────────────────────────────────────
#
# REST API docs: https://pubchemdocs.ncbi.nlm.nih.gov/pug-rest
# Rate limit: 5 requests/second

PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"


def pubchem_inchikey(session: requests.Session, name: str) -> Optional[str]:
    """Return the Standard InChI Key for a drug by name, or None."""
    url  = f"{PUBCHEM_BASE}/compound/name/{requests.utils.quote(name)}/property/InChIKey/JSON"
    data = _get(session, url)
    time.sleep(0.25)
    if not data:
        return None
    try:
        return data["PropertyTable"]["Properties"][0]["InChIKey"]
    except (KeyError, IndexError):
        return None


# ── Wikidata ──────────────────────────────────────────────────────────────────
#
# SPARQL docs: https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service
# No API key required.

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"


def wikidata_names(session: requests.Session,
                   inchi_key: str,
                   lang_codes: list[str]) -> dict[str, str]:
    """
    Return {lang_code: generic_name} for all requested languages via Wikidata.
    Languages with no label are omitted from the result.
    """
    lang_filter = ", ".join(f'"{lc}"' for lc in lang_codes)
    query = f"""
    SELECT ?label (LANG(?label) AS ?lang) WHERE {{
      ?drug wdt:P235 "{inchi_key}" .
      ?drug rdfs:label ?label .
      FILTER(LANG(?label) IN ({lang_filter}))
    }}
    """
    data = _get(session, WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                timeout=10, retries=2)
    time.sleep(0.5)
    if not data:
        return {}

    NON_LATIN = {"ja", "zh", "ar", "ko", "ru", "el", "hi", "uk",
                 "he", "fa", "am", "ka", "hy", "be", "bg", "mk",
                 "sr", "mn", "si", "my", "km", "th", "ur", "bn"}
    names: dict[str, str] = {}
    try:
        for b in data["results"]["bindings"]:
            lang  = b["lang"]["value"]
            label = b["label"]["value"]
            names[lang] = label if lang in NON_LATIN else label.capitalize()
    except KeyError:
        pass
    return names


# ── Brand name fetchers ───────────────────────────────────────────────────────
#
# Convention:
#   - Each fetcher signature:  f(session, english_name) -> list[str]
#   - Return title-cased brand names, no deduplication needed here
#     (merge_brands() handles that centrally)
#   - Return [] on any failure — never raise
#   - Include a polite sleep after each API call


# ── openFDA (US) ──────────────────────────────────────────────────────────────
# Docs: https://open.fda.gov/apis/drug/ndc/
# Searches FDA's NDC product database by generic name.

def _brands_openfda(session: requests.Session, name: str) -> list[str]:
    data = _get(session, "https://api.fda.gov/drug/ndc.json",
                params={"search": f'generic_name:"{name}"', "limit": 100})
    time.sleep(0.15)
    if not data:
        return []
    brands: list[str] = []
    for r in data.get("results", []):
        brand = (r.get("brand_name") or "").strip()
        if brand:
            brands.append(brand.title())
    return brands


# ── DailyMed (US — NIH drug label database) ───────────────────────────────────
# Docs: https://dailymed.nlm.nih.gov/dailymed/webservices-help/v2/
# Searches FDA-submitted drug labels. Broader coverage than NDC alone —
# catches drugs whose NDC entry is missing or incomplete.

def _brands_dailymed(session: requests.Session, name: str) -> list[str]:
    data = _get(
        session,
        "https://dailymed.nlm.nih.gov/dailymed/services/v2/drugnames.json",
        params={"drug_name": name, "pagesize": 200},
    )
    time.sleep(0.2)
    if not data:
        return []
    brands: list[str] = []
    try:
        for entry in data.get("data", []):
            brand = (entry.get("drug_name") or "").strip()
            if brand:
                brands.append(brand.title())
    except (KeyError, TypeError):
        pass
    return brands


# ── RxNorm (US — NIH normalised drug nomenclature) ────────────────────────────
# Docs: https://lhncbc.nlm.nih.gov/RxNav/APIs/RxNormAPIs.html
# Returns brand-name (BN) and semantic branded drug (SBD) concepts from
# the RxNorm graph.  Catches proprietary names that FDA databases may omit.

def _brands_rxnorm(session: requests.Session, name: str) -> list[str]:
    data = _get(
        session,
        "https://rxnav.nlm.nih.gov/REST/drugs.json",
        params={"name": name},
    )
    time.sleep(0.15)
    if not data:
        return []
    brands: list[str] = []
    try:
        for group in data.get("drugGroup", {}).get("conceptGroup", []):
            # tty BN = Brand Name, SBD = Semantic Branded Drug
            if group.get("tty") in ("BN", "SBD"):
                for concept in group.get("conceptProperties", []):
                    brand = (concept.get("name") or "").strip()
                    if brand:
                        # RxNorm names often include dose form — strip it
                        # e.g. "Advil 200 MG Oral Tablet" → "Advil"
                        brand = brand.split(" ")[0].title()
                        brands.append(brand)
    except (KeyError, TypeError):
        pass
    return brands


# ── Health Canada Drug Product Database (CA) ──────────────────────────────────
# Docs: https://health-products.canada.ca/api/drug/
# No API key required.

def _brands_health_canada(session: requests.Session, name: str) -> list[str]:
    data = _get(
        session,
        "https://health-products.canada.ca/api/drug/drugproduct/",
        params={"lang": "en", "type": "json", "drugname": name},
    )
    time.sleep(0.25)
    if not data:
        return []
    brands: list[str] = []
    products = data if isinstance(data, list) else data.get("data", [])
    for p in products:
        brand = (p.get("BRAND_NAME") or "").strip()
        if brand:
            brands.append(brand.title())
    return brands


# ── EMA — European Medicines Agency ──────────────────────────────────────────
# Linked Data SPARQL: https://linkeddata.ema.europa.eu/sparql
# Covers all centrally authorised products across EU/EEA.
# Nationally-authorised-only products are not included.

def _brands_ema(session: requests.Session, name: str) -> list[str]:
    query = f"""
    SELECT DISTINCT ?productName WHERE {{
      ?product a <https://spor.ema.europa.eu/v1/lists/MedicinalProduct> .
      ?product <https://spor.ema.europa.eu/v1/lists/hasActiveSubstance> ?sub .
      ?sub rdfs:label ?subName .
      FILTER(LCASE(STR(?subName)) = LCASE("{name}"))
      ?product rdfs:label ?productName .
    }}
    LIMIT 100
    """
    data = _get(
        session,
        "https://linkeddata.ema.europa.eu/sparql",
        params={"query": query, "format": "application/sparql-results+json"},
        timeout=20,
    )
    time.sleep(0.5)
    if not data:
        return []
    brands: list[str] = []
    try:
        for b in data["results"]["bindings"]:
            brand = b["productName"]["value"].strip()
            if brand:
                brands.append(brand.title())
    except KeyError:
        pass
    return brands


# ── MHRA — Medicines & Healthcare products Regulatory Agency (UK) ─────────────
# API: https://products.mhra.gov.uk/api/
# Covers UK-nationally authorised products (post-Brexit).
# Combined with EMA for GB to catch both centrally and nationally authorised.
# Note: MHRA's public API endpoint may change — verify at products.mhra.gov.uk
#       if results are unexpectedly empty.

def _brands_mhra(session: requests.Session, name: str) -> list[str]:
    data = _get(
        session,
        "https://products.mhra.gov.uk/api/",
        params={"search": name, "page": 1, "type": "medicine"},
    )
    time.sleep(0.3)
    if not data:
        return []
    brands: list[str] = []
    try:
        for product in data.get("products", data.get("results", [])):
            brand = (
                product.get("productName") or
                product.get("name") or
                product.get("title") or ""
            ).strip()
            if brand:
                brands.append(brand.title())
    except (KeyError, TypeError):
        pass
    return brands


# ── TGA — Therapeutic Goods Administration (AU) ───────────────────────────────
# Register: https://www.tga.gov.au/resources/artg
# Note: TGA's public JSON endpoint has changed over time.
# If this returns empty, check the current TGA ARTG search API at:
# https://www.tga.gov.au/resources/artg

def _brands_tga(session: requests.Session, name: str) -> list[str]:
    data = _get(
        session,
        "https://www.tga.gov.au/resources/artg/search-results",
        params={"query": name, "type": "prescription", "format": "json"},
    )
    time.sleep(0.3)
    if not data:
        return []
    brands: list[str] = []
    results = data.get("results", []) if isinstance(data, dict) else []
    for r in results:
        brand = (r.get("trade_name") or r.get("name") or "").strip()
        if brand:
            brands.append(brand.title())
    return brands


# ── ANVISA — Agência Nacional de Vigilância Sanitária (BR) ────────────────────
# Docs: https://consultas.anvisa.gov.br/#/medicamentos/
# Covers all medications authorised for the Brazilian market.
# No API key required.

def _brands_anvisa(session: requests.Session, name: str) -> list[str]:
    data = _get(
        session,
        "https://consultas.anvisa.gov.br/api/consulta/medicamentos/",
        params={"count": 50, "filter[nomeProduto]": name},
        timeout=20,
    )
    time.sleep(0.4)
    if not data:
        return []
    brands: list[str] = []
    try:
        for product in data.get("content", data.get("result", [])):
            brand = (
                product.get("nomeProduto") or
                product.get("nome") or ""
            ).strip()
            if brand:
                brands.append(brand.title())
    except (KeyError, TypeError):
        pass
    return brands


# ── PMDA — Pharmaceuticals and Medical Devices Agency (JP) ────────────────────
# API: https://api.pmda.go.jp/
# Returns Japanese drug product names (brand names in Japanese).
# Results are in Japanese script — appropriate for the JP locale.
# No API key required.

def _brands_pmda(session: requests.Session, name: str) -> list[str]:
    # PMDA's search works best with the English INN name
    data = _get(
        session,
        "https://api.pmda.go.jp/drug/v1/drugs",
        params={"name": name, "pageSize": 50},
        timeout=20,
    )
    time.sleep(0.4)
    if not data:
        return []
    brands: list[str] = []
    try:
        for item in data.get("items", data.get("result", [])):
            brand = (
                item.get("brandName") or
                item.get("name")      or
                item.get("namej")     or ""  # Japanese name field
            ).strip()
            if brand:
                brands.append(brand)   # preserve original casing for Japanese
    except (KeyError, TypeError):
        pass
    return brands


# ── Brand fetcher registry ────────────────────────────────────────────────────
#
# Add new fetchers here after implementing them above.
# Key must match the string used in brand_srcs in COUNTRY_CONFIG.

BRAND_FETCHERS: dict[str, callable] = {
    "openfda":       _brands_openfda,
    "dailymed":      _brands_dailymed,
    "rxnorm":        _brands_rxnorm,
    "health_canada": _brands_health_canada,
    "ema":           _brands_ema,
    "mhra":          _brands_mhra,
    "tga":           _brands_tga,
    "anvisa":        _brands_anvisa,
    "pmda":          _brands_pmda,
}


def fetch_from_source(session: requests.Session, src_key: str,
                      english_name: str) -> list[str]:
    """Call a single brand fetcher safely. Returns [] on any failure."""
    fetcher = BRAND_FETCHERS.get(src_key)
    if not fetcher:
        return []
    try:
        return fetcher(session, english_name)
    except Exception as exc:
        log.debug("Brand fetch error src=%s name=%s: %s", src_key, english_name, exc)
        return []


# ── Data assembly ─────────────────────────────────────────────────────────────

def build_rows(inchi_key: str,
               wikidata_names_: dict[str, str],
               brands_by_country: dict[str, list[str]],
               sources_used_by_country: dict[str, list[str]],
               countries: list[str]) -> list[dict]:
    """
    Assemble Codex CSV rows for every requested country.

    - One row per brand name per country.
    - If no brand names available, one row with empty Brand Name.
    - Countries whose language has no Wikidata entry are skipped.
    - Source column records all sources that contributed data.
    """
    rows = []

    for country in countries:
        cfg     = COUNTRY_CONFIG[country]
        lang_cd = cfg["lang_code"]
        generic = wikidata_names_.get(lang_cd)

        if not generic:
            continue

        brands      = brands_by_country.get(country) or [""]
        srcs_used   = sources_used_by_country.get(country, [])
        src_label   = "/".join(["PubChem", "Wikidata"] + srcs_used) if srcs_used \
                      else "PubChem/Wikidata"

        for brand in brands:
            rows.append({
                "InChI Key":       inchi_key,
                "Generic Name":    generic,
                "Brand Name":      brand,
                "Country":         country,
                "Source Language": cfg["language"],
                "Language Code":   lang_cd,
                "Source":          src_label,
            })

    return rows


# ── Output ────────────────────────────────────────────────────────────────────

def write_csvs(all_rows: list[dict], output_dir: Path,
               countries: list[str]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    by_country: dict[str, list[dict]] = {c: [] for c in countries}
    for row in all_rows:
        c = row["Country"]
        if c in by_country:
            by_country[c].append(row)

    for country, rows in by_country.items():
        if not rows:
            continue
        lang_code = COUNTRY_CONFIG[country]["lang_code"]
        path = output_dir / f"drugs_{lang_code}_{country}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        log.info("  %-36s %d rows", path.name, len(rows))


def write_summary(results: list[dict], output_dir: Path) -> None:
    path   = output_dir / "lookup_summary.csv"
    fields = ["Drug Name", "InChI Key", "Languages Found",
              "Languages Missing", "Countries With Brands", "Brand Sources Used"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    log.info("  %-36s %d drugs", path.name, len(results))


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(drug_names: list[str], countries: list[str],
        output_dir: Path, openfda_key: str = "") -> None:

    session    = _make_session(openfda_key)
    lang_codes = sorted({COUNTRY_CONFIG[c]["lang_code"] for c in countries})

    # Collect all unique source keys needed across all countries
    all_src_keys: set[str] = set()
    for c in countries:
        all_src_keys.update(COUNTRY_CONFIG[c]["brand_srcs"])
    all_src_keys_sorted = sorted(all_src_keys)

    print(f"\n  Codex drug data pipeline")
    print(f"  {'─' * 48}")
    print(f"  Drugs        : {len(drug_names)}")
    print(f"  Countries    : {', '.join(countries)}")
    print(f"  Languages    : {', '.join(lang_codes)}")
    print(f"  Brand sources: {', '.join(all_src_keys_sorted) or 'none'}")
    print(f"  Output       : {output_dir}")
    print()

    total  = len(drug_names)
    found  = 0
    failed = 0
    all_rows: list[dict] = []
    summary:  list[dict] = []

    for i, name in enumerate(drug_names, 1):
        print(f"  [{i:>3}/{total}]  {name:<26}", end=" ", flush=True)

        try:
            # Step 1 — PubChem → InChI Key
            inchi_key = pubchem_inchikey(session, name)
            if not inchi_key:
                print("✗  not found in PubChem")
                failed += 1
                summary.append({
                    "Drug Name":           name,
                    "InChI Key":           "",
                    "Languages Found":     "",
                    "Languages Missing":   "all (PubChem lookup failed)",
                    "Countries With Brands": "",
                    "Brand Sources Used":  "",
                })
                continue

            # Step 2 — Wikidata → multilingual generic names
            wk_names = wikidata_names(session, inchi_key, lang_codes)
            if "en" not in wk_names:
                wk_names["en"] = name.capitalize()
            en_name = wk_names.get("en", name)

            # Step 3 — Fetch each unique brand source ONCE per drug
            # Cache by source key so e.g. all 27 EU countries share one EMA call
            raw_by_src: dict[str, list[str]] = {}
            for src_key in all_src_keys_sorted:
                raw_by_src[src_key] = fetch_from_source(session, src_key, en_name)

            # Step 4 — Merge and deduplicate per country
            brands_by_country:      dict[str, list[str]] = {}
            srcs_used_by_country:   dict[str, list[str]] = {}

            for country in countries:
                srcs = COUNTRY_CONFIG[country]["brand_srcs"]
                if not srcs:
                    brands_by_country[country]    = []
                    srcs_used_by_country[country] = []
                    continue

                raw_lists  = [raw_by_src.get(s, []) for s in srcs]
                merged     = merge_brands(raw_lists, generic_name=en_name)
                srcs_used  = [s for s in srcs if raw_by_src.get(s)]

                brands_by_country[country]    = merged
                srcs_used_by_country[country] = srcs_used

            # Step 5 — Assemble rows
            rows = build_rows(
                inchi_key, wk_names,
                brands_by_country, srcs_used_by_country,
                countries,
            )
            all_rows.extend(rows)

            langs_found           = sorted(wk_names.keys())
            langs_missing         = sorted(set(lang_codes) - set(langs_found))
            countries_with_brands = sorted(
                c for c, b in brands_by_country.items() if b
            )
            brand_total = sum(len(b) for b in brands_by_country.values())
            all_srcs_used = sorted({
                s for srcs in srcs_used_by_country.values() for s in srcs
            })

            print(f"✓  {inchi_key}  "
                  f"langs={len(langs_found)}  "
                  f"brands={brand_total}")

            summary.append({
                "Drug Name":             name,
                "InChI Key":             inchi_key,
                "Languages Found":       ", ".join(langs_found),
                "Languages Missing":     ", ".join(langs_missing) or "none",
                "Countries With Brands": ", ".join(countries_with_brands) or "none",
                "Brand Sources Used":    ", ".join(all_srcs_used) or "none",
            })
            found += 1

        except Exception as exc:
            print(f"✗  unexpected error — skipping  ({exc})")
            failed += 1
            summary.append({
                "Drug Name":             name,
                "InChI Key":             "",
                "Languages Found":       "",
                "Languages Missing":     f"all (error: {exc})",
                "Countries With Brands": "",
                "Brand Sources Used":    "",
            })
            continue

    print()
    print("  Writing output files…")
    if all_rows:
        write_csvs(all_rows, output_dir, countries)
        write_summary(summary, output_dir)
    else:
        log.warning("No data collected — no CSV files written")

    print()
    print(f"  {'─' * 48}")
    print(f"  Resolved  : {found}/{total} drugs")
    if failed:
        print(f"  Not found : {failed} (see lookup_summary.csv)")
    print(f"  Output    : {output_dir}")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    country_list = ", ".join(sorted(COUNTRY_CONFIG.keys()))
    p = argparse.ArgumentParser(
        description="Fetch multilingual drug data and output Codex CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Supported countries:
  {country_list}

Examples:
  python fetch_drug_data.py
  python fetch_drug_data.py --countries US,CA,MX,FR,DE,BR,JP,UA
  python fetch_drug_data.py --drugs "Ibuprofen,Aspirin" --countries US,DE,JP
  python fetch_drug_data.py --file my_drugs.txt --countries GB,FR,ES
  python fetch_drug_data.py --openfda-key YOUR_KEY --countries US,CA
        """,
    )
    p.add_argument("--drugs",  "-d",
                   help="Comma-separated drug names  (default: built-in ~50)")
    p.add_argument("--file",   "-f",
                   help="Text file with one drug name per line  (#comments ok)")
    p.add_argument("--countries", "-c",
                   help=f"Comma-separated ISO country codes  "
                        f"(default: {', '.join(DEFAULT_COUNTRIES)})")
    p.add_argument("--output", "-o",
                   default=str(Path(__file__).resolve().parent),
                   help="Output directory  (default: same folder as this script)")
    p.add_argument("--openfda-key",
                   default="",
                   help="openFDA API key — free at open.fda.gov/apis/authentication/")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Drug list
    if args.file:
        path = Path(args.file)
        if not path.exists():
            sys.exit(f"File not found: {path}")
        drug_names = [
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        ]
    elif args.drugs:
        drug_names = [d.strip() for d in args.drugs.split(",") if d.strip()]
    else:
        drug_names = DEFAULT_DRUGS

    if not drug_names:
        sys.exit("No drug names provided.")

    # Country list
    if args.countries:
        requested = [c.strip().upper() for c in args.countries.split(",")]
        unknown   = [c for c in requested if c not in COUNTRY_CONFIG]
        if unknown:
            sys.exit(
                f"Unknown country codes: {', '.join(unknown)}\n"
                f"Supported: {', '.join(sorted(COUNTRY_CONFIG.keys()))}"
            )
        countries = requested
    else:
        countries = DEFAULT_COUNTRIES

    run(
        drug_names  = drug_names,
        countries   = countries,
        output_dir  = Path(args.output),
        openfda_key = args.openfda_key,
    )


if __name__ == "__main__":
    main()
