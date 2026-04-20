#!/usr/bin/env python3
"""
extract_drug_names.py — Extract a deduplicated English drug name list
=====================================================================
Reads all available Codex source files and normalized CSVs, extracts
the best English or Latin INN name for every unique compound, and
writes a clean text file suitable for use with fetch_drug_data.py.

Why English / Latin INN:
  fetch_drug_data.py queries PubChem by name to resolve InChI Keys.
  PubChem accepts English INN names and Latin INN names (e.g. "ibuprofenum")
  but does not reliably resolve Cyrillic or French names.  This script
  therefore extracts the English or Latin equivalent for every entry,
  including non-English files, using every available bridge:

    en_US   → Generic Name directly (already English)
    uk_UA   → Compendium UK latin INN  +  UTIS English page slug
              +  FIP English active ingredient  +  Wikidata label_en
    ru_UA   → Compendium RU latin INN  +  Wikidata label_en
    fr_FR   → Wikidata label_en (all French Wikidata entries have this)

Deduplication:
  Names are deduplicated by a normalised key (lowercase, strip
  punctuation and Latin endings).  Only the cleanest form is kept —
  English names are preferred over Latin INN, shorter names over
  longer compound names.

Output:
  drug_names.txt  — one name per line, alphabetically sorted,
                    written next to this script.

Usage
-----
  # Auto-discover all source files next to this script
  python extract_drug_names.py

  # Explicit paths
  python extract_drug_names.py --source-dir /path/to/Tables_for_database/
  python extract_drug_names.py --normalized-dir /path/to/normalized_csvs/
  python extract_drug_names.py --output /path/to/drug_names.txt
"""

from __future__ import annotations

import argparse
import ast
import csv
import re
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


# ── Name normalisation ────────────────────────────────────────────────────────

def norm(s: str) -> str:
    """Normalise to a deduplication key — lowercase, strip non-alphanumeric."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def strip_latin_ending(s: str) -> str:
    """
    Strip common Latin INN endings for deduplication root comparison.
    e.g. "ibuprofenum" and "ibuprofen" → same root → keep one entry.
    """
    for ending in ("um", "us", "ae", "is"):
        if s.endswith(ending) and len(s) > len(ending) + 3:
            return s[: -len(ending)]
    return s


def dedup_key(name: str) -> str:
    """Deduplication key: normalised + Latin ending stripped."""
    return strip_latin_ending(norm(name))


def is_latin_name(name: str) -> bool:
    """Return True if the name looks like a Latin INN (ends in -um/-us/-ae/-is)."""
    k = norm(name)
    return any(k.endswith(e) and len(k) > len(e) + 3
               for e in ("um", "us", "ae", "is"))


def clean(s: str) -> str:
    """Strip whitespace and surrounding quotes."""
    return (s or "").strip().strip('"').strip("'")


def parse_list_field(s: str) -> list[str]:
    """Parse fields stored as Python list strings: "['val1', 'val2']"."""
    s = clean(s)
    if not s or s in ("[]", "['']", ""):
        return []
    try:
        result = ast.literal_eval(s)
        if isinstance(result, list):
            return [str(v).strip() for v in result if str(v).strip()]
    except (ValueError, SyntaxError):
        pass
    # Fallback: extract quoted strings manually
    return [m.strip() for m in re.findall(r"'([^']+)'", s) if m.strip()]


# ── Loader helpers ────────────────────────────────────────────────────────────

def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        return list(csv.DictReader(f))


# ── Name collector ────────────────────────────────────────────────────────────

class NameCollector:
    """
    Collects drug names and deduplicates them.

    Preference order when two names share the same dedup key:
      1. English names (no Latin ending) — most PubChem-friendly
      2. Latin INN names
      3. Shorter names over longer ones (avoids verbose combo drug names)
    """

    def __init__(self) -> None:
        # dedup_key → (preferred_name, is_latin)
        self._names: dict[str, tuple[str, bool]] = {}
        self._counts: dict[str, int] = {}   # source → count

    def add(self, name: str, source: str = "") -> None:
        """Add a name to the collection."""
        name = clean(name)
        if not name or len(name) < 2:
            return
        # Skip obviously non-drug entries
        if re.match(r"^\d+[\-\d]*$", name):     # pure numbers
            return
        if len(name) > 120:                      # absurdly long
            return

        key  = dedup_key(name)
        if not key or len(key) < 2:
            return

        latin = is_latin_name(name)

        if key not in self._names:
            self._names[key] = (name, latin)
        else:
            existing, existing_latin = self._names[key]
            # Prefer English over Latin
            if existing_latin and not latin:
                self._names[key] = (name, False)
            # Prefer shorter (less likely to be a combo/verbose entry)
            elif not existing_latin and not latin:
                if len(name) < len(existing):
                    self._names[key] = (name, False)

        self._counts[source] = self._counts.get(source, 0) + 1

    def names(self) -> list[str]:
        """Return deduplicated names sorted alphabetically."""
        return sorted(n for n, _ in self._names.values())

    def report(self) -> dict[str, int]:
        return dict(sorted(self._counts.items()))


# ── Source extractors ─────────────────────────────────────────────────────────

def extract_drugbank(path: Path, col: NameCollector) -> None:
    """DrugBank vocabulary — common_name is the English INN."""
    for row in load_csv(path):
        name = clean(row.get("common_name", ""))
        if name:
            col.add(name, "DrugBank")
        # Also add synonyms — they may be known by alternate English names
        for syn in re.split(r"[|;]", row.get("synonyms", "")):
            syn = clean(syn)
            if syn and re.match(r"^[a-zA-Z]", syn):   # English-looking only
                col.add(syn, "DrugBank-synonyms")


def extract_rxterms_ing(path: Path, col: NameCollector) -> None:
    """RxTerms ingredients — ingredient is the English INN."""
    for row in load_csv(path):
        name = clean(row.get("ingredient", ""))
        if name:
            col.add(name, "RxTerms")


def extract_who(path: Path, col: NameCollector) -> None:
    """WHO Essential Medicines — medicine_name is English INN."""
    for row in load_csv(path):
        name = clean(row.get("medicine_name", "")).title()
        if name:
            col.add(name, "WHO")


def extract_wikidata(path: Path, col: NameCollector) -> None:
    """Wikidata — label_en is the English name."""
    for row in load_csv(path):
        name = clean(row.get("label_en", ""))
        if name:
            col.add(name, "Wikidata")


def extract_compendium(path: Path, col: NameCollector, label: str) -> None:
    """
    Compendium UK or RU — title_latin_name is the Latin INN.
    PubChem resolves Latin INNs reliably for approved drugs.
    """
    for row in load_csv(path):
        latin = clean(row.get("title_latin_name", ""))
        if latin:
            col.add(latin, label)


def extract_utis(path: Path, col: NameCollector) -> None:
    """UTIS — page_name_decoded is the English drug page name."""
    for row in load_csv(path):
        name = clean(row.get("page_name_decoded", ""))
        if name:
            col.add(name, "UTIS")


def extract_fip(path: Path, col: NameCollector) -> None:
    """FIP Equiv — activeingredient contains the English INN."""
    for row in load_csv(path):
        ingredients = parse_list_field(row.get("activeingredient", ""))
        for ing in ingredients:
            if ing and re.match(r"^[a-zA-Z]", ing):
                col.add(ing, "FIP")


def extract_normalized_csv(path: Path, col: NameCollector) -> None:
    """
    Normalized Codex CSV — if the file is English (lang_code = en),
    extract Generic Name directly.  Non-English files are skipped
    here because their generic names are in Cyrillic/French and
    PubChem cannot resolve them.  Their English equivalents are
    already captured by the source file extractors above.
    """
    for row in load_csv(path):
        if (row.get("Language Code", "").strip().lower() == "en"
                and row.get("Generic Name", "").strip()):
            col.add(clean(row["Generic Name"]), f"normalized:{path.stem}")


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extract a deduplicated English drug name list from all source files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_drug_names.py
  python extract_drug_names.py --source-dir ./Tables_for_database/
  python extract_drug_names.py --normalized-dir ./my_csvs/ --output my_names.txt
        """,
    )
    p.add_argument(
        "--source-dir", "-s",
        default=None,
        help=(
            "Directory containing the original source files "
            "(drugbank_vocabulary.csv, wikidata_names.csv, etc.)  "
            "Default: searches next to this script."
        ),
    )
    p.add_argument(
        "--normalized-dir", "-n",
        default=None,
        help=(
            "Directory containing normalized Codex CSVs "
            "(drugs_en_US.csv etc.)  "
            "Default: searches next to this script."
        ),
    )
    p.add_argument(
        "--output", "-o",
        default=str(SCRIPT_DIR / "drug_names.txt"),
        help="Output text file path  (default: drug_names.txt next to this script)",
    )
    return p.parse_args()


def find_dir(hint: str | None, candidates: list[Path]) -> Path | None:
    """Return the first existing directory from candidates."""
    if hint:
        p = Path(hint)
        return p if p.is_dir() else None
    for c in candidates:
        if c.is_dir():
            return c
    return None


def main() -> None:
    args = parse_args()

    # Locate source and normalized directories
    src_dir  = find_dir(
        args.source_dir,
        [SCRIPT_DIR, SCRIPT_DIR / "Tables for database",
         SCRIPT_DIR / "Tables_for_database"],
    )
    norm_dir = find_dir(
        args.normalized_dir,
        [SCRIPT_DIR, SCRIPT_DIR / "normalized_csvs"],
    )

    if not src_dir and not norm_dir:
        sys.exit(
            "Could not find source files.  "
            "Pass --source-dir and/or --normalized-dir."
        )

    col = NameCollector()

    # ── Source files ──────────────────────────────────────────────────────────
    if src_dir:
        print(f"  Source dir : {src_dir}")

        extract_drugbank(src_dir / "drugbank_vocabulary.csv", col)
        extract_rxterms_ing(src_dir / "rxterms_ing.csv", col)
        extract_who(src_dir / "who_essential.csv", col)
        extract_wikidata(src_dir / "wikidata_names.csv", col)
        extract_compendium(src_dir / "compendium_uk.csv", col, "Compendium-UK-latin")
        extract_compendium(src_dir / "compendium_ru.csv", col, "Compendium-RU-latin")
        extract_utis(src_dir / "utis_in_ua.csv", col)
        extract_fip(src_dir / "fip_equiv.csv", col)

    # ── Normalized CSVs (English only) ────────────────────────────────────────
    if norm_dir:
        print(f"  Normalized : {norm_dir}")
        for csv_path in sorted(norm_dir.glob("*.csv")):
            extract_normalized_csv(csv_path, col)

    # ── Write output ──────────────────────────────────────────────────────────
    output_path = Path(args.output)
    names = col.names()

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Drug names extracted from Codex source files\n")
        f.write("# Feed to fetch_drug_data.py:  python fetch_drug_data.py --file drug_names.txt\n")
        f.write("#\n")
        for name in names:
            f.write(name + "\n")

    # ── Report ────────────────────────────────────────────────────────────────
    print()
    print(f"  {'─' * 50}")
    print(f"  Unique drug names extracted : {len(names):>7}")
    print()
    print("  By source:")
    for src, count in col.report().items():
        print(f"    {src:<32} {count:>7} names added")
    print()
    print(f"  Output : {output_path}")
    print()
    print("  Next step:")
    print(f"    python fetch_drug_data.py --file {output_path.name} --countries US,UA,RU,FR")
    print()


if __name__ == "__main__":
    main()
