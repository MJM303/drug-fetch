#!/usr/bin/env python3
"""
scripts/merge_results.py
------------------------
Merges CSV outputs from all parallel fetch chunks into one file per
country, deduplicating on (InChI Key, Generic Name, Brand Name, Country).

Called by the 'merge' workflow job after all fetch jobs complete.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from pathlib import Path


CSV_FIELDS = [
    "InChI Key", "Generic Name", "Brand Name",
    "Country", "Source Language", "Language Code", "Source",
]


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir",  required=True,
                   help="Directory containing per-chunk artifact subdirs")
    p.add_argument("--output-dir", required=True,
                   help="Directory for merged output CSVs")
    return p.parse_args()


def main() -> None:
    args    = parse_args()
    in_dir  = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Collect every CSV across all chunk subdirectories
    # Structure: all-results/results-chunk-N/drugs_xx_XX.csv
    by_file: dict[str, list[dict]] = {}

    for chunk_dir in sorted(in_dir.iterdir()):
        if not chunk_dir.is_dir():
            continue
        for csv_path in sorted(chunk_dir.glob("*.csv")):
            fname = csv_path.name
            if fname not in by_file:
                by_file[fname] = []
            with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
                rows = list(csv.DictReader(f))
            by_file[fname].extend(rows)
            print(f"  {chunk_dir.name}/{fname}: {len(rows)} rows")

    if not by_file:
        print("No CSV files found — nothing to merge.")
        return

    # Merge each file group with deduplication
    total_in  = 0
    total_out = 0
    summaries = []

    for fname, rows in sorted(by_file.items()):
        total_in += len(rows)

        seen:    set[tuple]  = set()
        merged:  list[dict]  = []

        for r in rows:
            key = (
                norm(r.get("InChI Key",    "")),
                norm(r.get("Generic Name", "")),
                norm(r.get("Brand Name",   "")),
                norm(r.get("Country",      "")),
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append({f: r.get(f, "") for f in CSV_FIELDS})

        # Sort alphabetically
        merged.sort(key=lambda r: (
            norm(r["Generic Name"]),
            norm(r["Brand Name"]),
        ))

        out_path = out_dir / fname
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(merged)

        with_inchi = sum(1 for r in merged if r.get("InChI Key", "").strip())
        pct        = with_inchi * 100 // len(merged) if merged else 0
        total_out += len(merged)

        summaries.append({
            "file":       fname,
            "rows_in":    len(rows),
            "rows_out":   len(merged),
            "duplicates": len(rows) - len(merged),
            "with_inchi": with_inchi,
            "inchi_pct":  pct,
        })
        print(f"  → {fname}: {len(rows)} in → {len(merged)} out "
              f"({len(rows)-len(merged)} dupes removed, {pct}% InChI)")

    # Write lookup_summary merged
    all_summaries: list[dict] = []
    sum_fields = ["Drug Name","InChI Key","Languages Found",
                  "Languages Missing","Countries With Brands","Brand Sources Used"]
    for chunk_dir in sorted(in_dir.iterdir()):
        if not chunk_dir.is_dir():
            continue
        s_path = chunk_dir / "lookup_summary.csv"
        if s_path.exists():
            with open(s_path, encoding="utf-8-sig", errors="replace") as f:
                all_summaries.extend(csv.DictReader(f))

    if all_summaries:
        # Deduplicate by Drug Name
        seen_drugs: set[str] = set()
        deduped_summaries = []
        for s in all_summaries:
            k = norm(s.get("Drug Name", ""))
            if k not in seen_drugs:
                seen_drugs.add(k)
                deduped_summaries.append(s)
        deduped_summaries.sort(key=lambda r: norm(r.get("Drug Name", "")))

        with open(out_dir / "lookup_summary.csv", "w",
                  newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sum_fields,
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(deduped_summaries)
        print(f"  → lookup_summary.csv: {len(deduped_summaries)} drugs")

    # Final report
    print()
    print(f"  {'─' * 60}")
    print(f"  {'File':<36} {'In':>7} {'Out':>7} {'InChI%':>7}")
    print(f"  {'─' * 60}")
    for s in summaries:
        print(f"  {s['file']:<36} {s['rows_in']:>7} "
              f"{s['rows_out']:>7} {s['inchi_pct']:>6}%")
    print(f"  {'─' * 60}")
    print(f"  {'TOTAL':<36} {total_in:>7} {total_out:>7}")
    print()


if __name__ == "__main__":
    main()
