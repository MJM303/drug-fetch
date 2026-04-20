#!/usr/bin/env python3
"""
scripts/split_names.py
----------------------
Splits a drug name list into fixed-size chunks and writes the GitHub
Actions matrix JSON to $GITHUB_OUTPUT so the fetch job can parallelise.

Called by the 'prepare' workflow job.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--file",       required=True, help="Input name list (.txt)")
    p.add_argument("--chunk-size", required=True, type=int)
    p.add_argument("--output-dir", required=True, help="Directory to write chunk files")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read names — skip comments and blank lines
    names = [
        line.strip()
        for line in input_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]

    total_names  = len(names)
    chunk_size   = args.chunk_size
    total_chunks = math.ceil(total_names / chunk_size)

    print(f"Total names  : {total_names}")
    print(f"Chunk size   : {chunk_size}")
    print(f"Total chunks : {total_chunks}")

    # Write chunk files
    for i in range(total_chunks):
        start  = i * chunk_size
        end    = min(start + chunk_size, total_names)
        chunk  = names[start:end]
        chunk_path = output_dir / f"chunk_{i}.txt"
        chunk_path.write_text("\n".join(chunk) + "\n", encoding="utf-8")
        print(f"  chunk_{i}.txt : {len(chunk)} names  ({start}–{end-1})")

    # Build GitHub Actions matrix JSON
    matrix = {
        "chunk_index": list(range(total_chunks))
    }
    matrix_json = json.dumps(matrix)

    # Write to $GITHUB_OUTPUT
    github_output = os.environ.get("GITHUB_OUTPUT", "")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"matrix={matrix_json}\n")
            f.write(f"total_names={total_names}\n")
            f.write(f"total_chunks={total_chunks}\n")
    else:
        # Local testing fallback
        print(f"\nmatrix={matrix_json}")
        print(f"total_names={total_names}")
        print(f"total_chunks={total_chunks}")


if __name__ == "__main__":
    main()
