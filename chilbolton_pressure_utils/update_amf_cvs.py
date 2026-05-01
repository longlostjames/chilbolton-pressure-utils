#!/usr/bin/env python3
"""
Download AMF CVs TSV files for offline use on compute nodes without internet access.

Run this script on a machine with internet access whenever the CVs version changes.
Files are stored under amf_cvs/{tag}/product-definitions/tsv/ to match the directory
structure that nant expects when use_local_files is set.

Usage:
    python update_amf_cvs.py
    python update_amf_cvs.py --amf-cvs-tag v2.2.0 --instrument-cvs-tag v5
"""

import argparse
from pathlib import Path

import requests


AMF_CVS_RAW = "https://raw.githubusercontent.com/ncasuk/AMF_CVs"
INSTRUMENT_CVS_RAW = "https://raw.githubusercontent.com/ncasuk/ncas-data-instrument-vocabs"


def download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    dest.write_text(r.text, encoding="utf-8")
    print(f"  {dest.relative_to(dest.parents[3])}")


def main():
    parser = argparse.ArgumentParser(description="Download AMF CVs TSV files for offline nant use.")
    parser.add_argument("--amf-cvs-tag", default=None,
                        help="AMF_CVs release tag (default: latest)")
    parser.add_argument("--instrument-cvs-tag", default=None,
                        help="ncas-data-instrument-vocabs release tag (default: latest)")
    args = parser.parse_args()

    # amf_cvs/ lives next to this script inside the package
    amf_cvs_root = Path(__file__).parent / "amf_cvs"

    # --- Resolve tags ---
    if args.amf_cvs_tag:
        amf_tag = args.amf_cvs_tag
    else:
        print("Fetching latest AMF_CVs release tag...")
        r = requests.get("https://github.com/ncasuk/AMF_CVs/releases/latest",
                         timeout=30, allow_redirects=True)
        amf_tag = r.url.split("/")[-1]

    if args.instrument_cvs_tag:
        inst_tag = args.instrument_cvs_tag
    else:
        print("Fetching latest ncas-data-instrument-vocabs release tag...")
        r = requests.get("https://github.com/ncasuk/ncas-data-instrument-vocabs/releases/latest",
                         timeout=30, allow_redirects=True)
        inst_tag = r.url.split("/")[-1]

    print(f"AMF_CVs tag:              {amf_tag}")
    print(f"Instrument vocabs tag:    {inst_tag}")

    # nant resolves files as: {use_local_files}/{tag}/product-definitions/tsv/...
    # We use amf_tag as the single tag for both repos so process files only need one tag.
    tsv_dir = amf_cvs_root / amf_tag / "product-definitions" / "tsv"
    print(f"Output directory:         {tsv_dir}\n")

    # --- AMF_CVs TSV files ---
    amf_base = f"{AMF_CVS_RAW}/{amf_tag}/product-definitions/tsv"
    amf_files = [
        "_common/global-attributes.tsv",
        "_common/variables-land.tsv",
        "_common/variables-sea.tsv",
        "_common/variables-air.tsv",
        "_common/variables-trajectory.tsv",
        "_common/dimensions-land.tsv",
        "_common/dimensions-sea.tsv",
        "_common/dimensions-air.tsv",
        "_common/dimensions-trajectory.tsv",
        "_vocabularies/data-products.tsv",
        "_vocabularies/creators.tsv",
        "_vocabularies/file-naming.tsv",
        "surface-met/variables-specific.tsv",
        "surface-met/dimensions-specific.tsv",
    ]
    print(f"Downloading AMF_CVs ({amf_tag}) TSV files...")
    for rel_path in amf_files:
        download_file(f"{amf_base}/{rel_path}", tsv_dir / rel_path)

    # --- Instrument vocabs TSV files (stored under same amf_tag dir for simplicity) ---
    inst_base = f"{INSTRUMENT_CVS_RAW}/{inst_tag}/product-definitions/tsv"
    inst_files = [
        "_instrument_vocabs/ncas-instrument-name-and-descriptors.tsv",
        "_instrument_vocabs/community-instrument-name-and-descriptors.tsv",
    ]
    print(f"\nDownloading ncas-data-instrument-vocabs ({inst_tag}) TSV files...")
    for rel_path in inst_files:
        download_file(f"{inst_base}/{rel_path}", tsv_dir / rel_path)

    # Record which versions were downloaded
    (amf_cvs_root / "versions.txt").write_text(
        f"amf_cvs_tag={amf_tag}\ninstrument_cvs_tag={inst_tag}\n",
        encoding="utf-8",
    )
    print(f"\nVersions recorded in {amf_cvs_root / 'versions.txt'}")
    print("Done.")


if __name__ == "__main__":
    main()
