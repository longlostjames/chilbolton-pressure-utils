#!/usr/bin/env python3
"""Apply daily .corr QC files to existing PTB110 NetCDF files."""

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr

from .qc_corrections import load_qc_from_corr_file


def find_nc_file_for_date(input_dir, date_obj, year=None):
    """Find the NetCDF file corresponding to a given date."""
    date_str = date_obj.strftime("%Y%m%d")
    search_dir = Path(input_dir) / str(year) if year is not None else Path(input_dir)

    if not search_dir.exists():
        return None

    candidates = sorted(search_dir.glob(f"*{date_str}*.nc"))
    return candidates[0] if candidates else None


def apply_corr_to_file(nc_file, corr_file):
    """Apply QC from a .corr file to one NetCDF file."""
    missing_fill_value = np.int8(-128)

    with xr.open_dataset(nc_file, mode="r+") as ds:
        if "time" not in ds.dims:
            raise ValueError(f"No 'time' dimension found in {nc_file}")

        num_points = int(ds.dims["time"])
        qc_values = load_qc_from_corr_file(num_points, corr_file=corr_file, default_flag=0, bad_flag=2)

        if "qc_flag_air_pressure" in ds:
            ds["qc_flag_air_pressure"].values[:] = qc_values
        else:
            ds["qc_flag_air_pressure"] = (("time",), qc_values)
            ds["qc_flag_air_pressure"].attrs.update(
                {
                    "long_name": "Quality flag for air pressure",
                    "flag_values": [0, 1, 2],
                    "flag_meanings": "not_used good_data bad_data",
                    "valid_min": np.int8(0),
                    "valid_max": np.int8(2),
                }
            )

        ds["qc_flag_air_pressure"].encoding["_FillValue"] = missing_fill_value

        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        history_entry = f"{timestamp} - Applied daily .corr QC flags using apply-ptb110-corr-files"
        if "history" in ds.attrs:
            ds.attrs["history"] = f"{history_entry}\n{ds.attrs['history']}"
        else:
            ds.attrs["history"] = history_entry
        ds.attrs["last_modified"] = timestamp

        tmp_file = f"{nc_file}.tmp"
        ds.to_netcdf(tmp_file)

    shutil.move(tmp_file, nc_file)


def iter_corr_files(corrections_base, year=None):
    """Yield .corr files from corrections/YYYY folders."""
    base = Path(corrections_base)
    if year is not None:
        year_dir = base / f"{year:04d}"
        if year_dir.exists():
            for corr_file in sorted(year_dir.glob("*.corr")):
                yield corr_file
        return

    if not base.exists():
        return

    for year_dir in sorted(path for path in base.iterdir() if path.is_dir()):
        for corr_file in sorted(year_dir.glob("*.corr")):
            yield corr_file


def parse_corr_date(corr_file):
    """Parse YYYYMMDD date from a .corr filename stem."""
    stem = Path(corr_file).stem
    try:
        return datetime.strptime(stem, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"Correction file name must be YYYYMMDD.corr, got: {corr_file}") from exc


def main():
    """CLI entry point for apply-ptb110-corr-files command."""
    parser = argparse.ArgumentParser(
        description="Apply daily corrections/YYYY/YYYYMMDD.corr QC files to PTB110 NetCDF files.",
    )
    parser.add_argument(
        "-i",
        "--input-dir",
        required=True,
        help="Directory containing NetCDF files (or parent directory with year subdirectories)",
    )
    parser.add_argument(
        "-c",
        "--corrections-base",
        required=True,
        help="Base directory containing corrections/YYYY/YYYYMMDD.corr files",
    )
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        default=None,
        help="Optional year filter. Looks in corrections-base/YYYY and input-dir/YYYY.",
    )

    args = parser.parse_args()

    processed = 0
    skipped = 0

    corr_files = list(iter_corr_files(args.corrections_base, year=args.year))
    if not corr_files:
        print("No .corr files found to apply.")
        return

    for corr_file in corr_files:
        try:
            corr_date = parse_corr_date(corr_file)
        except ValueError as exc:
            print(f"Skipping {corr_file}: {exc}")
            skipped += 1
            continue

        nc_file = find_nc_file_for_date(args.input_dir, corr_date, year=args.year)
        if nc_file is None:
            print(f"No NetCDF file found for {corr_date.strftime('%Y-%m-%d')}, skipping")
            skipped += 1
            continue

        try:
            apply_corr_to_file(nc_file, corr_file)
            print(f"Updated {nc_file.name} using {corr_file.name}")
            processed += 1
        except Exception as exc:
            print(f"Error processing {nc_file} with {corr_file}: {exc}")
            skipped += 1

    print(f"\nDone. Processed: {processed}, skipped: {skipped}")


if __name__ == "__main__":
    main()
