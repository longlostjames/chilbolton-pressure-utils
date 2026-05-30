#!/usr/bin/env python3
"""Apply .corr QC files to existing PTB110 NetCDF files."""

import argparse
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr
from netCDF4 import Dataset

from .qc_corrections import load_qc_from_corr_file


def find_nc_file_for_date(input_dir, date_obj, year=None):
    """Find the NetCDF file corresponding to a given date."""
    date_str = date_obj.strftime("%Y%m%d")
    search_dir = Path(input_dir) / str(year) if year is not None else Path(input_dir)

    if not search_dir.exists():
        return None

    candidates = sorted(search_dir.glob(f"*{date_str}*.nc"))
    return candidates[0] if candidates else None


def apply_corr_to_file(nc_file, corr_file, target_date):
    """Apply QC from a .corr file to one NetCDF file."""
    missing_fill_value = np.int8(-128)
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%S")
    revised_timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")
    history_entry = f"{timestamp} - Applied .corr QC flags using apply-ptb110-corr-files"

    with xr.open_dataset(nc_file, decode_times=False) as ds:
        if "time" not in ds.sizes:
            raise ValueError(f"No 'time' dimension found in {nc_file}")

        num_points = int(ds.sizes["time"])
        qc_values = load_qc_from_corr_file(
            num_points,
            corr_file=corr_file,
            target_date=target_date,
            default_flag=0,
            bad_flag=2,
        )

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

        # Keep valid range metadata in sync with the actual QC values written.
        if qc_values.size > 0:
            ds["qc_flag_air_pressure"].attrs["valid_min"] = np.int8(np.min(qc_values))
            ds["qc_flag_air_pressure"].attrs["valid_max"] = np.int8(np.max(qc_values))

        ds["qc_flag_air_pressure"].encoding["_FillValue"] = missing_fill_value

        qc_to_drop = [
            name for name in ds.data_vars
            if name.startswith("qc_flag_") and name != "qc_flag_air_pressure"
        ]
        ds_to_write = ds.drop_vars(qc_to_drop) if qc_to_drop else ds
        if qc_to_drop:
            print(f"[INFO] Removed non-pressure QC flag variable(s): {', '.join(sorted(qc_to_drop))}")

        tmp_file = f"{nc_file}.tmp"
        ds_to_write.to_netcdf(tmp_file)

    shutil.move(tmp_file, nc_file)

    # Update global attributes on the final file to guarantee metadata persistence.
    with Dataset(nc_file, mode="r+") as ds_nc:
        existing_history = ds_nc.getncattr("history") if "history" in ds_nc.ncattrs() else ""
        if existing_history:
            ds_nc.setncattr("history", f"{history_entry}\n{existing_history}")
        else:
            ds_nc.setncattr("history", history_entry)
        if "last_modified" in ds_nc.ncattrs():
            ds_nc.delncattr("last_modified")
        ds_nc.setncattr("last_revised_date", revised_timestamp)
        if "time" in ds_nc.variables:
            ds_nc.variables["time"].setncattr("units", "seconds since 1970-01-01 00:00:00")


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


def dates_covered_by_corr_file(corr_file):
    """Return sorted dates covered by a .corr file.

    Supported naming conventions:
        YYYYMMDD.corr (daily)
        YYYYMM.corr   (monthly multi-day; rows must begin with YYYYMMDD)
    """
    stem = Path(corr_file).stem
    if len(stem) == 8 and stem.isdigit():
        return [datetime.strptime(stem, "%Y%m%d")]

    if len(stem) != 6 or not stem.isdigit():
        raise ValueError(f"Correction file name must be YYYYMMDD.corr or YYYYMM.corr, got: {corr_file}")

    dates = set()
    with Path(corr_file).open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split(",") if part.strip()]
            if len(parts) >= 3 and len(parts[0]) == 8 and parts[0].isdigit():
                date_obj = datetime.strptime(parts[0], "%Y%m%d")
                if date_obj.strftime("%Y%m") != stem:
                    raise ValueError(
                        f"Line {line_number} in {corr_file} has date {parts[0]} outside file month {stem}."
                    )
                dates.add(date_obj)

    if not dates:
        raise ValueError(
            f"Monthly correction file {corr_file} contains no YYYYMMDD-prefixed rows."
        )

    return sorted(dates)


def main():
    """CLI entry point for apply-ptb110-corr-files command."""
    parser = argparse.ArgumentParser(
        description=(
            "Apply corrections/YYYY/*.corr QC files to PTB110 NetCDF files. "
            "Supports daily YYYYMMDD.corr and monthly YYYYMM.corr with YYYYMMDD-prefixed rows."
        ),
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
        help="Base directory containing corrections/YYYY/*.corr files",
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
            corr_dates = dates_covered_by_corr_file(corr_file)
        except ValueError as exc:
            print(f"Skipping {corr_file}: {exc}")
            skipped += 1
            continue

        for corr_date in corr_dates:
            nc_file = find_nc_file_for_date(args.input_dir, corr_date, year=args.year)
            if nc_file is None:
                print(f"No NetCDF file found for {corr_date.strftime('%Y-%m-%d')}, skipping")
                skipped += 1
                continue

            try:
                apply_corr_to_file(nc_file, corr_file, target_date=corr_date)
                print(f"Updated {nc_file.name} using {corr_file.name} ({corr_date.strftime('%Y-%m-%d')})")
                processed += 1
            except Exception as exc:
                print(f"Error processing {nc_file} with {corr_file}: {exc}")
                skipped += 1

    print(f"\nDone. Processed: {processed}, skipped: {skipped}")


if __name__ == "__main__":
    main()
