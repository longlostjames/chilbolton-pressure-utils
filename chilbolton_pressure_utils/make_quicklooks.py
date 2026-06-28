#!/usr/bin/env python3
"""
Generate quicklook plots for PTB110 pressure data with QC flags.
"""

import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import argparse
from datetime import datetime

try:
    from . import __version__
except ImportError:
    __version__ = "unknown"


def get_flag_intervals(qc_flag, time_coord, flag_value=2):
    """Return list of (start_time, end_time) tuples where the specified QC flag value occurs."""
    flag_mask = (qc_flag == flag_value).values
    times = pd.to_datetime(time_coord.values)
    intervals = []

    start = None
    for i, val in enumerate(flag_mask):
        if val and start is None:
            start = times[i]
        elif not val and start is not None:
            end = times[i - 1]
            intervals.append((start, end))
            start = None
    if start is not None:
        intervals.append((start, times[-1]))
    return intervals


def plot_day(ds, nc_filename, outdir):
    """Plot air_pressure with QC flags, shading bad data regions."""
    if ds.time.size == 0:
        print(f"Skipping {nc_filename}: no data")
        return

    try:
        date_str = [s for s in nc_filename.split('_') if s.isdigit() and len(s) == 8][0]
        date_label = pd.to_datetime(date_str, format="%Y%m%d").strftime('%Y-%m-%d')
    except IndexError:
        date_label = "unknown_date"

    day_start = pd.to_datetime(date_label)
    day_end = day_start + pd.Timedelta(days=1)

    time = ds['time'].values

    fig, ax = plt.subplots(figsize=(14, 5))
    try:
        ax.plot(time, ds['air_pressure'], color='steelblue', label='Air pressure')

        # Shade bad data regions (flag=2)
        if 'qc_flag_air_pressure' in ds:
            bad_intervals = get_flag_intervals(ds['qc_flag_air_pressure'], ds['time'], flag_value=2)
            for i, (start, end) in enumerate(bad_intervals):
                label = "Bad data (flag=2)" if i == 0 else None
                ax.axvspan(start, end, color='grey', alpha=0.4, label=label)

        ax.set_ylabel('Air pressure (hPa)')
        ax.set_xlabel('Time (UTC)')
        ax.set_title(f'Air pressure from Vaisala PTB110 sensor at Chilbolton Observatory — {date_label}')
        ax.legend()
        ax.grid(True)
        ax.set_xlim(day_start, day_end)
        ax.xaxis.set_major_locator(mdates.HourLocator(byhour=range(0, 24, 3)))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

        # Fixed margins so axes are the same size across all daily plots.
        # Left margin sized for a 6-digit y-tick label (e.g. 1013.4).
        fig.subplots_adjust(left=0.08, right=0.98, top=0.92, bottom=0.12)

        outfile = os.path.join(outdir, f"{nc_filename.replace('.nc', '.png')}")
        plt.savefig(outfile, dpi=200)
        print(f"Saved {outfile}")
    finally:
        plt.close(fig)


def _extract_file_date(filename):
    """Extract datetime from a NetCDF filename containing an 8-digit YYYYMMDD token."""
    stem = os.path.splitext(os.path.basename(filename))[0]
    for part in stem.split('_'):
        if part.isdigit() and len(part) == 8:
            try:
                return datetime.strptime(part, "%Y%m%d")
            except ValueError:
                pass
    return None


def main():
    """CLI entry point for make-ptb110-quicklooks command."""
    parser = argparse.ArgumentParser(description="Generate daily QC flag plots for PTB110 pressure NetCDF files.")
    parser.add_argument(
        "-i", "--input_dir",
        default="/gws/ssde/j25a/chil_atmos/processing/stfc-pressure-1/data/20240401_longterm/",
        help="Base directory containing yearly subdirectories of NetCDF files"
    )
    parser.add_argument(
        "-o", "--output_dir",
        default="/gws/ssde/j25a/chil_atmos/processing/stfc-pressure-1/data/20240401_longterm/quicklooks/",
        help="Base directory to save yearly subdirectories of PNG plots"
    )
    parser.add_argument(
        "-y", "--year",
        default=None,
        help="Year to process (e.g., 2024). Required unless --start-date/--end-date are given."
    )
    parser.add_argument(
        "-d", "--day",
        help="Specific day to process (format: YYYYMMDD). If not provided, all days in the year will be processed."
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Only process dates on or after this date (format: YYYYMMDD)."
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Only process dates on or before this date (format: YYYYMMDD)."
    )
    args = parser.parse_args()

    start_date = None
    end_date = None
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y%m%d")
        except ValueError:
            parser.error(f"--start-date must be in YYYYMMDD format, got: {args.start_date}")
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y%m%d")
        except ValueError:
            parser.error(f"--end-date must be in YYYYMMDD format, got: {args.end_date}")
    if start_date and end_date and start_date > end_date:
        parser.error("--start-date must not be later than --end-date")

    # Determine which years to scan
    if args.year:
        years = [args.year]
    elif start_date and end_date:
        years = [str(y) for y in range(start_date.year, end_date.year + 1)]
    elif start_date:
        years = [str(start_date.year)]
    elif end_date:
        years = [str(end_date.year)]
    else:
        parser.error("Must specify --year or at least one of --start-date / --end-date.")

    from pathlib import Path

    for year in years:
        input_dir = os.path.join(args.input_dir, year)
        output_dir = os.path.join(args.output_dir, year)

        os.makedirs(output_dir, exist_ok=True)

        input_path = Path(input_dir)
        if not input_path.exists():
            print(f"Input directory does not exist: {input_dir}")
            continue

        nc_files = sorted(input_path.rglob("*.nc"))

        if not nc_files:
            print(f"No .nc files found in: {input_dir}")
            continue

        if args.day:
            nc_files = [f for f in nc_files if args.day in f.name]
            if not nc_files:
                print(f"No .nc files found for the specified day: {args.day}")
                continue
        elif start_date or end_date:
            filtered = []
            for f in nc_files:
                file_date = _extract_file_date(f.name)
                if file_date is None:
                    filtered.append(f)
                    continue
                if start_date and file_date < start_date:
                    continue
                if end_date and file_date > end_date:
                    continue
                filtered.append(f)
            nc_files = filtered

        for nc_file in nc_files:
            try:
                ds = xr.open_dataset(nc_file, decode_times=False)
                if 'time' not in ds:
                    print(f"Skipping {nc_file.name}: no 'time' variable")
                    continue
                ds = xr.decode_cf(ds)
                ds = ds.sortby('time')
                plot_day(ds, nc_file.name, output_dir)
            except Exception as e:
                print(f"Failed to process {nc_file.name}: {e}")


if __name__ == "__main__":
    main()
