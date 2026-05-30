"""
# Process Vaisala PTB110 barometric pressure sensor data from Format5 to netCDF
"""

import polars as pl
import numpy as np
import ncas_amof_netcdf_template as nant
import datetime as dt
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager
import cftime
from datetime import timezone

import re
import os
import xarray as xr
from .read_format5_content import read_format5_content
from .read_format5_header import read_format5_header
from .read_format5_chdb import read_format5_chdb
from .qc_corrections import load_qc_from_corr_file, apply_qc_to_netcdf

try:
    from . import __version__
except ImportError:
    __version__ = "unknown"

# NOTE: The Format5 channel name for barometric pressure is assumed to be 'baro_ch'.
# Verify this against the f5channelDB.chdb for the Chilbolton site before processing.
PRESSURE_CHANNEL = "baro_ch"


def _has_non_missing_data(var):
    """Return True if a variable contains at least one non-missing value."""
    values = var.values

    if np.ma.isMaskedArray(values):
        if values.count() > 0:
            return True
        values = values.filled(np.nan)

    arr = np.asarray(values)
    if arr.size == 0:
        return False

    invalid = np.zeros(arr.shape, dtype=bool)

    for attr_name in ("_FillValue", "missing_value"):
        candidate = None
        if attr_name in var.encoding:
            candidate = var.encoding.get(attr_name)
        elif attr_name in var.attrs:
            candidate = var.attrs.get(attr_name)
        if candidate is None:
            continue

        try:
            invalid |= arr == candidate
        except Exception:
            pass

    if np.issubdtype(arr.dtype, np.floating):
        invalid |= np.isnan(arr)
    elif arr.dtype.kind in {"U", "S", "O"}:
        invalid |= arr == ""

    return bool(np.any(~invalid))


def _fallback_remove_unused_variables(file_name):
    """Drop empty non-core variables without relying on remote CV lookups."""
    core_keep = {
        "time",
        "day_of_year",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "air_pressure",
        "qc_flag_air_pressure",
        "latitude",
        "longitude",
    }

    with xr.open_dataset(file_name) as ds:
        to_drop = []
        for var_name in ds.data_vars:
            if var_name in core_keep or var_name in ds.coords:
                continue
            if _has_non_missing_data(ds[var_name]):
                continue
            to_drop.append(var_name)

        if not to_drop:
            print("[INFO] Fallback cleanup: no empty variables to remove.")
            return

        cleaned = ds.drop_vars(to_drop)
        tmp_file = f"{file_name}.clean.tmp"
        cleaned.to_netcdf(tmp_file)

    os.replace(tmp_file, file_name)
    print(f"[INFO] Fallback cleanup removed {len(to_drop)} empty variable(s): {', '.join(sorted(to_drop))}")


def _remove_non_pressure_qc_flags(file_name):
    """Remove all qc_flag_* variables except qc_flag_air_pressure."""
    with xr.open_dataset(file_name) as ds:
        qc_to_drop = [
            name for name in ds.data_vars
            if name.startswith("qc_flag_") and name != "qc_flag_air_pressure"
        ]

        if not qc_to_drop:
            return

        cleaned = ds.drop_vars(qc_to_drop)
        tmp_file = f"{file_name}.qconly.tmp"
        cleaned.to_netcdf(tmp_file)

    os.replace(tmp_file, file_name)
    print(f"[INFO] Removed non-pressure QC flag variable(s): {', '.join(sorted(qc_to_drop))}")


def _download_amf_cvs(amf_cvs_root):
    """Download AMF_CVs and instrument vocab TSV files from GitHub for offline use."""
    import requests as _requests
    print("[INFO] Downloading AMF_CVs files from GitHub for offline use...")

    amf_tag = nant.values.get_latest_CVs_version()
    inst_tag = nant.values.get_latest_instrument_CVs_version()

    tsv_dir = amf_cvs_root / amf_tag / "product-definitions" / "tsv"
    amf_base = f"https://raw.githubusercontent.com/ncasuk/AMF_CVs/{amf_tag}/product-definitions/tsv"
    inst_base = f"https://raw.githubusercontent.com/ncasuk/ncas-data-instrument-vocabs/{inst_tag}/product-definitions/tsv"

    files = [
        (amf_base, "_common/global-attributes.tsv"),
        (amf_base, "_common/variables-land.tsv"),
        (amf_base, "_common/dimensions-land.tsv"),
        (amf_base, "_vocabularies/data-products.tsv"),
        (amf_base, "_vocabularies/file-naming.tsv"),
        (amf_base, "_vocabularies/creators.tsv"),
        (amf_base, "surface-met/variables-specific.tsv"),
        (amf_base, "surface-met/dimensions-specific.tsv"),
        (inst_base, "_instrument_vocabs/ncas-instrument-name-and-descriptors.tsv"),
        (inst_base, "_instrument_vocabs/community-instrument-name-and-descriptors.tsv"),
    ]

    for base_url, rel_path in files:
        dest = tsv_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        url = f"{base_url}/{rel_path}"
        resp = _requests.get(url, timeout=30)
        resp.raise_for_status()
        dest.write_text(resp.text)
        print(f"[INFO]   Downloaded {rel_path}")

    versions_file = amf_cvs_root / "versions.txt"
    versions_file.write_text(f"amf_cvs_tag={amf_tag}\ninstrument_cvs_tag={inst_tag}\n")
    print(f"[INFO] AMF_CVs downloaded (AMF: {amf_tag}, instruments: {inst_tag})")


@contextmanager
def _nant_local_files():
    """Context manager that patches nant's _check_website_exists so it can load
    TSV files from the local amf_cvs/ directory without needing internet access.
    If the local files are absent, downloads them from GitHub first.
    Yields (use_local_files, tag) for passing to make_product_netcdf."""
    _amf_cvs_root = Path(__file__).parent / "amf_cvs"
    _versions_file = _amf_cvs_root / "versions.txt"

    if not _versions_file.exists():
        print("[INFO] amf_cvs/versions.txt not found - downloading from GitHub...")
        try:
            _download_amf_cvs(_amf_cvs_root)
        except Exception as _e:
            print(f"[WARNING] Failed to download AMF_CVs ({_e}) - falling back to live GitHub lookup.")
            yield None, "latest"
            return

    _versions = dict(
        line.split("=", 1)
        for line in _versions_file.read_text().splitlines()
        if "=" in line
    )
    _tag = _versions.get("amf_cvs_tag", "latest")
    _use_local = str(_amf_cvs_root)

    # nant calls requests.get() on local file paths, which fails.
    # Patch _check_website_exists to use os.path.isfile for non-HTTP paths.
    from ncas_amof_netcdf_template.file_info import FileInfo
    _orig = FileInfo._check_website_exists

    def _patched(self, url):
        if not url.startswith("http"):
            return os.path.isfile(url)
        return _orig(self, url)

    FileInfo._check_website_exists = _patched
    try:
        yield _use_local, _tag
    finally:
        FileInfo._check_website_exists = _orig


def preprocess_data_f5(infile):
    """
    Preprocesses a Format5 data file to extract a Polars DataFrame with
    TIMESTAMP and BP_mbar_Avg columns.  Applies the rawrange/realrange
    calibration from the channel database.
    """
    print(f"Processing file: {infile}")

    # Step 1: Read the Format5 header
    header = read_format5_header(infile)

    # Step 2: Read the Format5 content
    df = read_format5_content(infile, header)
    print(df)

    # Load the channel database from the package installation directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    chdb_file = os.path.join(script_dir, "f5channelDB.chdb")
    chdb = read_format5_chdb(chdb_file)

    print(chdb[PRESSURE_CHANNEL])

    # Extract rawrange and realrange for the pressure channel
    baro_rawrange = chdb[PRESSURE_CHANNEL]["rawrange"]
    baro_realrange = chdb[PRESSURE_CHANNEL]["realrange"]

    def map_to_real_range(raw_col, raw_range, real_range):
        raw_min, raw_max = raw_range["lower"], raw_range["upper"]
        real_min, real_max = real_range["lower"], real_range["upper"]
        return (pl.col(raw_col) - raw_min) / (raw_max - raw_min) * (real_max - real_min) + real_min

    # Ensure the pressure column is numeric
    df = df.with_columns([
        pl.col(PRESSURE_CHANNEL).cast(pl.Float64)
    ])

    # Apply the calibration mapping
    df = df.with_columns([
        map_to_real_range(PRESSURE_CHANNEL, baro_rawrange, baro_realrange).alias("BP_mbar_Avg")
    ])

    # Keep only TIMESTAMP and pressure columns
    df = df.select(["TIMESTAMP", "BP_mbar_Avg"])

    return df


def process_file(infile, outdir="./", metadata_file="metadata_f5.json", corr_file=None):
    df = preprocess_data_f5(infile)
    print(df)

    # Check if the year of the last timestamp is one greater than the previous timestamp
    if df["TIMESTAMP"][-1].year > df["TIMESTAMP"][-2].year:
        print("[INFO] Adjusting the last timestamp to be one year earlier temporarily.")
        original_last_timestamp = df["TIMESTAMP"][-1]
        adjusted_last_timestamp = original_last_timestamp.replace(year=original_last_timestamp.year - 1)
        df = df.with_columns(
            pl.when(pl.col("TIMESTAMP") == original_last_timestamp)
            .then(adjusted_last_timestamp)
            .otherwise(pl.col("TIMESTAMP"))
            .alias("TIMESTAMP")
        )

    # Get all the time formats
    unix_times, day_of_year, years, months, days, hours, minutes, seconds, time_coverage_start_unix, time_coverage_end_unix, file_date = nant.util.get_times(df["TIMESTAMP"])

    # Restore the original last timestamp for correction later
    if "adjusted_last_timestamp" in locals():
        print("[INFO] Restoring the original last timestamp for correction.")
        unix_times[-1] = int(original_last_timestamp.replace(tzinfo=timezone.utc).timestamp())
        day_of_year[-1] = original_last_timestamp.timetuple().tm_yday
        years[-1] = original_last_timestamp.year
        months[-1] = original_last_timestamp.month
        days[-1] = original_last_timestamp.day
        hours[-1] = original_last_timestamp.hour
        minutes[-1] = original_last_timestamp.minute
        seconds[-1] = original_last_timestamp.second
        time_coverage_end_unix = unix_times[-1]

    file_date = f"{str(years[0])}{str(months[0]).zfill(2)}{str(days[0]).zfill(2)}"

    # Read product_version from metadata file
    import json
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    product_version = metadata.get('product_version', 'v1.1').lstrip('v')

    # Create NetCDF file
    with _nant_local_files() as (_use_local, _tag):
        nc = nant.create_netcdf.make_product_netcdf("surface-met", "ncas-pressure-1", date=file_date,
                                     dimension_lengths={"time": len(unix_times)},
                                     file_location=outdir, platform="cao",
                                     product_version=product_version,
                                     use_local_files=_use_local,
                                     tag=_tag)
    if isinstance(nc, list):
        print("[WARNING] Unexpectedly got multiple netCDFs returned from nant.create_netcdf.main, just using first file...")
        nc = nc[0]

    # Add time variable data to NetCDF file
    nant.util.update_variable(nc, "time", unix_times)
    nant.util.update_variable(nc, "day_of_year", day_of_year)
    nant.util.update_variable(nc, "year", years)
    nant.util.update_variable(nc, "month", months)
    nant.util.update_variable(nc, "day", days)
    nant.util.update_variable(nc, "hour", hours)
    nant.util.update_variable(nc, "minute", minutes)
    nant.util.update_variable(nc, "second", seconds)

    # Correct the last timestamp and year in the NetCDF file
    if "adjusted_last_timestamp" in locals():
        print("[INFO] Correcting the last timestamp and year in the NetCDF file.")
        corrected_last_unix_time = int(original_last_timestamp.replace(tzinfo=timezone.utc).timestamp())
        nc.variables["time"][-1] = corrected_last_unix_time

        if "valid_max" in nc.variables["time"].ncattrs():
            nc.variables["time"].setncattr("valid_max", max(corrected_last_unix_time, nc.variables["time"].getncattr("valid_max")))

        nc.variables["year"][-1] = original_last_timestamp.year

        if "valid_max" in nc.variables["year"].ncattrs():
            nc.variables["year"].setncattr("valid_max", max(original_last_timestamp.year, nc.variables["year"].getncattr("valid_max")))

    # Add pressure data to NetCDF file
    nant.util.update_variable(nc, "air_pressure", df["BP_mbar_Avg"])

    # Initialize QC to 0 and apply optional corrections from daily .corr file.
    if corr_file and not Path(corr_file).exists():
        print(f"[WARNING] Correction file not found: {corr_file} (using default QC=0)")
    elif corr_file:
        print(f"[INFO] Applying QC corrections from {corr_file}")
    qc_target_date = datetime(int(years[0]), int(months[0]), int(days[0]))
    qc_values = load_qc_from_corr_file(
        len(unix_times),
        corr_file=corr_file,
        target_date=qc_target_date,
        default_flag=0,
        bad_flag=2,
    )
    apply_qc_to_netcdf(nc, qc_values, variable_name="qc_flag_air_pressure")

    # Add time_coverage_start and time_coverage_end metadata
    nc.setncattr(
        "time_coverage_start",
        dt.datetime.fromtimestamp(time_coverage_start_unix, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    )
    nc.setncattr(
        "time_coverage_end",
        dt.datetime.fromtimestamp(time_coverage_end_unix, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    )

    # Add metadata from file
    nant.util.add_metadata_to_netcdf(nc, metadata_file)

    # Set processing software version from package
    version_str = __version__ if __version__.startswith('v') else f"v{__version__}"
    nc.setncattr("processing_software_version", version_str)

    # Ensure the 'time' variable has the correct units and values
    if "time" in nc.variables:
        print("[INFO] Correcting the 'time' variable in the NetCDF file.")
        nc.variables["time"].setncattr("units", "seconds since 1970-01-01 00:00:00")
        nc.variables["time"].setncattr("standard_name", "time")
        nc.variables["time"].setncattr("long_name", "Time (seconds since 1970-01-01 00:00:00)")
        nc.variables["time"].setncattr("axis", "T")
        nc.variables["time"].setncattr("calendar", "standard")
        print(nc['time'])
        time_values = nc.variables["time"][:]
        corrected_time_values = [
            cftime.date2num(
                cftime.num2date(t, "seconds since 1970-01-01 00:00:00"),
                "seconds since 1970-01-01 00:00:00"
            )
            for t in time_values
        ]
        nc.variables["time"][:] = corrected_time_values
        if len(corrected_time_values) > 0:
            nc.variables["time"].setncattr("valid_min", float(min(corrected_time_values)))
            nc.variables["time"].setncattr("valid_max", float(max(corrected_time_values)))

    # Close file, remove empty variables
    file_name = nc.filepath()
    nc.close()
    try:
        nant.remove_empty_variables.main(file_name)
    except Exception as e:
        print(f"[WARNING] remove_empty_variables failed (GitHub lookup may be unavailable): {e}")
        try:
            _fallback_remove_unused_variables(file_name)
        except Exception as fallback_exc:
            print(f"[WARNING] fallback variable cleanup also failed: {fallback_exc}")

    try:
        _remove_non_pressure_qc_flags(file_name)
    except Exception as qc_exc:
        print(f"[WARNING] non-pressure QC flag cleanup failed: {qc_exc}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process Vaisala PTB110 barometric pressure data from Format5 to netCDF")
    parser.add_argument("infile", type=str, help="Input Format5 file")
    parser.add_argument("-o", "--outdir", type=str, default="./", help="Output directory")
    parser.add_argument("-m", "--metadata_file", type=str, default="metadata_f5.json", help="Metadata file")
    parser.add_argument("--corr-file", type=str, default=None, help="Optional daily .corr file to apply QC flags")
    args = parser.parse_args()
    process_file(args.infile, outdir=args.outdir, metadata_file=args.metadata_file, corr_file=args.corr_file)


def main():
    """CLI entry point for process-ptb110-f5 command."""
    import argparse
    parser = argparse.ArgumentParser(description="Process Vaisala PTB110 barometric pressure data from Format5 to netCDF")
    parser.add_argument("infile", type=str, help="Input Format5 file")
    parser.add_argument("-o", "--outdir", type=str, default="./", help="Output directory")
    parser.add_argument("-m", "--metadata_file", type=str, default="metadata_f5.json", help="Metadata file")
    parser.add_argument("--corr-file", type=str, default=None, help="Optional daily .corr file to apply QC flags")
    args = parser.parse_args()
    process_file(args.infile, outdir=args.outdir, metadata_file=args.metadata_file, corr_file=args.corr_file)
