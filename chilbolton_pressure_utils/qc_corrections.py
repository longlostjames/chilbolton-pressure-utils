"""Helpers for initializing and applying PTB110 QC flags from .corr files."""

from datetime import datetime
from pathlib import Path
import numpy as np


def build_daily_correction_path(corrections_base, date_obj):
    """Return corrections/YYYY/YYYYMMDD.corr path for a given date."""
    base_path = Path(corrections_base)
    return base_path / f"{date_obj.year:04d}" / f"{date_obj.strftime('%Y%m%d')}.corr"


def build_monthly_correction_path(corrections_base, date_obj):
    """Return corrections/YYYY/YYYYMM.corr path for a given date."""
    base_path = Path(corrections_base)
    return base_path / f"{date_obj.year:04d}" / f"{date_obj.strftime('%Y%m')}.corr"


def find_correction_file_for_date(corrections_base, date_obj):
    """Return existing daily or monthly .corr path for a given date, preferring daily."""
    daily = build_daily_correction_path(corrections_base, date_obj)
    if daily.exists():
        return daily

    monthly = build_monthly_correction_path(corrections_base, date_obj)
    if monthly.exists():
        return monthly

    return None


def parse_corr_intervals_from_file(corr_file, target_date=None, bad_flag=2):
    """Parse intervals from a .corr file.

    Supported line formats:
        start_idx,end_idx[,flag]
        YYYYMMDD,start_idx,end_idx[,flag]
    """
    intervals = []
    corr_path = Path(corr_file)

    with corr_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue

            parts = [item.strip() for item in line.split(",") if item.strip()]
            if len(parts) not in (2, 3, 4):
                raise ValueError(
                    f"Invalid .corr line {line_number} in {corr_path}: '{raw_line.strip()}'. "
                    "Expected start_idx,end_idx[,flag] or YYYYMMDD,start_idx,end_idx[,flag]."
                )

            line_date = None
            if len(parts) in (3, 4) and len(parts[0]) == 8 and parts[0].isdigit():
                line_date = datetime.strptime(parts[0], "%Y%m%d").date()
                offset = 1
            else:
                offset = 0

            if target_date is not None and line_date is not None and line_date != target_date:
                continue
            if target_date is not None and line_date is None and len(parts) in (2, 3):
                # Legacy daily files have no explicit date column; treat all lines as matching.
                pass

            start_idx = int(parts[offset])
            end_idx = int(parts[offset + 1])
            if len(parts) == offset + 3:
                flag_value = int(parts[offset + 2])
            else:
                flag_value = bad_flag

            intervals.append((start_idx, end_idx, flag_value))

    return intervals


def load_qc_from_corr_file(
    num_points,
    corr_file=None,
    target_date=None,
    default_flag=0,
    good_flag=1,
    bad_flag=2,
):
    """Load QC flags from a .corr file.

    Supported line formats (comma-separated):
        start_idx,end_idx[,flag]
        YYYYMMDD,start_idx,end_idx[,flag]

    Blank lines and anything after '#' are ignored.
    """
    qc = np.full(num_points, default_flag, dtype=np.int8)

    if not corr_file:
        return qc

    corr_path = Path(corr_file)
    if not corr_path.exists():
        return qc

    target_day = target_date.date() if hasattr(target_date, "date") else target_date
    intervals = parse_corr_intervals_from_file(corr_path, target_date=target_day, bad_flag=bad_flag)

    for start_idx, end_idx, flag_value in intervals:
        if start_idx > end_idx:
            raise ValueError(
                f"Invalid interval in {corr_path}: start_idx ({start_idx}) must be <= end_idx ({end_idx})."
            )
        if start_idx < 0 or end_idx >= num_points:
            raise ValueError(
                f"Invalid interval in {corr_path}: index range {start_idx}:{end_idx} is outside 0:{num_points - 1}."
            )

        qc[start_idx:end_idx + 1] = np.int8(flag_value)

    # A .corr file indicates QC review has happened for this day,
    # so any still-unset values are considered good data.
    qc[qc == np.int8(default_flag)] = np.int8(good_flag)

    return qc


def apply_qc_to_netcdf(nc, qc_values, variable_name="qc_flag_air_pressure"):
    """Write QC values to a NetCDF variable, creating it when absent."""
    qc_values = np.asarray(qc_values, dtype=np.int8)
    missing_fill_value = np.int8(-128)

    if variable_name not in nc.variables:
        qc_var = nc.createVariable(variable_name, "i1", ("time",), fill_value=missing_fill_value)
        qc_var.setncattr("long_name", "Quality flag for air pressure")
        qc_var.setncattr("flag_values", np.array([0, 1, 2], dtype=np.int8))
        qc_var.setncattr("flag_meanings", "not_used good_data bad_data")
        qc_var.setncattr("valid_min", np.int8(0))
        qc_var.setncattr("valid_max", np.int8(2))

    nc.variables[variable_name][:] = qc_values

    qc_var = nc.variables[variable_name]
    if "valid_min" in qc_var.ncattrs():
        qc_var.setncattr("valid_min", int(np.min(qc_values)))
    if "valid_max" in qc_var.ncattrs():
        qc_var.setncattr("valid_max", int(np.max(qc_values)))
