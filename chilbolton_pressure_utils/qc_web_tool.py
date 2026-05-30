#!/usr/bin/env python3
"""Web-based QC editor for PTB110 daily .corr files."""

if __name__ == "__main__":
    try:
        from .qc_flask_tool import main as _flask_main
    except ImportError:
        from qc_flask_tool import main as _flask_main

    _flask_main()
    raise SystemExit(0)

from datetime import datetime
from pathlib import Path
import argparse
import subprocess
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

try:
    from .qc_corrections import build_daily_correction_path, load_qc_from_corr_file
except ImportError:
    # Support direct execution via `python qc_web_tool.py`.
    from qc_corrections import build_daily_correction_path, load_qc_from_corr_file


def _find_nc_file_for_date(input_dir, day):
    search_dir = Path(input_dir) / f"{day.year:04d}"
    if not search_dir.exists():
        search_dir = Path(input_dir)
    candidates = sorted(search_dir.glob(f"*{day.strftime('%Y%m%d')}*.nc"))
    return candidates[0] if candidates else None


def _read_corr_as_dataframe(corr_file):
    rows = []
    if not corr_file.exists():
        return pd.DataFrame(columns=["start_idx", "end_idx", "flag"])

    with corr_file.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split(",") if part.strip()]
            if len(parts) not in (2, 3):
                continue
            flag = int(parts[2]) if len(parts) == 3 else 2
            rows.append(
                {
                    "start_idx": int(parts[0]),
                    "end_idx": int(parts[1]),
                    "flag": flag,
                }
            )

    return pd.DataFrame(rows, columns=["start_idx", "end_idx", "flag"])


def _validate_intervals(intervals_df, num_points):
    validated = []
    for i, row in intervals_df.iterrows():
        if pd.isna(row["start_idx"]) or pd.isna(row["end_idx"]):
            continue
        start_idx = int(row["start_idx"])
        end_idx = int(row["end_idx"])
        flag = int(row["flag"]) if not pd.isna(row["flag"]) else 2

        if start_idx < 0 or end_idx < 0:
            raise ValueError(f"Row {i + 1}: start_idx/end_idx must be >= 0")
        if start_idx > end_idx:
            raise ValueError(f"Row {i + 1}: start_idx must be <= end_idx")
        if end_idx >= num_points:
            raise ValueError(f"Row {i + 1}: end_idx must be <= {num_points - 1}")

        validated.append((start_idx, end_idx, flag))

    return validated


def _write_corr_file(corr_file, intervals):
    corr_file.parent.mkdir(parents=True, exist_ok=True)
    with corr_file.open("w", encoding="utf-8") as handle:
        handle.write("# start_idx,end_idx[,flag]\n")
        for start_idx, end_idx, flag in intervals:
            if flag == 2:
                handle.write(f"{start_idx},{end_idx}\n")
            else:
                handle.write(f"{start_idx},{end_idx},{flag}\n")


def _build_preview_qc(num_points, intervals):
    qc = np.zeros(num_points, dtype=np.int8)
    for start_idx, end_idx, flag in intervals:
        qc[start_idx:end_idx + 1] = np.int8(flag)
    if intervals:
        qc[qc == 0] = 1
    return qc


def run_app():
    try:
        import streamlit as st
    except ImportError as exc:
        raise SystemExit(
            "streamlit is required for the web QC tool. Install with: "
            "pip install 'chilbolton-pressure-utils[qcweb]'"
        ) from exc

    st.set_page_config(page_title="PTB110 QC Tool", layout="wide")
    st.title("PTB110 Web QC Tool")
    st.caption("Create and edit daily .corr files for PTB110 pressure data.")

    with st.sidebar:
        st.header("Inputs")
        input_dir = st.text_input("NetCDF input directory", value="./")
        corrections_base = st.text_input("Corrections base directory", value="./corrections")
        selected_date = st.date_input("Date", value=datetime.utcnow().date())

    day = datetime.combine(selected_date, datetime.min.time())
    corr_file = build_daily_correction_path(corrections_base, day)
    nc_file = _find_nc_file_for_date(input_dir, day)

    st.write(f"NetCDF file: {nc_file if nc_file else 'Not found'}")
    st.write(f"Correction file: {corr_file}")

    if not nc_file:
        st.warning("No NetCDF file found for selected date.")
        return

    with xr.open_dataset(nc_file) as ds:
        if "air_pressure" not in ds or "time" not in ds:
            st.error("Dataset must contain 'time' and 'air_pressure'.")
            return

        time_vals = pd.to_datetime(ds["time"].values)
        pressure_vals = ds["air_pressure"].values
        num_points = int(ds.sizes["time"])

    if "corr_df" not in st.session_state or st.session_state.get("corr_file_path") != str(corr_file):
        st.session_state.corr_df = _read_corr_as_dataframe(corr_file)
        st.session_state.corr_file_path = str(corr_file)

    st.subheader("Edit Correction Intervals")
    st.write("Use index ranges from 0 to {}. Omit flag to default to 2 (bad_data).".format(num_points - 1))

    edited_df = st.data_editor(
        st.session_state.corr_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "start_idx": st.column_config.NumberColumn("start_idx", step=1),
            "end_idx": st.column_config.NumberColumn("end_idx", step=1),
            "flag": st.column_config.NumberColumn("flag", step=1),
        },
        key="interval_editor",
    )

    left_col, right_col = st.columns(2)

    with left_col:
        if st.button("Save .corr file", type="primary"):
            try:
                intervals = _validate_intervals(edited_df, num_points)
                _write_corr_file(corr_file, intervals)
                st.session_state.corr_df = edited_df
                st.success(f"Saved {corr_file}")
            except Exception as exc:
                st.error(f"Failed to save: {exc}")

    with right_col:
        if st.button("Reload from disk"):
            st.session_state.corr_df = _read_corr_as_dataframe(corr_file)
            st.rerun()

    st.subheader("Preview")
    try:
        intervals = _validate_intervals(edited_df, num_points)
        qc_preview = _build_preview_qc(num_points, intervals)
        good_count = int(np.sum(qc_preview == 1))
        bad_count = int(np.sum(qc_preview == 2))
        not_used_count = int(np.sum(qc_preview == 0))
        st.write(f"QC counts: good_data={good_count}, bad_data={bad_count}, not_used={not_used_count}")

        fig, ax = plt.subplots(figsize=(14, 4))
        ax.plot(time_vals, pressure_vals, linewidth=0.8, color="black", label="air_pressure")

        for start_idx, end_idx, flag in intervals:
            if flag == 1:
                color = "#2ca02c"
                alpha = 0.15
            elif flag == 2:
                color = "#d62728"
                alpha = 0.25
            else:
                color = "#1f77b4"
                alpha = 0.15
            ax.axvspan(time_vals[start_idx], time_vals[end_idx], color=color, alpha=alpha)

        ax.set_title(f"{selected_date} air_pressure with QC interval shading")
        ax.set_ylabel("Pressure (hPa)")
        ax.grid(alpha=0.3)
        st.pyplot(fig)

        if corr_file.exists():
            qc_from_disk = load_qc_from_corr_file(num_points, corr_file=str(corr_file), default_flag=0, good_flag=1, bad_flag=2)
            st.caption(
                "Saved-file QC counts: "
                f"good_data={int(np.sum(qc_from_disk == 1))}, "
                f"bad_data={int(np.sum(qc_from_disk == 2))}, "
                f"not_used={int(np.sum(qc_from_disk == 0))}"
            )

    except Exception as exc:
        st.error(f"Preview error: {exc}")


def _running_under_streamlit():
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
    except Exception:
        return False

    return get_script_run_ctx() is not None


def main():
    """Launch the Streamlit QC app as a command-line tool."""
    parser = argparse.ArgumentParser(description="Launch PTB110 web QC tool")
    parser.add_argument("--port", type=int, default=8501, help="Port for Streamlit server")
    args = parser.parse_args()

    if _running_under_streamlit():
        run_app()
        return

    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(Path(__file__).resolve()),
        "--server.port",
        str(args.port),
    ]
    subprocess.run(command, check=False)


if __name__ == "__main__":
    main()
