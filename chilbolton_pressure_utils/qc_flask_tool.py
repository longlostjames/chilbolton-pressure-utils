#!/usr/bin/env python3
"""Flask-based web QC editor for PTB110 monthly multi-day .corr files."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import argparse

import numpy as np
import pandas as pd
import xarray as xr

try:
    from flask import Flask, flash, get_flashed_messages, redirect, render_template_string, request, url_for
except ImportError:  # pragma: no cover - handled at runtime
    Flask = None
    flash = get_flashed_messages = redirect = render_template_string = request = url_for = None

try:
    import plotly.graph_objects as go
    import plotly.io as pio
except ImportError:  # pragma: no cover - handled at runtime
    go = None
    pio = None

try:
    from .qc_corrections import (
        build_daily_correction_path,
        build_monthly_correction_path,
        find_correction_file_for_date,
        parse_corr_intervals_from_file,
    )
except ImportError:  # pragma: no cover - support direct execution
    from qc_corrections import (
        build_daily_correction_path,
        build_monthly_correction_path,
        find_correction_file_for_date,
        parse_corr_intervals_from_file,
    )


app = Flask(__name__) if Flask is not None and go is not None and pio is not None else None
if app is not None:
    app.secret_key = "chilbolton-pressure-utils-qc"
    app.config["QC_DEFAULT_INPUT_DIR"] = "./"
    app.config["QC_DEFAULT_CORRECTIONS_BASE"] = "./corrections"


PAGE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PTB110 QC Tool</title>
  <style>
    :root {
      --bg: #f4f7fb;
      --panel: #ffffff;
      --text: #132238;
      --muted: #5b6777;
      --accent: #0f766e;
      --accent-2: #1d4ed8;
      --border: #d7e0ea;
    }
    body {
      margin: 0;
      background: linear-gradient(180deg, #eaf1f8 0%, #f8fafc 100%);
      color: var(--text);
      font-family: Arial, Helvetica, sans-serif;
    }
    .wrap { max-width: 1460px; margin: 0 auto; padding: 24px; }
    .hero, .card {
      background: rgba(255,255,255,0.92);
      border: 1px solid var(--border);
      border-radius: 18px;
      box-shadow: 0 10px 30px rgba(16, 32, 51, 0.08);
    }
    .hero { padding: 20px 24px; margin-bottom: 18px; }
    .card { padding: 18px; margin-bottom: 18px; }
    h1, h2 { margin: 0 0 12px 0; }
    p { margin-top: 0; }
    .flash {
      border-left: 4px solid var(--accent);
      background: #eefaf8;
      padding: 10px 12px;
      border-radius: 10px;
      margin: 8px 0;
    }
        .toolbar {
      display: grid;
            grid-template-columns: 1.4fr 1.2fr 0.8fr 0.8fr 0.8fr 0.6fr;
      gap: 14px;
      align-items: end;
    }
    .editor-grid {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 16px;
    }
    .field { display: flex; flex-direction: column; gap: 6px; }
        .field input, .field textarea, .field select {
      width: 100%; box-sizing: border-box;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 12px;
      background: white;
      color: var(--text);
    }
    .field textarea {
      min-height: 280px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      line-height: 1.4;
      white-space: pre;
    }
    button {
      appearance: none;
      border: 0;
      border-radius: 999px;
      padding: 10px 16px;
      font-weight: 700;
      cursor: pointer;
    }
    .primary { background: var(--accent); color: white; }
    .secondary { background: var(--accent-2); color: white; }
    .muted { background: #dce6f4; color: #1f3354; }
    .buttons { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 12px; }
    .meta, .status { display: grid; gap: 8px; }
    .meta { color: var(--muted); margin-bottom: 12px; }
    .meta code, .status code { background: #eef2f7; padding: 2px 6px; border-radius: 6px; }
    .help { color: var(--muted); margin-top: 10px; line-height: 1.5; }
    .plot { min-height: 500px; }
        .plot-actions {
            margin-top: 14px;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        .plot-actions form {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            align-items: center;
        }
        .plot-actions .selection-summary {
            color: var(--muted);
            font-size: 0.95rem;
        }
        .plot-actions button:disabled {
            cursor: not-allowed;
            opacity: 0.55;
        }
    @media (max-width: 1180px) {
      .toolbar { grid-template-columns: 1fr; }
      .editor-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>PTB110 QC Tool</h1>
    <p>Load data across a date range, edit a combined correction view, and save monthly multi-day <code>.corr</code> files to <code>corrections/YYYY/YYYYMM.corr</code>.</p>
    </div>

    {% for message in flashes %}
      <div class="flash">{{ message }}</div>
    {% endfor %}

    <form method="get" class="card">
      <div class="toolbar">
        <div class="field">
          <label for="input_dir">NetCDF input directory</label>
          <input id="input_dir" name="input_dir" value="{{ input_dir }}" placeholder="./" />
        </div>
        <div class="field">
          <label for="corrections_base">Corrections base directory</label>
          <input id="corrections_base" name="corrections_base" value="{{ corrections_base }}" placeholder="./corrections" />
        </div>
        <div class="field">
          <label for="start_date">Start date</label>
          <input id="start_date" name="start_date" type="date" value="{{ start_date }}" />
        </div>
        <div class="field">
          <label for="end_date">End date</label>
          <input id="end_date" name="end_date" type="date" value="{{ end_date }}" />
        </div>
                <div class="field">
                    <label for="plot_style">Plot style</label>
                    <select id="plot_style" name="plot_style">
                        <option value="lines" {% if plot_style == "lines" %}selected{% endif %}>Lines</option>
                        <option value="points" {% if plot_style == "points" %}selected{% endif %}>Small points</option>
                        <option value="lines+points" {% if plot_style == "lines+points" %}selected{% endif %}>Lines + points</option>
                    </select>
                </div>
        <div class="field">
          <button class="primary" type="submit">Load range</button>
        </div>
      </div>
    </form>

    <form method="post">
      <input type="hidden" name="input_dir" value="{{ input_dir }}" />
      <input type="hidden" name="corrections_base" value="{{ corrections_base }}" />
      <input type="hidden" name="start_date" value="{{ start_date }}" />
      <input type="hidden" name="end_date" value="{{ end_date }}" />
    <input type="hidden" name="plot_style" value="{{ plot_style }}" />

      <div class="editor-grid">
        <div class="card">
          <h2>Correction Editor</h2>
          <div class="field">
            <label for="corr_text">Combined .corr view (global indices across loaded range)</label>
            <textarea id="corr_text" name="corr_text" spellcheck="false">{{ corr_text }}</textarea>
          </div>
          <div class="buttons">
            <button class="secondary" name="action" value="preview" type="submit">Preview</button>
            <button class="muted" name="action" value="flag_all_good" type="submit">Flag all as good</button>
            <button class="primary" name="action" value="save" type="submit">Save monthly .corr files</button>
          </div>
          <div class="help">
            Format: <code>start_idx,end_idx</code> or <code>start_idx,end_idx,flag</code>. Blank lines and <code>#</code> comments are ignored. Missing flags default to <code>2</code>.
                        Indices in this editor are global across the loaded range; when saving, ranges are automatically split and rebased to local per-day indices in monthly multi-day <code>.corr</code> files with <code>YYYYMMDD</code> as the first column.
          </div>
        </div>

        <div class="card">
          <h2>Status</h2>
          <div class="meta">
            <div>Loaded files: <code>{{ loaded_days }}</code> day(s)</div>
            <div>Samples: <code>{{ num_points }}</code></div>
            <div>Date range: <code>{{ start_date }}</code> to <code>{{ end_date }}</code></div>
          </div>
          <div class="status">
            <div>QC counts: good_data=<strong>{{ good_count }}</strong></div>
            <div>QC counts: bad_data=<strong>{{ bad_count }}</strong></div>
            <div>QC counts: not_used=<strong>{{ not_used_count }}</strong></div>
            <div>{{ qc_mode_text }}</div>
          </div>
        </div>
      </div>
    </form>

                <div class="card plot">
      <h2>Preview</h2>
      {{ plot_html|safe }}
    </div>
  </div>
</body>
</html>
"""


def _iter_days(start_day, end_day):
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def _find_nc_file_for_date(input_dir, day):
    search_dir = Path(input_dir) / f"{day.year:04d}"
    if not search_dir.exists():
        search_dir = Path(input_dir)
    candidates = sorted(search_dir.glob(f"*{day.strftime('%Y%m%d')}*.nc"))
    return candidates[0] if candidates else None


def _read_corr_as_text(corr_file):
    if not corr_file.exists():
        return ""
    return corr_file.read_text(encoding="utf-8")


def _parse_corr_text(corr_text, num_points):
    intervals = []
    for line_number, raw_line in enumerate(corr_text.splitlines(), start=1):
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split(",") if part.strip()]
        if len(parts) not in (2, 3):
            raise ValueError(f"Invalid .corr line {line_number}: expected start_idx,end_idx[,flag]")
        start_idx = int(parts[0])
        end_idx = int(parts[1])
        flag = int(parts[2]) if len(parts) == 3 else 2
        if start_idx < 0 or end_idx < 0:
            raise ValueError(f"Invalid .corr line {line_number}: indices must be >= 0")
        if start_idx > end_idx:
            raise ValueError(f"Invalid .corr line {line_number}: start_idx must be <= end_idx")
        if end_idx >= num_points:
            raise ValueError(f"Invalid .corr line {line_number}: end_idx must be <= {num_points - 1}")
        intervals.append((start_idx, end_idx, flag))
    return intervals


def _format_intervals_text(intervals):
    lines = ["# start_idx,end_idx[,flag]"]
    for start_idx, end_idx, flag in intervals:
        lines.append(f"{start_idx},{end_idx},{flag}")
    return "\n".join(lines) + "\n"


def _build_qc_preview(num_points, intervals, promote_unset=False):
    qc = np.zeros(num_points, dtype=np.int8)
    for start_idx, end_idx, flag in intervals:
        qc[start_idx:end_idx + 1] = np.int8(flag)
    if promote_unset:
        qc[qc == 0] = 1
    return qc


def _split_full_day_good_interval(selected_start_idx, selected_end_idx, num_points):
    intervals = []
    if selected_start_idx > 0:
        intervals.append((0, selected_start_idx - 1, 1))
    intervals.append((selected_start_idx, selected_end_idx, 2))
    if selected_end_idx < num_points - 1:
        intervals.append((selected_end_idx + 1, num_points - 1, 1))
    return intervals


def _merge_adjacent_intervals(intervals):
    if not intervals:
        return []

    merged = [intervals[0]]
    for start_idx, end_idx, flag in intervals[1:]:
        prev_start, prev_end, prev_flag = merged[-1]
        if flag == prev_flag and start_idx <= prev_end + 1:
            merged[-1] = (prev_start, max(prev_end, end_idx), flag)
        else:
            merged.append((start_idx, end_idx, flag))
    return merged


def _apply_selected_bad_interval(intervals, selected_start_idx, selected_end_idx):
    return _apply_selected_interval(intervals, selected_start_idx, selected_end_idx, 2)


def _apply_selected_interval(intervals, selected_start_idx, selected_end_idx, selected_flag):
    updated = []
    for start_idx, end_idx, flag in intervals:
        if end_idx < selected_start_idx or start_idx > selected_end_idx:
            updated.append((start_idx, end_idx, flag))
            continue

        if start_idx < selected_start_idx:
            updated.append((start_idx, selected_start_idx - 1, flag))
        if end_idx > selected_end_idx:
            updated.append((selected_end_idx + 1, end_idx, flag))

    updated.append((selected_start_idx, selected_end_idx, selected_flag))
    updated.sort(key=lambda interval: (interval[0], interval[1], interval[2]))
    return _merge_adjacent_intervals(updated)


def _parse_selection_bounds(selected_start_idx, selected_end_idx, num_points):
    if selected_start_idx is None or selected_end_idx is None:
        return None

    start_idx = int(selected_start_idx)
    end_idx = int(selected_end_idx)
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx
    if start_idx < 0 or end_idx < 0:
        raise ValueError("Selected indices must be >= 0")
    if end_idx >= num_points:
        raise ValueError(f"Selected end index must be <= {num_points - 1}")

    return start_idx, end_idx


def _build_selection_controls(num_points):
    if num_points <= 0:
        return ""

    return """
<div class="plot-actions">
    <div class="selection-summary" id="selection-summary">Use the plot toolbar or drag a zoom box, then turn on Select data to box- or click-select samples.</div>
  <form method="post" id="selection-form">
    <input type="hidden" name="input_dir" id="selection-input-dir" />
    <input type="hidden" name="corrections_base" id="selection-corrections-base" />
    <input type="hidden" name="start_date" id="selection-start-date" />
    <input type="hidden" name="end_date" id="selection-end-date" />
    <input type="hidden" name="plot_style" id="selection-plot-style" />
    <input type="hidden" name="corr_text" id="selection-corr-text" />
    <input type="hidden" name="action" id="selection-action" value="" />
    <input type="hidden" name="selected_start_idx" id="selection-start-idx" />
    <input type="hidden" name="selected_end_idx" id="selection-end-idx" />
    <button class="secondary" type="button" id="selection-mode-toggle">Select data</button>
    <button class="primary" type="button" id="selection-flag-bad" disabled>Flag as bad</button>
    <button class="secondary" type="button" id="selection-flag-good" disabled>Flag as good</button>
    <button class="muted" type="button" id="selection-clear-flags" disabled>Clear flags</button>
    <button class="muted" type="button" id="selection-clear-selection">Clear selection</button>
  </form>
</div>
"""


def _build_plot(
    time_vals,
    pressure_vals,
    intervals,
    title,
    plot_style="lines",
    use_index_xaxis=False,
    qc_values=None,
):
    mode_map = {
        "lines": "lines",
        "points": "markers",
        "lines+points": "lines+markers",
    }
    plotly_mode = mode_map.get(plot_style, "lines")

    qc_array = None if qc_values is None else np.asarray(qc_values, dtype=np.int8)

    figure = go.Figure()
    if qc_array is None or len(qc_array) != len(time_vals):
        figure.add_trace(
            go.Scatter(
                x=time_vals,
                y=pressure_vals,
                mode=plotly_mode,
                name="air_pressure",
                line={"color": "#1f2937", "width": 1.1},
                marker={"size": 2, "color": "#1f2937", "symbol": "circle"},
            )
        )
    else:
        point_color_map = {
            0: "#9ca3af",
            1: "#16a34a",
            2: "#dc2626",
        }
        point_name_map = {
            0: "Unflagged",
            1: "Good data",
            2: "Bad data",
        }

        if plot_style in ("lines", "lines+points"):
            figure.add_trace(
                go.Scatter(
                    x=time_vals,
                    y=pressure_vals,
                    mode="lines",
                    name="air_pressure",
                    line={"color": "#1f2937", "width": 1.1},
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

        for flag_value in (0, 1, 2):
            mask = qc_array == flag_value
            if not np.any(mask):
                continue
            if plot_style == "lines":
                point_mode = "markers"
                point_size = 5
            elif plot_style == "points":
                point_mode = "markers"
                point_size = 6
            else:
                point_mode = "markers"
                point_size = 5

            figure.add_trace(
                go.Scatter(
                    x=time_vals[mask],
                    y=pressure_vals[mask],
                    mode=point_mode,
                    name=point_name_map[flag_value],
                    marker={"size": point_size, "color": point_color_map[flag_value], "symbol": "circle"},
                    line={"color": point_color_map[flag_value], "width": 0},
                )
            )

    for start_idx, end_idx, flag in intervals:
        if flag == 1:
            color = "rgba(34, 197, 94, 0.15)"
        elif flag == 2:
            color = "rgba(239, 68, 68, 0.22)"
        else:
            color = "rgba(59, 130, 246, 0.16)"
        figure.add_vrect(
            x0=time_vals[start_idx],
            x1=time_vals[end_idx],
            fillcolor=color,
            line_width=0,
            annotation_text=str(flag),
            annotation_position="top left",
        )

    figure.update_layout(
        title=title,
        xaxis_title="Sample index" if use_index_xaxis else "Time",
        yaxis_title="Pressure (hPa)",
        height=520,
        margin={"l": 40, "r": 20, "t": 50, "b": 40},
        template="plotly_white",
        legend={"orientation": "h"},
        dragmode="zoom",
        clickmode="event",
    )

    post_script = """
const plotDiv = document.getElementById('__PLOT_ID__');
const selectionSummary = document.getElementById('selection-summary');
const selectionModeToggle = document.getElementById('selection-mode-toggle');
const selectionFlagBad = document.getElementById('selection-flag-bad');
const selectionFlagGood = document.getElementById('selection-flag-good');
const selectionClearFlags = document.getElementById('selection-clear-flags');
const selectionClearSelection = document.getElementById('selection-clear-selection');
const selectionForm = document.getElementById('selection-form');
const selectionAction = document.getElementById('selection-action');
const selectionStart = document.getElementById('selection-start-idx');
const selectionEnd = document.getElementById('selection-end-idx');
const selectionCorrText = document.getElementById('selection-corr-text');
const selectionInputDir = document.getElementById('selection-input-dir');
const selectionCorrectionsBase = document.getElementById('selection-corrections-base');
const selectionStartDate = document.getElementById('selection-start-date');
const selectionEndDate = document.getElementById('selection-end-date');
const selectionPlotStyle = document.getElementById('selection-plot-style');
const sourceCorrText = document.getElementById('corr_text');
const sourceInputDir = document.getElementById('input_dir');
const sourceCorrectionsBase = document.getElementById('corrections_base');
const sourceStartDate = document.getElementById('start_date');
const sourceEndDate = document.getElementById('end_date');
const sourcePlotStyle = document.getElementById('plot_style');
let selectionModeEnabled = false;
let selectionReady = false;

function updateSelectionButtons() {
    const enabled = selectionModeEnabled && selectionReady;
    if (selectionFlagBad) {
        selectionFlagBad.disabled = !enabled;
    }
    if (selectionFlagGood) {
        selectionFlagGood.disabled = !enabled;
    }
    if (selectionClearFlags) {
        selectionClearFlags.disabled = !enabled;
    }
}

function updatePlotInteraction() {
    if (!plotDiv || typeof Plotly === 'undefined') {
        return;
    }

    Plotly.relayout(plotDiv, {
        dragmode: selectionModeEnabled ? 'select' : 'zoom',
        clickmode: selectionModeEnabled ? 'event+select' : 'event',
        selectdirection: 'h',
    });

    if (selectionModeToggle) {
        selectionModeToggle.textContent = selectionModeEnabled ? 'Stop selecting' : 'Select data';
    }
}

function syncFormFields() {
    if (sourceCorrText) {
        selectionCorrText.value = sourceCorrText.value;
    }
    if (sourceInputDir) {
        selectionInputDir.value = sourceInputDir.value;
    }
    if (sourceCorrectionsBase) {
        selectionCorrectionsBase.value = sourceCorrectionsBase.value;
    }
    if (sourceStartDate) {
        selectionStartDate.value = sourceStartDate.value;
    }
    if (sourceEndDate) {
        selectionEndDate.value = sourceEndDate.value;
    }
    if (sourcePlotStyle) {
        selectionPlotStyle.value = sourcePlotStyle.value;
    }
}

function submitSelection(actionValue) {
    if (!selectionReady || !selectionAction || !selectionForm) {
        return;
    }

    selectionAction.value = actionValue;
    syncFormFields();
    selectionForm.submit();
}

function clearSelection(message) {
    selectionStart.value = '';
    selectionEnd.value = '';
    selectionReady = false;
    updateSelectionButtons();
    selectionSummary.textContent = message || 'Zoom mode is on. Turn on Select data to select samples.';
}

if (selectionModeToggle) {
    selectionModeToggle.addEventListener('click', () => {
        selectionModeEnabled = !selectionModeEnabled;
        clearSelection(selectionModeEnabled ? 'Select data mode is on. Box-select or click a sample.' : 'Zoom mode is on. Turn on Select data to select samples.');
        updatePlotInteraction();
    });
}

if (selectionClearSelection) {
    selectionClearSelection.addEventListener('click', () => {
        clearSelection(selectionModeEnabled ? 'Select data mode is on. Box-select or click a sample.' : 'Zoom mode is on. Turn on Select data to select samples.');
    });
}

if (selectionFlagBad) {
    selectionFlagBad.addEventListener('click', () => submitSelection('flag_selected_bad'));
}

if (selectionFlagGood) {
    selectionFlagGood.addEventListener('click', () => submitSelection('flag_selected_good'));
}

if (selectionClearFlags) {
    selectionClearFlags.addEventListener('click', () => submitSelection('flag_selected_clear'));
}

if (plotDiv) {
    updatePlotInteraction();
    updateSelectionButtons();

    plotDiv.on('plotly_selected', (eventData) => {
        if (!selectionModeEnabled) {
            return;
        }

        const points = eventData && eventData.points ? eventData.points : [];
        if (!points.length) {
            clearSelection();
            return;
        }

        const pointIndices = points
            .map((point) => point.pointIndex)
            .filter((pointIndex) => Number.isInteger(pointIndex));
        if (!pointIndices.length) {
            clearSelection();
            return;
        }

        const startIdx = Math.min(...pointIndices);
        const endIdx = Math.max(...pointIndices);
        selectionStart.value = String(startIdx);
        selectionEnd.value = String(endIdx);
        selectionReady = true;
        updateSelectionButtons();
        selectionSummary.textContent = `Selected samples ${startIdx} to ${endIdx} (${endIdx - startIdx + 1} point${endIdx === startIdx ? '' : 's'})`;
    });

    plotDiv.on('plotly_click', (eventData) => {
        if (!selectionModeEnabled) {
            return;
        }

        const point = eventData && eventData.points && eventData.points.length ? eventData.points[0] : null;
        if (!point || !Number.isInteger(point.pointIndex)) {
            return;
        }

        const selectedIndex = point.pointIndex;
        selectionStart.value = String(selectedIndex);
        selectionEnd.value = String(selectedIndex);
        selectionReady = true;
        updateSelectionButtons();
        selectionSummary.textContent = `Selected sample ${selectedIndex}`;
    });

    plotDiv.on('plotly_deselect', () => clearSelection());
    clearSelection();
}
""".replace("__PLOT_ID__", "qc-preview-plot")

    plot_html = pio.to_html(
        figure,
        include_plotlyjs="cdn",
        full_html=False,
        config={
            "responsive": True,
            "displayModeBar": True,
            "modeBarButtonsToAdd": ["select2d", "lasso2d"],
        },
        div_id="qc-preview-plot",
        post_script=post_script,
        )
    return _build_selection_controls(len(time_vals)) + plot_html


def _load_range_bundle(input_dir, corrections_base, start_day, end_day):
    segments = []
    time_parts = []
    pressure_parts = []
    qc_parts = []
    missing_days = []
    load_errors = []
    use_index_xaxis = False
    global_start = 0

    for day in _iter_days(start_day, end_day):
        nc_file = _find_nc_file_for_date(input_dir, day)
        if nc_file is None:
            missing_days.append(day)
            continue

        try:
            with xr.open_dataset(nc_file) as ds:
                if "time" not in ds or "air_pressure" not in ds:
                    missing_days.append(day)
                    continue

                pressure_vals = np.asarray(ds["air_pressure"].values)
                num_points = len(pressure_vals)
                if num_points == 0:
                    missing_days.append(day)
                    continue

                if "qc_flag_air_pressure" in ds:
                    qc_vals = np.asarray(ds["qc_flag_air_pressure"].values)
                    if len(qc_vals) == num_points:
                        qc_parts.append(qc_vals)

                try:
                    time_vals = pd.to_datetime(ds["time"].values)
                except Exception:
                    # Fall back to sample index axis for problematic time encodings.
                    use_index_xaxis = True
                    time_vals = np.arange(global_start, global_start + num_points)
        except Exception as exc:
            load_errors.append((day, str(exc)))
            continue

        time_parts.append(np.asarray(time_vals))
        pressure_parts.append(pressure_vals)

        segments.append(
            {
                "date": day,
                "num_points": num_points,
                "start_idx": global_start,
                "end_idx": global_start + num_points - 1,
                "corr_file": build_daily_correction_path(corrections_base, day),
                "corr_month_file": build_monthly_correction_path(corrections_base, day),
                "corrections_base": corrections_base,
            }
        )
        global_start += num_points

    if time_parts:
        time_vals = np.concatenate(time_parts)
        pressure_vals = np.concatenate(pressure_parts)
    else:
        time_vals = np.array([])
        pressure_vals = np.array([])

    qc_vals = np.concatenate(qc_parts) if qc_parts and len(qc_parts) == len(segments) else None

    return {
        "segments": segments,
        "time_vals": time_vals,
        "pressure_vals": pressure_vals,
        "qc_vals": qc_vals,
        "missing_days": missing_days,
        "load_errors": load_errors,
        "use_index_xaxis": use_index_xaxis,
        "num_points": global_start,
    }


def _combined_intervals_from_existing(segments):
    intervals = []
    month_groups = {}
    for segment in segments:
        month_key = segment["date"].strftime("%Y%m")
        month_groups.setdefault(month_key, []).append(segment)

    for month_segments in month_groups.values():
        month_file = month_segments[0]["corr_month_file"]
        if month_file.exists():
            month_intervals = _read_monthly_corr_file_by_day(month_file)
            for segment in month_segments:
                for start_idx, end_idx, flag in month_intervals.get(segment["date"].date(), []):
                    if start_idx < 0 or end_idx >= segment["num_points"] or start_idx > end_idx:
                        continue
                    intervals.append(
                        (
                            segment["start_idx"] + start_idx,
                            segment["start_idx"] + end_idx,
                            flag,
                        )
                    )
            continue

        for segment in month_segments:
            corr_file = find_correction_file_for_date(segment["corrections_base"], segment["date"])
            if corr_file is None:
                continue
            local_intervals = parse_corr_intervals_from_file(
                corr_file,
                target_date=segment["date"].date(),
                bad_flag=2,
            )
            for start_idx, end_idx, flag in local_intervals:
                if start_idx < 0 or end_idx >= segment["num_points"] or start_idx > end_idx:
                    continue
                intervals.append(
                    (
                        segment["start_idx"] + start_idx,
                        segment["start_idx"] + end_idx,
                        flag,
                    )
                )
    return intervals


def _split_intervals_by_day(intervals, segments):
    by_day = {segment["date"]: [] for segment in segments}
    for start_idx, end_idx, flag in intervals:
        for segment in segments:
            seg_start = segment["start_idx"]
            seg_end = segment["end_idx"]
            overlap_start = max(start_idx, seg_start)
            overlap_end = min(end_idx, seg_end)
            if overlap_start <= overlap_end:
                by_day[segment["date"]].append(
                    (
                        overlap_start - seg_start,
                        overlap_end - seg_start,
                        flag,
                    )
                )
    return by_day


def _read_monthly_corr_file_by_day(month_file):
    by_day = {}
    month_path = Path(month_file)
    if not month_path.exists():
        return by_day

    with month_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue

            parts = [part.strip() for part in line.split(",") if part.strip()]
            if len(parts) not in (3, 4):
                continue

            if len(parts) in (3, 4) and len(parts[0]) == 8 and parts[0].isdigit():
                day = datetime.strptime(parts[0], "%Y%m%d").date()
                offset = 1
            else:
                continue

            try:
                start_idx = int(parts[offset])
                end_idx = int(parts[offset + 1])
                flag = int(parts[offset + 2]) if len(parts) == offset + 3 else 2
            except ValueError:
                continue

            by_day.setdefault(day, []).append((start_idx, end_idx, flag))

    return by_day


if app is not None:

    @app.route("/", methods=["GET", "POST"])
    def index():
        input_dir = request.values.get("input_dir", app.config.get("QC_DEFAULT_INPUT_DIR", "./"))
        corrections_base = request.values.get(
            "corrections_base", app.config.get("QC_DEFAULT_CORRECTIONS_BASE", "./corrections")
        )
        today = datetime.utcnow().strftime("%Y-%m-%d")
        start_date_str = request.values.get("start_date", today)
        end_date_str = request.values.get("end_date", start_date_str)
        plot_style = request.values.get("plot_style", "lines")
        action = request.form.get("action")
        corr_text = request.form.get("corr_text", "")

        try:
            start_day = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_day = datetime.strptime(end_date_str, "%Y-%m-%d")
            if end_day < start_day:
                start_day, end_day = end_day, start_day
                start_date_str = start_day.strftime("%Y-%m-%d")
                end_date_str = end_day.strftime("%Y-%m-%d")
        except ValueError:
            start_day = datetime.utcnow()
            end_day = start_day
            start_date_str = end_date_str = start_day.strftime("%Y-%m-%d")
            flash("Invalid date input. Using today's date.")

        bundle = _load_range_bundle(input_dir, corrections_base, start_day, end_day)
        segments = bundle["segments"]
        num_points = bundle["num_points"]
        time_vals = bundle["time_vals"]
        pressure_vals = bundle["pressure_vals"]
        qc_vals = bundle["qc_vals"]
        use_index_xaxis = bundle["use_index_xaxis"]

        if bundle["missing_days"]:
            flash(f"Missing/invalid NetCDF files for {len(bundle['missing_days'])} day(s) in selected range.")
        if bundle["load_errors"]:
            first_day, first_error = bundle["load_errors"][0]
            flash(
                f"Failed to read {len(bundle['load_errors'])} day(s). "
                f"First error: {first_day.strftime('%Y-%m-%d')} - {first_error}"
            )

        if request.method == "GET":
            existing_intervals = _combined_intervals_from_existing(segments)
            corr_text = _format_intervals_text(existing_intervals)

        intervals = []
        if num_points and corr_text.strip():
            try:
                intervals = _parse_corr_text(corr_text, num_points)
            except Exception as exc:
                flash(f"Correction parse error: {exc}")
                intervals = []

        if request.method == "POST" and action == "flag_all_good":
            if num_points > 0:
                corr_text = _format_intervals_text([(0, num_points - 1, 1)])
                intervals = [(0, num_points - 1, 1)]
            else:
                flash("No loaded data in selected range.")

        if request.method == "POST" and action in ("flag_selected_bad", "flag_selected_good", "flag_selected_clear"):
            try:
                selected_bounds = _parse_selection_bounds(
                    request.form.get("selected_start_idx"),
                    request.form.get("selected_end_idx"),
                    num_points,
                )
                if selected_bounds is None:
                    raise ValueError("Select a span in the preview plot first.")
                selected_start_idx, selected_end_idx = selected_bounds
                current_intervals = _parse_corr_text(corr_text, num_points) if corr_text.strip() else []
                selected_flag = {
                    "flag_selected_bad": 2,
                    "flag_selected_good": 1,
                    "flag_selected_clear": 0,
                }[action]
                current_intervals = _apply_selected_interval(
                    current_intervals,
                    selected_start_idx,
                    selected_end_idx,
                    selected_flag,
                )
                corr_text = _format_intervals_text(current_intervals)
                intervals = current_intervals
                action_label = {2: "bad data", 1: "good data", 0: "clear flags"}[selected_flag]
                flash(f"Flagged samples {selected_start_idx} to {selected_end_idx} as {action_label} in the editor.")
            except Exception as exc:
                flash(f"Selection flagging failed: {exc}")

        if request.method == "POST" and action == "save":
            try:
                if not segments:
                    raise ValueError("No loaded NetCDF days in selected range.")
                intervals = _parse_corr_text(corr_text, num_points) if corr_text.strip() else []
                split = _split_intervals_by_day(intervals, segments)
                month_groups = {}
                for segment in segments:
                    month_key = segment["date"].strftime("%Y%m")
                    month_groups.setdefault(month_key, []).append(segment)

                saved_files = 0
                for month_segments in month_groups.values():
                    month_file = month_segments[0]["corr_month_file"]
                    month_file.parent.mkdir(parents=True, exist_ok=True)
                    merged_by_day = _read_monthly_corr_file_by_day(month_file)
                    has_current_intervals = False
                    for day_segment in month_segments:
                        day = day_segment["date"].date()
                        day_intervals = split.get(day_segment["date"], [])
                        if day_intervals:
                            has_current_intervals = True
                        merged_by_day[day] = day_intervals

                    if not has_current_intervals:
                        flash(f"No intervals to save for {month_file.name}; existing file left unchanged.")
                        continue

                    with month_file.open("w", encoding="utf-8") as handle:
                        handle.write("# YYYYMMDD,start_idx,end_idx[,flag]\n")
                        for day in sorted(merged_by_day):
                            day_tag = day.strftime("%Y%m%d")
                            for start_idx, end_idx, flag in merged_by_day.get(day, []):
                                handle.write(f"{day_tag},{start_idx},{end_idx},{flag}\n")
                    saved_files += 1

                flash(
                    f"Saved {saved_files} monthly multi-day .corr file(s) "
                    f"covering {len(segments)} day(s)."
                )
                return redirect(
                    url_for(
                        "index",
                        input_dir=input_dir,
                        corrections_base=corrections_base,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        plot_style=plot_style,
                    )
                )
            except Exception as exc:
                flash(f"Save failed: {exc}")
        if num_points and qc_vals is not None:
            qc_preview = np.asarray(qc_vals, dtype=np.int8).copy()
            for start_idx, end_idx, flag in intervals:
                qc_preview[start_idx:end_idx + 1] = np.int8(flag)
            qc_mode_text = "Loaded QC counts were read from the NetCDF file and updated from the editor."
        else:
            qc_preview = _build_qc_preview(num_points, intervals, promote_unset=bool(segments)) if num_points else np.array([])
            qc_mode_text = "Range review mode: when saved, loaded days are written into monthly multi-day .corr files."

        good_count = int(np.sum(qc_preview == 1)) if num_points else 0
        bad_count = int(np.sum(qc_preview == 2)) if num_points else 0
        not_used_count = int(np.sum(qc_preview == 0)) if num_points else 0

        if num_points:
            plot_html = _build_plot(
                time_vals,
                pressure_vals,
                intervals,
                f"air_pressure with QC intervals ({start_date_str} to {end_date_str})",
                plot_style=plot_style,
                use_index_xaxis=use_index_xaxis,
                qc_values=qc_preview,
            )
        else:
            plot_html = "<p>No plot available for selected range.</p>"

        return render_template_string(
            PAGE_TEMPLATE,
            flashes=list(get_flashed_messages()),
            input_dir=input_dir,
            corrections_base=corrections_base,
            start_date=start_date_str,
            end_date=end_date_str,
            plot_style=plot_style,
            corr_text=corr_text,
            loaded_days=len(segments),
            num_points=num_points,
            good_count=good_count,
            bad_count=bad_count,
            not_used_count=not_used_count,
            qc_mode_text=qc_mode_text,
            plot_html=plot_html,
        )


def run_app(host="127.0.0.1", port=8501, debug=False):
    if app is None:
        raise SystemExit(
            "Flask and Plotly are required for the web QC tool. Install with: "
            "pip install 'chilbolton-pressure-utils[qcweb]'"
        )
    app.run(host=host, port=port, debug=debug)


def main():
    parser = argparse.ArgumentParser(description="Launch PTB110 Flask web QC tool")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind")
    parser.add_argument("--port", type=int, default=8501, help="Port for the Flask server")
    parser.add_argument(
        "--input-dir",
        default="./",
        help="Default NetCDF input directory shown in the web app",
    )
    parser.add_argument(
        "--corrections-base",
        default="./corrections",
        help="Default corrections base directory shown in the web app",
    )
    parser.add_argument("--debug", action="store_true", help="Run Flask in debug mode")
    args = parser.parse_args()

    if app is not None:
        app.config["QC_DEFAULT_INPUT_DIR"] = args.input_dir
        app.config["QC_DEFAULT_CORRECTIONS_BASE"] = args.corrections_base

    run_app(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
