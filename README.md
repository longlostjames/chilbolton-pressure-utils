# chilbolton-pressure-utils

Processing utilities for Chilbolton barometric pressure sensor data.

## Overview

This package provides tools for processing barometric pressure data from the
Vaisala PTB110 sensor at Chilbolton Atmospheric Observatory (CAO), logged by a
Campbell Scientific CR1000X datalogger. It supports three data pipelines:

- **Format5** (legacy binary, pre-2020): `process-ptb110-f5`
- **CR1000X NCAS** (2020 onwards): `process-ptb110`
- **CR1000X STFC** (2024 onwards): `process-ptb110-stfc`

Output files follow CF-compliant NetCDF conventions using the
[NCAS AMoF NetCDF Template](https://github.com/ncasuk/ncas-amof-netcdf-template).

## Installation

See [INSTALL.md](INSTALL.md) for full installation instructions.

```bash
pip install chilbolton-pressure-utils
```

## Quick Start

### Process a single day (CR1000X format)

```bash
process-ptb110 CR1000XSeries_Chilbolton_Rxcabinmet1_20240115.dat \
    -o /output/dir/ -m metadata.json
```

### Process a full year

```bash
process-ptb110-year -y 2024 \
    --raw-data-base /path/to/raw/data \
    --output-base /path/to/output \
    --corrections-base /path/to/corrections
```

### Process legacy Format5 data

```bash
process-ptb110-f5 chan240115.000 -o /output/dir/ -m metadata_f5.json
process-ptb110-year-f5 -y 2018 \
    --raw-data-base /path/to/format5/data \
    --output-base /path/to/output
```

### Generate quicklook plots

```bash
make-ptb110-quicklooks -y 2024 -i /path/to/netcdf/ -o /path/to/plots/
```

### Bad data management

```bash
# Extract bad data indices to CSV for review
extract-ptb110-bad-data-indices -i /path/to/netcdf/ -o bad_data_indices_2024.csv -y 2024

# Apply corrected bad data indices back to NetCDF files
apply-ptb110-bad-data-indices -c bad_data_indices_2024.csv -i /path/to/netcdf/ -y 2024
```

### Apply daily .corr files to existing NetCDF files

```bash
apply-ptb110-corr-files -i /path/to/netcdf/ -c /path/to/corrections -y 2024
```

Correction files should be stored as:

```text
corrections/
    2024/
        20240115.corr
        20240116.corr
```

Each `.corr` file supports one interval per line in this format:

```text
start_idx,end_idx
start_idx,end_idx,flag
```

- Blank lines are ignored.
- Anything after `#` on a line is treated as a comment.
- If `flag` is omitted, `2` is used.
- Processing starts with QC = `0` before `.corr` application; when a `.corr` file is present, all remaining `0` values are promoted to `1` (good data), and only explicitly flagged ranges remain non-good.

## Data Paths (JASMIN)

Default data paths assume the NCAS GWS on JASMIN:

| Data stream | Raw data | Output |
|---|---|---|
| NCAS CR1000X | `.../new_daily_split/{YYYY}/{YYYYMM}/CR1000XSeries_Chilbolton_Rxcabinmet1_{YYYYMMDD}.dat` | `.../ncas-pressure-1/data/long-term/level1a/{YYYY}/` |
| Format5 | `.../format5/chan{YYMMDD}.000` | `.../ncas-pressure-1/data/long-term/level1_f5/{YYYY}/` |
| STFC CR1000X | `.../long-term/{YYYY}/{YYYYMM}/CR1000XSeries_Chilbolton_Rxcabinmet1_{YYYYMMDD}.dat` | `.../ncas-pressure-1/data/long-term/level1/{YYYY}/` |

## NetCDF Output

Files are named following the NCAS convention:

```
ncas-pressure-1_cao_{YYYYMMDD}_surface-met_v1.1.nc
```

Key variables:
- `air_pressure` — barometric pressure (hPa)
- `qc_flag_air_pressure` — QC flag (`0` = not used, `1` = good data, `2` = bad data)

## Notes on Format5 Channel

The Format5 channel name for pressure in the `f5channelDB.chdb` database is
assumed to be `baro_ch`. Verify this against the actual channel database before
processing Format5 data; update `process_ptb110_f5.py` if needed.

## Author

Chris Walden (chris.walden@ncas.ac.uk)

## License

See [LICENSE](LICENSE).
