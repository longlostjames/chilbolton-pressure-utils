# Installation

## Prerequisites

- Python 3.11 or higher
- Conda environment recommended (e.g. `cao_3_11`)

## Install from source

```bash
git clone https://github.com/longlostjames/chilbolton-pressure-utils.git
cd chilbolton-pressure-utils
pip install -e .
```

## Install from PyPI (when available)

```bash
pip install chilbolton-pressure-utils
```

## Dependencies

The following packages are installed automatically:

- `numpy>=1.24.0`
- `polars>=0.19.0`
- `pandas>=2.0.0`
- `xarray>=2023.1.0`
- `netCDF4>=1.6.0`
- `matplotlib>=3.7.0`
- `cftime>=1.6.0`
- `ncas-amof-netcdf-template>=2.0.0`

## Verify installation

```bash
python -c "import chilbolton_pressure_utils; print(chilbolton_pressure_utils.__version__)"
process-ptb110 --help
```
