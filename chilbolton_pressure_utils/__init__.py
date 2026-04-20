"""
Chilbolton Pressure Utils
==========================
Processing utilities for Chilbolton barometric pressure sensor data.

Provides tools to convert raw Campbell Scientific CR1000X datalogger files
and legacy Format5 binary files containing Vaisala PTB110 pressure data into
CF-compliant NetCDF files, with quality control flagging and quicklook plots.
"""
__version__ = "1.0.0"
__author__ = "Chris Walden"

from .process_ptb110 import main as process_ptb110_main
from .process_ptb110_f5 import main as process_ptb110_f5_main
from .process_ptb110_stfc import main as process_ptb110_stfc_main
from .read_format5_header import read_format5_header
from .read_format5_content import read_format5_content
from .split_cr1000x_data_daily import main as split_cr1000x_data_daily_main
from .make_quicklooks import main as make_quicklooks_main
from .read_format5_chdb import read_format5_chdb

__all__ = [
    "process_ptb110_main",
    "process_ptb110_f5_main",
    "process_ptb110_stfc_main",
    "read_format5_header",
    "read_format5_content",
    "split_cr1000x_data_daily_main",
    "make_quicklooks_main",
    "read_format5_chdb",
]
