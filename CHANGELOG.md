# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-04-20

### Added
- Initial release based on chilbolton-temperature-rh-utils v1.0.0
- Processing of Vaisala PTB110 barometric pressure data from Campbell Scientific
  CR1000X datalogger files (`BP_mbar_Avg` column, direct hPa values)
- Legacy Format5 binary file support (`baro_ch` channel — verify channel name
  against the site channel database)
- STFC variant for data collected under STFC affiliation
- Bad data indices management via `extract-ptb110-bad-data-indices` and
  `apply-ptb110-bad-data-indices`
- Batch year and month processing via `process-ptb110-year[-f5|-stfc]`
  and `process-ptb110-month[-f5|-stfc]`
- Quicklook plot generation via `make-ptb110-quicklooks`
- CF-compliant NetCDF output using NCAS AMoF NetCDF Template
  (`ncas-pressure-1`, product `surface-met`)
