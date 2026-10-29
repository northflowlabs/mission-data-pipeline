# Examples

Both scripts generate their own synthetic input and need no external files. Run them from the project root.

| Script | What it does |
|---|---|
| [`examples/01_binary_ingest.py`](https://github.com/northflowlabs/mission-data-pipeline/blob/main/examples/01_binary_ingest.py) | Writes synthetic housekeeping packets for APID 0x100, decommutates three parameters, applies polynomial calibrations and writes one Parquet file per parameter |
| [`examples/02_csv_to_hdf5.py`](https://github.com/northflowlabs/mission-data-pipeline/blob/main/examples/02_csv_to_hdf5.py) | Reads engineering telemetry from CSV, calibrates it and writes HDF5 |

```bash
python -m examples.01_binary_ingest
python -m examples.02_csv_to_hdf5
```
