# Examples

Both scripts read real spacecraft telemetry from `examples/data`, whose README gives the sources and licences. Run them from the project root.

| Script | What it does |
|---|---|
| [`examples/01_binary_ingest.py`](https://github.com/northflowlabs/mission-data-pipeline/blob/main/examples/01_binary_ingest.py) | Reads NASA CYGNSS Level 0 packets, takes each packet's time from its secondary header, decommutates the GPS week and seconds, converts the seconds to hours of the week and writes one Parquet file per parameter |
| [`examples/02_csv_to_hdf5.py`](https://github.com/northflowlabs/mission-data-pipeline/blob/main/examples/02_csv_to_hdf5.py) | Reads ESA OPS-SAT telemetry from CSV and writes HDF5 |

```bash
python -m examples.01_binary_ingest
python -m examples.02_csv_to_hdf5
```
