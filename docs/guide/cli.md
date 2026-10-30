# CLI reference

The package installs the `mdp` command.

## Global options

| Option | Default | Values |
|---|---|---|
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--log-format` | `console` | `console`, `json` |

Global options go before the command: `mdp --log-format json run ...`.

## mdp version

Prints the installed version.

## mdp stages

Lists the registered extractors, transformers and loaders.

## mdp inspect

Reads a raw binary telemetry file and prints packet summaries.

```bash
mdp inspect examples/data/CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm --max-packets 50 --apid 1313
```

| Option | Default | Effect |
|---|---|---|
| `--max-packets` | 20 | Number of packets to show |
| `--apid` | all | Show only this APID, as a decimal integer. Repeatable |

## mdp run

Builds a pipeline from registered stage names and runs it.

```bash
mdp run \
  --extractor binary \
  --extractor-config extractor.json \
  --transformer decom \
  --transformer-config decom=decom.json \
  --loader parquet \
  --loader-config loader.json \
  --pipeline-name hk-ingest
```

| Option | Effect |
|---|---|
| `--extractor` | Extractor name, required |
| `--extractor-config` | JSON file with the extractor config |
| `--transformer` | Transformer name. Repeatable, applied in order |
| `--transformer-config` | `NAME=FILE`: JSON file with that transformer's config. Repeatable |
| `--loader` | Loader name |
| `--loader-config` | JSON file with the loader config |
| `--pipeline-name` | Run label, default `mdp-run` |
| `--dry-run` | Extract and transform only, write nothing |
| `--max-batches` | Stop after this many batches, at least 1 |
| `--strict` | Treat data losses as a failed run |

The JSON files hold the fields of each stage's config model:

```json
{"path": "examples/data/CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm", "sec_hdr_length": 10, "apid_filter": [1313]}
```

```json
{"parameters": [{"name": "gps_seconds", "apid": 1313, "byte_offset": 6, "bit_length": 64, "param_type": "double", "unit": "s", "little_endian": true}]}
```

```json
{"output_dir": "output"}
```

Binary packets carry no parameters until `decom` decodes them.

The command exits with status 1 when the run fails, and when a run gives the loader nothing to write. It exits with status 3 when the run finished but lost data, as `summary()` shows.
