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
mdp inspect telemetry.bin --max-packets 50 --apid 256
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
  --transformer apid_filter \
  --loader parquet \
  --loader-config loader.json \
  --pipeline-name hk-ingest
```

| Option | Effect |
|---|---|
| `--extractor` | Extractor name, required |
| `--extractor-config` | JSON file with the extractor config |
| `--transformer` | Transformer name. Repeatable, applied in order |
| `--loader` | Loader name |
| `--loader-config` | JSON file with the loader config |
| `--pipeline-name` | Run label, default `mdp-run` |
| `--dry-run` | Extract and transform only, write nothing |
| `--max-batches` | Stop after this many batches |

The JSON files hold the fields of the stage config model, for example `{"path": "telemetry.bin", "sec_hdr_length": 4}` for `binary` and `{"output_dir": "output"}` for `parquet`.

Transformers are built with their default config. `decom` and `calibration` need parameter and calibration definitions, so use them from Python, as in the [quick start](../getting-started/quickstart.md).

The command exits with status 1 when the run fails.
