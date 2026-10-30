# Plugins

Built-in stages register themselves by name in `mdp.core.registry.registry` when `mdp.plugins` is imported. The `mdp` command imports them for you.

## Built-in stages

| Kind | Name | Class | Config |
|---|---|---|---|
| Extractor | `binary` | `BinaryPacketExtractor` | `BinaryExtractorConfig` |
| Extractor | `csv` | `CsvTelemetryExtractor` | `CsvExtractorConfig` |
| Transformer | `decom` | `DecomTransformer` | `DecomConfig` |
| Transformer | `apid_filter` | `ApidFilterTransformer` | `ApidFilterConfig` |
| Transformer | `calibration` | `CalibrationTransformer` | `CalibrationConfig` |
| Loader | `parquet` | `ParquetLoader` | `ParquetLoaderConfig` |
| Loader | `hdf5` | `HDF5Loader` | `HDF5LoaderConfig` |
| Loader | `csv` | `CsvLoader` | `CsvLoaderConfig` |

### Extractors

- `binary` reads a raw CCSDS packet stream. Options: `path`, `batch_size` (256), `apid_filter`, `sec_hdr_length` (0), `frame_sync` (scan for the 0x1ACFFC1D marker), `source_id`, `ground_receipt_time`. With `frame_sync`, a frame runs from one marker to the next and may hold several packets. A marker inside a packet is accepted as payload only if the packet ends at another marker or the end of the file; otherwise the frame is skipped and counted. Bytes before the first marker count in `lead_in_bytes`, a file with no marker fails, and fill and idle packets count in `fill_bytes` and `idle_packets`.
- `csv` reads engineering values from CSV. Options: `path`, `batch_size` (1000), `time_column`, `apid_column`, `seq_count_column`, `parameter_columns`, `parameter_types`, `delimiter`, `source_id`. `parameter_types` maps a column to a `ParameterType`, such as `{"mode": "string", "counter": "uint"}`; cells are read as text and converted by it, and an undeclared cell written as a plain number becomes one. `source_id` goes into the dataset metadata. A row of the wrong width is counted in `wrong_width_rows`, never shifted.

### Transformers

- `decom` extracts parameters from packet user data. Each `ParameterDefinition` gives `name`, `apid`, `byte_offset`, `bit_length`, `param_type`, and optionally `bit_offset`, `unit`, `little_endian` and `description`. `bit_offset` (0 to 7, from the most significant bit) places integer and boolean fields at any bit; floats and little-endian fields must be byte-aligned.
- `apid_filter` keeps the APIDs in `include` or drops the APIDs in `exclude`. Set one, not both.
- `calibration` converts raw values to engineering units. Each `CalibrationEntry` uses `CalibrationMethod.POLYNOMIAL` with `coefficients`, `CalibrationMethod.TABLE` with `table_raw` and `table_eng`, or `CalibrationMethod.IDENTITY`.

### Loaders

- `parquet` writes to `output_dir`, Snappy compression by default, one file per parameter or one wide table (`wide_format`), optionally partitioned by APID.
- `hdf5` writes to one file at `path` with gzip compression. It can append across batches.
- `csv` writes to `output_dir`, per parameter or wide format.

## Write your own stage

Subclass `Extractor`, `Transformer` or `Loader`, set `config_class` to a Pydantic model, and register the class with a decorator.

```python
from pydantic import BaseModel

from mdp.core.base import Transformer
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset


class DropEmptyConfig(BaseModel):
    min_bytes: int = 1


@registry.transformer("drop_empty")
class DropEmptyTransformer(Transformer[DropEmptyConfig]):
    config_class = DropEmptyConfig

    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        # Keep the parameters and metadata: loss counters travel in the metadata
        out = TelemetryDataset(parameters=dataset.parameters, metadata=dataset.metadata)
        for packet in dataset.packets:
            if len(packet.user_data) >= self.config.min_bytes:
                out.add_packet(packet)
        return out
```

The required methods are `extract()` for an extractor (a generator of `TelemetryDataset`), `transform(dataset)` for a transformer and `load(dataset)` for a loader. `validate_config()`, `setup()` and `teardown()` are optional. A pipeline calls the first two once per run before the first batch, and `teardown()` once at the end, even after an error.

Look up a registered class by name with `registry.get_extractor(name)`, `registry.get_transformer(name)` or `registry.get_loader(name)`.
