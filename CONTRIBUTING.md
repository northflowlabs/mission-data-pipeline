# Contributing to Mission Data Pipeline

Thank you for your interest in contributing. MDP is an open-source project and welcomes contributions from mission operations engineers, data scientists, and software developers.

---

## Development Setup

**Requirements:** Python 3.10+, Git

```bash
git clone https://github.com/northflowlabs/mission-data-pipeline.git
cd mission-data-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pre-commit install
```

---

## Running Tests

```bash
pytest                            # full suite with coverage
pytest tests/test_models_packet.py -v   # single module
pytest -k "calibration" -v             # by keyword
```

---

## Code Style

This project uses **ruff** for linting and formatting, and **mypy** for strict type checking.

```bash
ruff check src/ tests/
ruff format src/ tests/
mypy src/
```

Pre-commit hooks run both automatically on every commit.

---

## Project Structure

```
src/mdp/
├── __init__.py
├── __version__.py
├── core/               # Abstract ETL base classes + pipeline engine
│   ├── base.py         #   Extractor, Transformer, Loader, StageResult
│   ├── pipeline.py     #   Pipeline orchestrator
│   └── registry.py     #   Plugin self-registration
├── models/             # Typed data models (CCSDS packets, frames, parameters)
├── observability/      # Structured logging, metrics, event hooks
├── plugins/            # Concrete stage implementations
│   ├── extractors/
│   ├── transformers/
│   └── loaders/
└── cli/                # `mdp` CLI entry point
```

---

## Adding a Plugin

### 1. Create the file

Add a new file in the appropriate `plugins/` subdirectory, e.g.  
`src/mdp/plugins/extractors/kafka.py`

### 2. Implement the stage

```python
from pydantic import BaseModel
from mdp.core.base import Extractor
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset

class KafkaExtractorConfig(BaseModel):
    topic: str
    bootstrap_servers: str

@registry.extractor("kafka")
class KafkaExtractor(Extractor[KafkaExtractorConfig]):
    config_class = KafkaExtractorConfig

    def extract(self):
        # ... your implementation
        yield TelemetryDataset()
```

### 3. Export from `plugins/__init__.py`

Add your class to `src/mdp/plugins/__init__.py`.

### 4. Write tests

Add tests in `tests/test_plugins_extractor_kafka.py`.

---

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(extractor): add Kafka packet extractor
fix(decom): handle sub-byte bit fields correctly
docs: update CCSDS reference links
test(calibration): add polynomial edge case for negative coefficients
```

---

## Pull Request Checklist

- [ ] Tests pass (`pytest`)
- [ ] Type annotations present and mypy passes
- [ ] ruff reports no issues
- [ ] Docstrings updated for public APIs
- [ ] CHANGELOG updated (if applicable)

---

## Reporting Issues

Open a GitHub Issue at https://github.com/northflowlabs/mission-data-pipeline/issues with:
- MDP version (`mdp version`)
- Python version and OS
- Minimal reproducible example
- Expected vs actual behaviour
