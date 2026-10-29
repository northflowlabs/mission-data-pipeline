# Installation

MDP needs Python 3.10 or later. It is installed from source.

```bash
git clone https://github.com/northflowlabs/mission-data-pipeline.git
cd mission-data-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

Runtime dependencies are NumPy, pandas, PyArrow, h5py, Pydantic v2, structlog, Click and Rich. They install with the package.

## Extras

| Extra | Contents |
|---|---|
| `dev` | pytest, pytest-cov, pytest-asyncio, mypy, ruff, pre-commit |
| `docs` | mkdocs, mkdocs-material, mkdocstrings |

To build this documentation locally:

```bash
pip install -e ".[docs]"
mkdocs serve
```

## Check the install

```bash
mdp version
mdp stages
```

`mdp stages` lists the built-in extractors, transformers and loaders.
