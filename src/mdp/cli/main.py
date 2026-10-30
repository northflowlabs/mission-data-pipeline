"""MDP command-line interface.

Usage::

    mdp --help
    mdp run --extractor binary --loader parquet ...
    mdp inspect <file.bin>
    mdp stages
    mdp version
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from rich.table import Table

import mdp.plugins  # noqa: F401  (registers the built-in stages)
from mdp.__version__ import __version__
from mdp.observability.logging import configure_logging

console = Console()

EXIT_DATA_LOST = 3  # the run finished, but data was lost; see the summary


@click.group()
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    show_default=True,
    help="Log verbosity level.",
)
@click.option(
    "--log-format",
    default="console",
    type=click.Choice(["console", "json"], case_sensitive=False),
    show_default=True,
    help="Log output format.",
)
@click.pass_context
def cli(ctx: click.Context, log_level: str, log_format: str) -> None:
    """Mission Data Pipeline (MDP): modular ETL for space telemetry."""
    ctx.ensure_object(dict)
    configure_logging(level=log_level, fmt=log_format)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
#  mdp version                                                                 #
# --------------------------------------------------------------------------- #


@cli.command()
def version() -> None:
    """Print the MDP version."""
    console.print(f"[bold cyan]Mission Data Pipeline[/] v{__version__}")


# --------------------------------------------------------------------------- #
#  mdp stages                                                                  #
# --------------------------------------------------------------------------- #


@cli.command()
def stages() -> None:
    """List all registered pipeline stage plugins."""
    from mdp.core.registry import registry  # noqa: PLC0415

    data = registry.all_stages()

    for category, names in data.items():
        table = Table(title=category.upper(), show_header=False, box=None)
        table.add_column("name", style="green")
        for n in names:
            table.add_row(n)
        console.print(table)

    if not any(data.values()):
        console.print("[yellow]No stages registered. Import your plugins first.[/]")


# --------------------------------------------------------------------------- #
#  mdp inspect                                                                 #
# --------------------------------------------------------------------------- #


@cli.command()
@click.argument("file", type=click.Path(exists=True, path_type=Path))
@click.option("--max-packets", default=20, show_default=True, help="Max packets to display.")
@click.option(
    "--apid",
    multiple=True,
    type=int,
    help="Filter to specific APIDs (repeatable).",
)
def inspect(file: Path, max_packets: int, apid: tuple[int, ...]) -> None:
    """Inspect a raw binary telemetry file and display packet summaries."""
    from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor

    cfg = BinaryExtractorConfig(
        path=file,
        batch_size=max_packets,
        apid_filter=list(apid) if apid else None,
    )
    extractor = BinaryPacketExtractor(cfg)

    table = Table(title=f"[bold]{file.name}[/]", show_lines=False)
    table.add_column("APID", style="cyan", justify="right")
    table.add_column("SeqCount", justify="right")
    table.add_column("DataLen", justify="right")
    table.add_column("Type")
    table.add_column("SecHdr")

    count = 0
    try:
        for dataset in extractor.extract():
            for packet in dataset.packets:
                h = packet.header
                table.add_row(
                    f"0x{h.apid:04X}",
                    str(h.seq_count),
                    str(h.packet_data_length),
                    "TM" if h.type_flag == 0 else "TC",
                    "yes" if h.sec_hdr_flag else "no",
                )
                count += 1
                if count >= max_packets:
                    break
            if count >= max_packets:
                break
    except (FileNotFoundError, ValueError) as exc:
        console.print(table)
        Console(stderr=True).print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    console.print(table)
    console.print(f"\n[dim]Showed {count} packet(s). Use --max-packets to increase.[/]")


# --------------------------------------------------------------------------- #
#  mdp run                                                                     #
# --------------------------------------------------------------------------- #


@cli.command()
@click.option("--extractor", "extractor_name", required=True, help="Extractor plugin name.")
@click.option("--loader", "loader_name", default=None, help="Loader plugin name.")
@click.option(
    "--extractor-config",
    default=None,
    type=click.Path(exists=True, path_type=Path),
    help="JSON file with extractor config.",
)
@click.option(
    "--loader-config",
    default=None,
    type=click.Path(exists=True, path_type=Path),
    help="JSON file with loader config.",
)
@click.option(
    "--transformer",
    "transformer_names",
    multiple=True,
    help="Transformer plugin names (repeatable, applied in order).",
)
@click.option(
    "--transformer-config",
    "transformer_configs",
    multiple=True,
    metavar="NAME=FILE",
    help="JSON config for a transformer, e.g. decom=decom.json (repeatable).",
)
@click.option("--pipeline-name", default="mdp-run", show_default=True)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Extract and transform only; skip loading.",
)
@click.option(
    "--max-batches", default=None, type=click.IntRange(min=1), help="Stop after N batches."
)
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help="Treat data losses, such as skipped frames, as a failed run.",
)
def run(
    extractor_name: str,
    loader_name: str | None,
    extractor_config: Path | None,
    loader_config: Path | None,
    transformer_names: tuple[str, ...],
    transformer_configs: tuple[str, ...],
    pipeline_name: str,
    dry_run: bool,
    max_batches: int | None,
    strict: bool,
) -> None:
    """Run a pipeline from the command line using registered plugins."""
    from mdp.core.base import StageStatus
    from mdp.core.pipeline import Pipeline, PipelineConfig
    from mdp.core.registry import registry

    t_cfg_paths = _parse_transformer_configs(transformer_configs, transformer_names)

    ext_cls: Any = registry.get_extractor(extractor_name)
    ext_cfg_data = _load_json(extractor_config) if extractor_config else {}
    extractor = ext_cls(ext_cls.config_class(**ext_cfg_data))

    transformers = []
    for tname in transformer_names:
        t_cls: Any = registry.get_transformer(tname)
        t_cfg_data = _load_json(t_cfg_paths[tname]) if tname in t_cfg_paths else {}
        transformers.append(t_cls(t_cls.config_class(**t_cfg_data)))

    loader = None
    if loader_name and not dry_run:
        loader_cls: Any = registry.get_loader(loader_name)
        loader_cfg_data = _load_json(loader_config) if loader_config else {}
        loader = loader_cls(loader_cls.config_class(**loader_cfg_data))

    pipeline = Pipeline(
        config=PipelineConfig(
            name=pipeline_name,
            dry_run=dry_run,
            max_batches=max_batches,
            strict=strict,
        ),
        extractor=extractor,
        transformers=transformers,
        loader=loader,
    )

    result = pipeline.run()
    console.print(result.summary())

    if result.status == StageStatus.FAILED:
        sys.exit(1)

    if loader is not None and result.total_samples == 0:
        Console(stderr=True).print(
            "[red]Error:[/] nothing was written: no parameter samples reached the loader. "
            "Binary packets carry no parameters until the [bold]decom[/] transformer "
            "decodes them; see --transformer-config."
        )
        sys.exit(1)

    if result.status == StageStatus.DEGRADED:
        sys.exit(EXIT_DATA_LOST)


def _parse_transformer_configs(
    values: tuple[str, ...], transformer_names: tuple[str, ...]
) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for value in values:
        name, sep, file = value.partition("=")
        if not sep or not file:
            raise click.BadParameter(f"expected NAME=FILE, got {value!r}")
        if name not in transformer_names:
            raise click.BadParameter(f"{name!r} is not one of the --transformer stages")
        path = Path(file)
        if not path.exists():
            raise click.BadParameter(f"file not found: {file}")
        paths[name] = path
    return paths


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)  # type: ignore[no-any-return]
