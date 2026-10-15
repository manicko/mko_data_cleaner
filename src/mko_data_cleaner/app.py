"""
mko_data_cleaner — основной публичный интерфейс
"""

import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from mko_data_cleaner.core.app_service import build_app_paths, get_app_service
from mko_data_cleaner.core.init_service import init_project
from mko_data_cleaner.core.models import ReportResult
from mko_data_cleaner.core.utils import list_files_in_directory

# Jupyter / ipykernel replaces sys.stdout (and sys.stderr) with an OutStream-like
# object that does not implement reconfigure(). The previous unconditional call
# raised AttributeError at import time and broke `import mko_data_cleaner` inside
# any kernel — the project's primary "Jupyter pipeline step" entry point. Guard
# the call so it only reconfigures real terminal streams; kernels skip it
# (line buffering is irrelevant inside a kernel).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

console = Console()
app = typer.Typer(
    name="mko-data-cleaner",
    help="Инструмент для дополнительной очистки и подготовки массивов данных Mediascope (TV / TV_REG).",
    add_completion=True,
)


@app.command()
def init(
    force: bool = typer.Option(
        False, "--force", "-f", help="Перезаписать существующие файлы"
    ),
    settings_dir: Annotated[
        Path | None,
        typer.Option(
            exists=False,
            file_okay=False,
            dir_okay=True,
            help="User settings directory (default: system config dir)",
        ),
    ] = None,
):
    """Инициализировать пользовательские настройки"""
    path = init_project(force=force, settings_dir=settings_dir)
    console.print(f"[green]✅ Настройки инициализированы:[/green] {path}")


@app.command()
def run(
    report: Annotated[
        Path,
        typer.Argument(exists=True, dir_okay=True, help="Путь к директории отчета"),
    ],
    verbose: bool = typer.Option(True, "--verbose", "-v"),
    settings_dir: Annotated[
        Path | None,
        typer.Option(
            exists=False,
            file_okay=False,
            dir_okay=True,
            help="User settings directory (default: system config dir)",
        ),
    ] = None,
):
    """Запустить выгрузку отчёта"""
    if verbose:
        console.print(f"[blue]▶ Запуск отчёта:[/blue] {report.name}")

    # The service is created via the factory; SettingsNotInitializedError is
    # propagated when user settings are not initialized.
    service = get_app_service(settings_dir)

    try:
        result = service.run_report(report)
        console.print(
            f"[green]✅ Report complete:[/green] "
            f"{result.rows_loaded:,} loaded → {result.rows_exported:,} clean "
            f"({result.duration:.1f}s) ↠ {result.export_dir}"
        )

    except Exception as e:
        console.print(f"[red]❌ Ошибка:[/red] {e}")
        raise typer.Exit(1) from e


@app.command()
def list_settings(
    settings_dir: Annotated[
        Path | None,
        typer.Option(
            exists=False,
            file_okay=False,
            dir_okay=True,
            help="User settings directory (default: system config dir)",
        ),
    ] = None,
):
    """Show settings files in the user settings directory"""
    app_paths = build_app_paths(settings_dir)
    settings_path = app_paths.user_settings_dir

    if not settings_path.is_dir():
        console.print(
            f"[yellow]⚠ Settings directory not found: {settings_path}[/yellow]"
        )
        console.print("Run `mko-data-cleaner init` to create settings.")
        return

    files = list_files_in_directory(
        settings_path, extensions=("yaml", "yml"), include_subfolders=False
    )
    if not files:
        console.print("[yellow]No settings files found[/yellow]")
        return
    for f in sorted(files):
        console.print(f"• {f.name}")


# Public API
def process_data(
    report_path: str | Path, settings_dir: Path | None = None
) -> ReportResult:
    """
    Run the data-cleaning report for the given directory.

    Synchronous entry point for non-CLI contexts (Jupyter / Airflow).
    ``report_path`` must reference an existing directory (the report folder).
    User settings are loaded lazily via the factory; if ``app_config.yaml`` or
    ``log_config.yaml`` are missing, :class:`SettingsNotInitializedError`
    (a subclass of ``FileNotFoundError``) is raised.

    Returns:
        :class:`~mko_data_cleaner.core.models.ReportResult` with output
        paths, timings, and row counts.
    """
    path = Path(report_path)
    if not path.is_dir():
        raise FileNotFoundError(f"Directory not found: {path}")
    return get_app_service(settings_dir).run_report(path)


def initialize_settings(force: bool = False, settings_dir: Path | None = None) -> Path:
    """Initialize user settings with default templates (Jupyter/Airflow entry point)."""
    return init_project(force=force, settings_dir=settings_dir)


if __name__ == "__main__":
    app()


__all__ = [
    "process_data",
    "initialize_settings",
    "init",
    "run",
    "list_settings",
    "app",
    "get_app_service",
]
