"""
mko_get_mediascope_data — основной публичный интерфейс
"""
import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from mko_data_cleaner.core.init_service import init_project
from mko_data_cleaner.core.services import app_service
from mko_data_cleaner.core.utils import list_files_in_directory

sys.stdout.reconfigure(line_buffering=True)

console = Console()
app = typer.Typer(
    name="mko_get_mediascope_data",
    help="Инструмент выгрузки данных из Mediascope (TV / TV_REG)",
    add_completion=True,
)


@app.command()
def init(
    force: bool = typer.Option(
        False, "--force", "-f", help="Перезаписать существующие файлы"
    )
):
    """Инициализировать пользовательские настройки"""
    path = init_project(force=force)
    console.print(f"[green]✅ Настройки инициализированы:[/green] {path}")


@app.command()
def run(
    report: Annotated[
        Path,
        typer.Argument(exists=True, dir_okay=False, help="Путь к yaml-файлу задания"),
    ],
    verbose: bool = typer.Option(True, "--verbose", "-v"),
):
    """Запустить выгрузку отчёта"""
    if verbose:
        console.print(f"[blue]▶ Запуск отчёта:[/blue] {report.name}")

    try:
        app_service.process_data(report)
        console.print("[green]✅ Отчёт успешно завершён[/green]")

    except Exception as e:
        console.print(f"[red]❌ Ошибка:[/red] {e}")
        raise typer.Exit(1) from e


@app.command()
def list_settings():
    """Показать все доступные файлы заданий"""
    reports_dir = app_service.app_paths.reports
    files = list_files_in_directory(
        reports_dir, extensions=("yaml",), include_subfolders=True
    )
    if not files:
        console.print("[yellow]Нет файлов в reports/[/yellow]")
        return
    for f in sorted(files):
        console.print(f"• {f}")


# Public API
def process_data(report_path: str | Path) -> None:
    """
    Асинхронная версия для async-окружений (например, FastAPI).
    """
    path = Path(report_path)
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")
    app_service.process_data(path)




def initialize_settings(force: bool = False) -> Path:
    """Обёртка для Jupyter/Airflow"""
    return init_project(force=force)


if __name__ == "__main__":
    app()


__all__ = [
    "process_data",
    "initialize_settings",
    "init",
    "run",
    "list_settings",
    "app",
]
