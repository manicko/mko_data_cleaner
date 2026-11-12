"""Tests for the example notebook (structure only, fast, always green).

These tests validate the notebook artifact statically (nbformat validity, cell
ids, install-pin, public-API reference). Kernel execution is intentionally NOT
performed here to avoid environment fragility; real execution is exercised via
the B5 verification commands (dev-venv ``nbclient`` run) and the later against-tag
gate. See ``examples/README.md`` for the full cell map and expected outputs.
"""

from pathlib import Path

import nbformat

NOTEBOOK = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "notebooks"
    / "mko_data_cleaner_pipeline_example.ipynb"
)


def _load() -> nbformat.notebooknode.NotebookNode:
    nb = nbformat.read(NOTEBOOK, as_version=4)
    nbformat.validate(nb)
    return nb


def test_notebook_is_valid_nbformat():
    """Notebook is structurally valid (nbformat 4.5+)."""
    _load()


def test_notebook_has_unique_nonempty_cell_ids():
    """Every cell has a non-empty, unique id (nbformat >= 4.5)."""
    nb = _load()
    ids = [c.get("id", "") for c in nb.cells]
    assert all(ids), "some cells have empty/missing ids"
    assert len(ids) == len(set(ids)), "duplicate cell ids"


def test_notebook_install_cell_uses_library_check():
    """The install cell delegates version checking to the library function.

    If the package is missing or too old (no check_notebook_environment), the
    cell falls back to a %pip install --upgrade. The version-check logic itself
    lives in the library, not inline in the cell.
    """
    nb = _load()

    # The install cell (id="cell-install") must use the library check, not
    # inline importlib.metadata logic.
    install_cell = next(c for c in nb.cells if c.get("id") == "cell-install")
    assert "check_notebook_environment" in install_cell.source
    assert "importlib.metadata" not in install_cell.source  # delegated to library
    assert "ImportError" in install_cell.source  # fallback install path
    assert "--no-deps" not in install_cell.source

    # v0.2.0 still appears as a PIN reference across code cells.
    joined = "\n".join(c.source for c in nb.cells if c.cell_type == "code")
    assert "v0.2.0" in joined
    assert "git+https://github.com/manicko/mko_data_cleaner.git" in joined


def test_notebook_imports_public_api():
    """The import cell references the top-level public API (B4 contract)."""
    nb = _load()
    joined = "\n".join(c.source for c in nb.cells if c.cell_type == "code")
    assert "initialize_settings" in joined
    assert "process_data" in joined
    assert "ReportResult" in joined
