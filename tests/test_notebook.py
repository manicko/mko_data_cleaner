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


def test_notebook_install_cell_pins_v0_2_0():
    """The install cell pins the v0.2.0 tag (D1/D11)."""
    nb = _load()
    joined = "\n".join(c.source for c in nb.cells if c.cell_type == "code")
    assert "git+https://github.com/manicko/mko_data_cleaner.git@v0.2.0" in joined


def test_notebook_imports_public_api():
    """The import cell references the top-level public API (B4 contract)."""
    nb = _load()
    joined = "\n".join(c.source for c in nb.cells if c.cell_type == "code")
    assert "initialize_settings" in joined
    assert "process_data" in joined
    assert "ReportResult" in joined
