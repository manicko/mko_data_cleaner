"""Tests for mko_data_cleaner.notebook.check_notebook_environment."""

from importlib.metadata import PackageNotFoundError

from mko_data_cleaner.notebook import check_notebook_environment


def test_check_reports_up_to_date(monkeypatch, capfd):
    """Installed version matches pin -> up-to-date message, no warning."""
    monkeypatch.setattr("mko_data_cleaner.notebook.version", lambda pkg: "0.2.0")
    check_notebook_environment()
    captured = capfd.readouterr()
    assert "up to date" in captured.out
    assert "--force-reinstall" not in captured.out


def test_check_warns_stale_version(monkeypatch, capfd):
    """Installed version behind pin -> warning with force-reinstall command."""
    monkeypatch.setattr("mko_data_cleaner.notebook.version", lambda pkg: "0.1.0")
    check_notebook_environment()
    captured = capfd.readouterr()
    assert "--force-reinstall" in captured.out
    assert "outdated" in captured.out
    assert "up to date" not in captured.out


def test_check_not_installed(monkeypatch, capfd):
    """Package not found -> prints install hint with git URL."""

    def _raise(pkg):
        raise PackageNotFoundError(pkg)

    monkeypatch.setattr("mko_data_cleaner.notebook.version", _raise)
    check_notebook_environment()
    captured = capfd.readouterr()
    assert "github.com/manicko/mko_data_cleaner" in captured.out
    assert "--force-reinstall" in captured.out


def test_check_respected_disable_env(monkeypatch, capfd):
    """MKO_NO_INSTALL_CHECK set -> no-op, no output."""
    monkeypatch.setenv("MKO_NO_INSTALL_CHECK", "1")
    monkeypatch.setattr("mko_data_cleaner.notebook.version", lambda pkg: "0.1.0")
    check_notebook_environment()
    captured = capfd.readouterr()
    assert captured.out == ""
    assert captured.err == ""
