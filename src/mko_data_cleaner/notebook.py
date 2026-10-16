"""Notebook environment checker for mko_data_cleaner.

Provides :func:`check_notebook_environment` — a lightweight, zero-network
version check intended for use in example notebooks.  It compares the
*installed* distribution version against a hardcoded ``pinned_tag`` using
:mod:`packaging` (correct PEP 440 comparison) and prints either a
``✓ … up to date`` confirmation or a ``%pip install –-force-reinstall``
hint.

No network access is performed.  Set ``MKO_NO_INSTALL_CHECK`` to a truthy
value to skip the check entirely (offline / air-gapped escape hatch,
modeled on ``HF_HUB_DISABLE_UPDATE_CHECK``).
"""

import logging
import os
from importlib.metadata import PackageNotFoundError, version

from packaging.version import Version

logger = logging.getLogger(__name__)

#: Repository install URL used in the recovery hint printed to notebook stdout.
GIT_URL = "https://github.com/manicko/mko_data_cleaner.git"


def check_notebook_environment(
    pkg: str = "mko_data_cleaner", pinned_tag: str = "v0.2.0"
) -> None:
    """Compare the installed version of *pkg* against the *pinned_tag*.

    The check is **offline**: the installed version is read via
    :func:`importlib.metadata.version` and compared to *pinned_tag* only.
    No PyPI or network lookup is performed.

    Parameters
    ----------
    pkg:
        Distribution name to inspect (default ``"mko_data_cleaner"``).
    pinned_tag:
        Reference tag/version to compare against (default ``"v0.2.0"``).
        A leading ``v`` is stripped before parsing with
        :class:`packaging.version.Version`.

    User-facing output (printed to stdout):
    * Package not installed — an install hint with the full git URL.
    * Installed version behind pin — a stale warning with the recovery
      ``%pip install –-force-reinstall –-no-deps`` command.
    * Installed version at or above pin — ``✓ {pkg} {version} — up to date``.

    Set the ``MKO_NO_INSTALL_CHECK`` environment variable to any truthy
    value to make this function a silent no-op.
    """
    if os.environ.get("MKO_NO_INSTALL_CHECK"):
        return

    try:
        installed = version(pkg)
    except PackageNotFoundError:
        print(
            f"{pkg} is not installed. Install it with:\n"
            f"  %pip install --force-reinstall --no-deps "
            f"git+{GIT_URL}@{pinned_tag}"
        )
        return

    logger.debug(
        "Installed %s %s; pin is %s", pkg, installed, pinned_tag
    )

    pinned = Version(pinned_tag.lstrip("v"))
    current = Version(installed)

    if current < pinned:
        print(
            f"{pkg} is outdated (installed {installed}, pinned {pinned}). "
            f"Update with:\n"
            f"  %pip install --force-reinstall --no-deps "
            f"git+{GIT_URL}@{pinned_tag}"
        )
    else:
        print(f"✓ {pkg} {installed} — up to date")
