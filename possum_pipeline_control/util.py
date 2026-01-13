from __future__ import annotations
from pathlib import Path
import os
import errno
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from prefect import task
from prefect.blocks.system import Secret

"""
Utility methods shared across the scripts
"""


def get_band_number(band: str) -> str:
    """
    Convert band string to band number.
    """
    return "1" if band == "943MHz" else "2"


def get_full_field_name(field_ID: str, band: str) -> str:
    """
    Build field name prefix

    args:
        field_ID: e.g. 1227-69
        band: '943MHz' or '1367MHz'

    returns:
        full field name: e.g. EMU_1227-69 for band 1, WALLABY_1227-69 for band 2
    """
    fieldname = (
        "EMU_" if band == "943MHz" else "WALLABY_"
    )  # TODO: verify WALLABY_ fieldname
    return f"{fieldname}{field_ID}"


def get_sbid_num(sbid: str | None) -> str | None:
    """
    Remove ASKAP- prefix from sbid if present
    """
    if sbid is None:
        return None

    if sbid.startswith("ASKAP-"):
        return sbid.replace("ASKAP-", "")

    return sbid

@task(name="Stage CADC certificate")
def stage_cadc_certificate(
    workdir: str,
    source_cert: str = "~/.ssl/cadcproxy.pem",
    dest_relpath: str = ".ssl/cadcproxy.pem",
    max_age_days: int = 30
) -> str:
    src = Path(os.path.expanduser(source_cert))
    if not src.exists():
        # Try the secret from prefect
        secret_block = Secret.load("cadc-proxy-pem")
        pem_str = secret_block.get()
        if pem_str:
            write_cadcproxy_pem(pem_str)
            src = Path(os.path.expanduser(source_cert))
        else:
            raise FileNotFoundError(
                f"Required CADC certificate not found: {src}"
                "Please run cadc-get-cert -u <UserName> --days-valid 30 in a CANFAR interactive session "
                "to update your CADC certificate!!!"
            )

    # Check age, certificates are max valid for 30 days
    mtime = datetime.fromtimestamp(src.stat().st_mtime, tz=timezone.utc)
    if datetime.now(timezone.utc) - mtime > timedelta(days=max_age_days):
        raise ValueError(
            "Please run cadc-get-cert -u <UserName> --days-valid 30 in a CANFAR interactive session "
            "to update your CADC certificate!!!"
        )

    # Copy to workdir/.ssl/cadcproxy.pem (create directory if needed)
    if workdir:
        dest = Path(workdir) / dest_relpath
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        return str(dest)

def write_cadcproxy_pem(content):
    # Construct the full path using the user's home directory
    home_dir = os.path.expanduser("~")
    ssl_dir = os.path.join(home_dir, ".ssl")
    file_path = os.path.join(ssl_dir, "cadcproxy.pem")

    # Create the directory if it does not exist
    if not os.path.exists(ssl_dir):
        try:
            os.makedirs(ssl_dir)
            print(f"Created directory: {ssl_dir}")
        except OSError as e:
            # Handle potential permissions issues or race conditions
            if e.errno != errno.EEXIST:
                raise

    # Write the content to the file
    try:
        with open(file_path, "w") as f:
            f.write(content.strip()) 
        print(f"Successfully wrote certificate to: {file_path}")
    except IOError as e:
        print(f"Error writing to file {file_path}: {e}")



class TemporaryWorkingDirectory:
    """Context manager to temporarily change the current working directory."""

    def __init__(self, path: str | os.PathLike):
        self.new_path = Path(path).expanduser().resolve()
        self._original_path: Path | None = None

    def __enter__(self) -> Path:
        # Store the original working directory
        self._original_path = Path.cwd()

        # Change to the new directory
        os.chdir(self.new_path)

        # Optionally return the new path for use inside the with-block
        return self.new_path

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        # Always restore the original directory, even if an exception occurred
        if self._original_path is not None:
            os.chdir(self._original_path)

        # Returning False means exceptions are not suppressed
        return False
