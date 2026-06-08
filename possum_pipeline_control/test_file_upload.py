from datetime import datetime, timezone
from pathlib import Path
import subprocess
from vos import Client
# create file name for backup
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
OUTDIR = Path.home() / "prefect-backups"
OUTDIR.mkdir(parents=True, exist_ok=True)
db_file_name = f"prefect-{ts}.sql"
db_backup = OUTDIR / db_file_name
db_backup.write_text("mock backup")

# --- Copy to CANFAR ---
print("Copying the backup to CANFAR...")
try:
    client = Client()
    VOS_FOLDER = "arc:projects/CIRADA/polarimetry/software/prefect-backups"
    remote_file = f"{VOS_FOLDER}/{db_file_name}"
    client.copy(str(db_backup), remote_file)
    print("Backup completed successfully")    
except Exception as e:
    print(f"Failed to copy backup to CANFAR: {e}")  