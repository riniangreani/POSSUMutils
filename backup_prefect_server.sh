### Script to make a one-shot backup of the prefect database
### will be saved with timestamp, as e.g. prefect-2025-12-16T142502Z.sql

# --- PostgreSQL connection settings ---
PGHOST="localhost"
PGPORT="5432"
PGDATABASE="prefect"
PGUSER="prefect"
# Prefer .pgpass or env var over hardcoding
# export PGPASSWORD="secret"

# --- Backup output ---
OUTDIR="$HOME/prefect-backups"
mkdir -p "$OUTDIR"

ts="$(date -u +%Y-%m-%dT%H%M%SZ)"
out="$OUTDIR/prefect-$ts.sql"

echo "Starting PostgreSQL backup..."

pg_dump \
  --host="$PGHOST" \
  --port="$PGPORT" \
  --username="$PGUSER" \
  --format=plain \
  --no-owner \
  --no-privileges \
  "$PGDATABASE" > "$out"

echo "Backup written to: $out"

# --- Copy to CANFAR ---
echo "Copying the backup to CANFAR..."
vcp "$out" arc:projects/CIRADA/polarimetry/software/prefect-backups

echo "Backup completed successfully"

