#!/usr/bin/env bash
set -euo pipefail
ZIP_FILE=${1:-}
if [[ -z "$ZIP_FILE" ]]; then
  echo "Uso: $0 <backup.zip>"
  exit 1
fi
mkdir -p data/storage
unzip -o "$ZIP_FILE" -d /tmp/parker_restore
cp /tmp/parker_restore/parker.db data/parker.db
rsync -a /tmp/parker_restore/storage/ data/storage/
echo "Restauração concluída"
