#!/bin/sh
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if command -v powershell.exe >/dev/null 2>&1; then
  exec powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "$SCRIPT_DIR/.mvn/wrapper/mvnw.ps1" "$@"
fi
echo "This project wrapper currently requires Windows PowerShell." >&2
exit 1
