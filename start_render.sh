#!/bin/bash
# Render.com 起動スクリプト

DATA_DIR="${DATA_DIR:-$(pwd)}"

if [ -d "$DATA_DIR" ] && touch "$DATA_DIR/.write_test" 2>/dev/null; then
    rm -f "$DATA_DIR/.write_test"
    echo "DB場所: $DATA_DIR/scan.db"
else
    export DATA_DIR="$(pwd)"
    echo "DB場所: $(pwd)/scan.db (ローカル)"
fi

exec gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120
