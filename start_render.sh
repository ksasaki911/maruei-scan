#!/bin/bash
# Render.com 起動スクリプト (PostgreSQL版)
# ワーカー1つ: スレッドベースの同期ステータス共有のため

echo "PostgreSQL版 起動中..."
exec gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 300
