#!/bin/bash

# CRM Orchestrator Server Starter

echo "Starting CRM Orchestrator..."

# Ensure we are in the project root
cd "$(dirname "$0")/.."

# Start the orchestrator in background (optional nohup for persistence)
# To run in background: nohup ~/.local/bin/uv run python src/orchestrator.py > orchestrator.log 2>&1 &
~/.local/bin/uv run python src/orchestrator.py

# --- CRONTAB ALTERNATIVE ---
# If you prefer using system cron instead of a running python server:
# 0 0 * * * cd /home/caiafa/projetos/michel/crm && export PYTHONPATH=$PYTHONPATH:. && ~/.local/bin/uv run python src/orchestrator.py --now >> data/cron.log 2>&1
