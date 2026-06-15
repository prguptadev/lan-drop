#!/bin/bash
# Lan Drop — LAN chat + file sharing. Run on BOTH Macs.
cd "$(dirname "$0")"
if [ ! -d venv ]; then
  python3 -m venv venv
  ./venv/bin/pip install -q -r requirements.txt
fi
./venv/bin/python app.py --name "$(scutil --get ComputerName 2>/dev/null || hostname)" "$@"
