#!/bin/bash
# Double-click this in Finder to start Lan Drop in the menu bar.
cd "$(dirname "$0")"

if [ ! -d venv ]; then
  echo "First run: setting up… (this happens once)"
  python3 -m venv venv
  ./venv/bin/pip install -q -r requirements.txt
fi

# Launch the menu-bar app detached so closing this window won't stop it.
nohup ./venv/bin/python menubar.py > /tmp/lan-drop.log 2>&1 &
disown

echo ""
echo "  Lan Drop is starting in your menu bar (top-right, 📩 icon)."
echo "  You can close this window now."
sleep 2
