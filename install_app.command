#!/bin/bash
# Build LanDrop.app (a real menu-bar app) into ~/Applications and optionally
# start it at login. Double-click this file in Finder, or run it in Terminal.
set -e
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP="$HOME/Applications/LanDrop.app"

echo "Building LanDrop.app from: $PROJECT_DIR"

# ensure the venv exists
if [ ! -d "$PROJECT_DIR/venv" ]; then
  echo "Setting up Python environment (one time)…"
  python3 -m venv "$PROJECT_DIR/venv"
  "$PROJECT_DIR/venv/bin/pip" install -q -r "$PROJECT_DIR/requirements.txt"
fi

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>LanDrop</string>
  <key>CFBundleDisplayName</key><string>LanDrop</string>
  <key>CFBundleIdentifier</key><string>com.landrop.app</string>
  <key>CFBundleVersion</key><string>1.0</string>
  <key>CFBundleShortVersionString</key><string>1.0</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleExecutable</key><string>LanDrop</string>
  <key>LSUIElement</key><true/>
  <key>LSMinimumSystemVersion</key><string>10.13</string>
</dict>
</plist>
PLIST

cat > "$APP/Contents/MacOS/LanDrop" <<LAUNCHER
#!/bin/bash
cd "$PROJECT_DIR"
if [ ! -d venv ]; then
  python3 -m venv venv
  ./venv/bin/pip install -q -r requirements.txt
fi
exec ./venv/bin/python menubar.py
LAUNCHER
chmod +x "$APP/Contents/MacOS/LanDrop"

echo ""
echo "  ✓ Installed: $APP"
echo "  Launch it from Applications (or Spotlight: 'LanDrop'). It runs in the menu bar (📩)."
echo ""

read -r -p "Start LanDrop automatically at login? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
  PLIST_PATH="$HOME/Library/LaunchAgents/com.landrop.plist"
  mkdir -p "$HOME/Library/LaunchAgents"
  cat > "$PLIST_PATH" <<AGENT
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.landrop.app</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PROJECT_DIR/venv/bin/python</string>
    <string>$PROJECT_DIR/menubar.py</string>
  </array>
  <key>WorkingDirectory</key><string>$PROJECT_DIR</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><false/>
</dict>
</plist>
AGENT
  launchctl unload "$PLIST_PATH" 2>/dev/null || true
  launchctl load "$PLIST_PATH"
  echo "  ✓ Will start at login (and starting now)."
else
  echo "  Skipped login-start. You can add LanDrop.app under System Settings → General → Login Items anytime."
fi
echo ""
echo "Done. You can close this window."
