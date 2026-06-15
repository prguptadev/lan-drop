# LanDrop

A private, peer-to-peer **chat + file sharing + remote-dev** app for Macs on the same local network. No cloud, no accounts, no internet — everything stays on your LAN. Built to work even on locked-down corporate networks (e.g. Netskope) where cloud sync and device discovery are blocked.

Each Mac runs a tiny local server and opens a web UI in the browser. They find each other automatically (or by IP), and you get a Teams-style app for messaging, transferring files of any size, syncing project folders, and even opening a remote terminal.

---

## Requirements

- **macOS** (tested on Apple Silicon + Intel)
- **Python 3** (3.9+; comes with most setups — check `python3 --version`)
- Both Macs on the **same Wi-Fi / subnet**

Everything else (Python packages, the terminal library) installs automatically or is bundled — **no manual installs for normal use**.

Optional, only for the FUSE "Mount" mode (see Remote Dev):
- [macFUSE](https://macfuse.github.io) + `pip install fusepy requests`

---

## Quick start

Do this on **both** Macs.

1. Copy the whole `lan-drop` folder to the Mac (AirDrop, `scp`, git, etc.). Don't copy the `venv/` folder — it's machine-specific and rebuilds itself.
2. Double-click **`Start Lan Drop.command`** in Finder.
   - First run sets up a Python environment (~20s). Click **Allow** if macOS firewall prompts.
   - macOS may warn "unidentified developer" → right-click the file → **Open** → **Open**.
3. A **📩 icon** appears in the menu bar (top-right). Click it → **Open Chat** → the UI opens in your browser.
4. The two Macs discover each other and appear in each other's sidebars. Start chatting / sending files.

> Same port (**8765**) on both Macs — they're separate machines, so no conflict.

### Run it like a real app (optional)

Double-click **`install_app.command`** to build **`~/Applications/LanDrop.app`** (a proper menu-bar app named "LanDrop") and optionally **start it at login**.

### Run from a terminal instead

```bash
cd ~/Developer/lan-drop
./run.sh                 # foreground, with logs
# or a specific name:
./venv/bin/python app.py --name "My Mac" --port 8765
```

---

## Connecting the two Macs

The app tries three ways, in order of convenience:

| Method | When to use |
|---|---|
| **Auto-discovery (Bonjour)** | Home networks — devices just appear. |
| **Add by IP** | Office/corporate Wi-Fi that blocks discovery. Sidebar → "Add by IP" → type the other Mac's IP (comma-separate several). |
| **Scan network** | Finds every LanDrop instance on your subnet in one click. Great when discovery is blocked. |

Find a Mac's IP: `ipconfig getifaddr en0` (try `en1` if blank). Adding from **one** side connects both (they auto-learn each other).

You can also open the **▦ QR** code and scan it from a phone on the same Wi-Fi.

---

## Features

### Messaging
- **Full-duplex text chat** between any two devices.
- **Markdown & code blocks** — `**bold**`, `*italic*`, `` `inline code` ``, and triple-backtick code fences render nicely.
- **Clickable links** — URLs become tappable.
- **Reply** — hover a message → ↩ to quote-reply.
- **Reactions** — hover → ☺ → 👍 ❤️ 😂 🎉 ✅ 👀 (syncs to the other side).
- **Timestamps + delivery ticks**: ⏳ queued · 🕓 sending · ✓ sent · ✓✓ delivered · ⚠️ failed.
- **Unread badges + sound** when a message arrives in another conversation.
- **Message search** — 🔍 in the sidebar searches all conversations; click a result to jump.
- **Store-and-forward** — messages/files to an **offline** device are queued and auto-delivered when it comes back.

### Files
- **Any size** — files stream directly between Macs; a 50 GB file works like a 50 KB one.
- **Drag & drop** onto the chat, or 📎 to pick.
- **Paste-to-send** — copy a screenshot and paste (⌘V) into the chat to send it.
- **Folder send** — 📁 zips a folder on the fly and sends it.
- **Inline image preview** + an **audio player** for voice memos.
- **Voice memos** — 🎙️ record, tap again to send.
- **Auto-save** — received files are also saved to **`~/Downloads/LanDrop/`** (toggle in Settings; "Open folder" reveals it).
- **Transfer panel** — bottom-right cards show in-flight transfers with a progress bar and a ✕ to cancel.

### Clipboard sync
- 📋 button sends your current clipboard (text or image) to the selected device. It lands on **their** clipboard, ready to paste — useful across Macs that don't share an Apple ID.

### Devices & groups
- **Online/offline dots** (green = reachable), refreshed every 5s.
- **Rename** a device (✏️, saved per IP) or **set your own name** (✏️ by your name, shown to others).
- **Remove** a device (✕); **remembered peers auto-reconnect** on next launch.
- **Groups** — ＋ create a group of devices; messages/files broadcast to all members.

### Settings (⚙️)
- **Dark / light theme**
- **Image previews** on/off
- **Sound** on/off
- **Auto-save received files** on/off + Open folder
- **Enable remote terminal** + PIN (see below)

### Remote dev (terminal + project sync)
The headline feature: run a CLI (e.g. an AI coding tool) that lives on **one** Mac, operating on a project that lives on the **other**. See the dedicated section below.

---

## Remote dev: terminal + project sync

> Use case: your office Mac has the code but can't install CLIs; your personal Mac has the CLIs (Claude/Gemini/Codex). Run the personal CLI on the office code.

### A. Remote terminal (PIN-gated, off by default)

Opens a real shell **on the target Mac**, shown in your browser.

1. On the Mac you want to control (e.g. **personal**): ⚙️ Settings → **Enable remote terminal** → set a **PIN**.
2. On the other Mac: open that device's chat → **Terminal** in the header → enter the PIN (**once** — it's remembered) → live `xterm` shell.

> Security: this is a full shell over the LAN. It's **off by default** and **PIN-protected**. Only enable it on machines/networks you trust, and be mindful of corporate policy.

### B. Project file sync (the workspace)

The Mac that **owns the code** *shares* a folder; the Mac that **runs the CLI** *opens* it as a synced mirror.

1. **On the code Mac**: sidebar `</>` icon → **Share a folder** → enter a **specific project path** (e.g. `/Users/you/code/myproject` — *not* a giant parent folder). Set a PIN (Settings → terminal PIN gates file shares too).
2. **On the CLI Mac**: open the code Mac's chat → **Project** in the header → enter the PIN → pick the shared folder → choose:
   - **Sync** — mirrors it to `~/LanDrop-Workspaces/<name>/` and keeps it two-way synced. *No extra install.*
   - **Mount** — live FUSE mount, nothing copied. *Needs macFUSE.*
3. **On the CLI Mac**: open the *other* Mac's **Terminal**, then:
   ```bash
   cd ~/LanDrop-Workspaces/<name>
   claude        # or gemini / codex / any tool
   ```
4. Every edit syncs back automatically — **local edits in ~0.2s** (instant FSEvents watch), remote edits within ~3s.

### Sync details

- **Two-way & automatic** — no clicks. Edit, add, or delete a file → it syncs.
- **Per-file** — only changed files move, not the whole folder.
- **Status bar** (under the header) + the `</>` panel show live status: `synced · N files`, `pulling 12/40…`, or a red `paused: <reason>` (wrong PIN / can't reach host).
- **Conflicts** (both sides edit the same file): host copy wins, with a toast.
- **Skipped automatically** to stay lean: `.git`, `node_modules`, `venv`, `dist`, `build`, archives (`.zip .tar .gz .rar .7z .dmg .iso`), media, model weights (`.pt .ckpt .safetensors .h5 .pkl .onnx`), databases, and **any file over 100 MB**.

> Tip: share the **smallest folder you actually need** (one repo). Sync scales with file count.

> Note: the Sync route copies the shared folder onto the other Mac. If that's a concern for confidential code, use **Mount (FUSE)** so nothing is copied — or get IT sign-off.

---

## Testing guide

Quick checks that each area works. Run with two Macs connected (both showing in the sidebar).

| Feature | How to test | Expected |
|---|---|---|
| Chat | Send a message | Appears on the other Mac; tick goes ✓ → ✓✓ |
| Offline queue | Quit app on Mac B, send from A, relaunch B | Message shows ⏳ then delivers when B returns |
| File | Drag a file onto the chat | Arrives; saved in `~/Downloads/LanDrop/` on the receiver |
| Big file | Send a few-hundred-MB file | Transfers fully; progress in the bottom-right panel |
| Folder | 📁 → pick a folder | Receiver gets `<folder>.zip` |
| Voice | 🎙️ record → tap again | Audio player bubble on both sides |
| Clipboard | Copy text, click 📋 | Paste (⌘V) on the other Mac yields it |
| Reply/React | Hover a message → ↩ / ☺ | Quote + emoji appear on both sides |
| Search | 🔍 → type | Matching messages listed; click jumps to chat |
| Theme | ⚙️ → toggle Dark mode | UI switches instantly |
| Terminal | Enable + PIN on B, Terminal on A | Shell prompt from B; `hostname` shows B |
| Sync | Share folder on B, open Sync on A, edit a file in `~/LanDrop-Workspaces/<name>` | Change appears on B within a couple seconds; status bar shows `synced · N files` |

---

## Troubleshooting

- **Devices don't appear** → office Wi-Fi likely blocks discovery. Use **Add by IP** or **Scan network**. Confirm reachability by opening `http://<other-ip>:8765/whoami` in a browser — JSON = reachable.
- **Files/messages don't arrive one direction** → a firewall is blocking incoming. The status bar / a red toast shows the reason. For project sync, the fix is to **flip roles** (let the reachable Mac host).
- **Terminal button missing** → the other Mac hasn't enabled the terminal (Settings → Enable remote terminal).
- **Terminal won't open / wrong PIN** → re-enter; the saved PIN is cleared on failure so you'll be prompted again.
- **Sync stuck on `connecting…` / `pulling…`** → you shared a huge folder. Share a single project, not a multi-GB parent. Big blobs are skipped automatically now.
- **Mount (FUSE) error about "Benjamin Fleischer"** → that's macFUSE's kernel extension. System Settings → Privacy & Security → **Allow** → restart (on Apple Silicon, also enable kernel extensions via Recovery). Or just use **Sync** mode.
- **"Python launcher" name in the menu bar** → run `install_app.command` to get a proper `LanDrop.app`.
- **Logs**: menu-bar app logs to `/tmp/lan-drop.log`.

---

## Security & privacy

- 100% **local LAN** — no data leaves your network, no third parties.
- **Remote terminal** and **file shares** are **off by default** and **PIN-gated**. Anyone on the LAN who has the PIN can use them — set a strong PIN and only enable on trusted networks.
- Traffic is **plain HTTP on the LAN** (no TLS) — fine for a trusted home/closed network; don't expose it to untrusted networks.
- No device-approval prompt yet: any device on the LAN can send you chat/files. Keep that in mind on shared Wi-Fi.

---

## Project layout

```
lan-drop/
├── app.py                  # the server: chat, files, discovery, terminal, file API
├── remotefs.py             # two-way sync engine + FUSE mount
├── menubar.py              # menu-bar (📩) launcher
├── index.html              # the entire web UI (HTML/CSS/JS, no build step)
├── vendor/                 # bundled xterm.js (terminal) — no CDN needed
├── requirements.txt        # aiohttp, zeroconf, rumps, qrcode, watchdog
├── run.sh                  # run in a terminal
├── Start Lan Drop.command  # double-click launcher (menu-bar, detached)
├── install_app.command     # build ~/Applications/LanDrop.app + login item
├── uploads/                # transient file cache (gitignored)
└── ~/Downloads/LanDrop/    # auto-saved received files
    ~/LanDrop-Workspaces/   # mirrored project folders
```

**Port:** 8765 · **Bonjour service:** `_landrop._tcp` · **Per-machine state** (`config.json`, `known_peers.json`, `shares.json`, etc.) is gitignored.
