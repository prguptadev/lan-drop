import argparse
import asyncio
import fcntl
import json
import os
import pty
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import termios
import time
import urllib.parse
import uuid
import zipfile
from io import BytesIO
from pathlib import Path

from aiohttp import web, ClientSession, ClientTimeout
from zeroconf import ServiceInfo, ServiceStateChange
from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser, AsyncServiceInfo

SERVICE_TYPE = "_landrop._tcp.local."
BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
NAME_FILE = BASE_DIR / ".displayname"
KNOWN_FILE = BASE_DIR / "known_peers.json"
OUTBOX_FILE = BASE_DIR / "outbox.json"
CONFIG_FILE = BASE_DIR / "config.json"
SHARES_FILE = BASE_DIR / "shares.json"
WORKSPACES_FILE = BASE_DIR / "workspaces.json"
SAVE_DIR = Path.home() / "Downloads" / "LanDrop"
WS_ROOT = Path.home() / "LanDrop-Workspaces"


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


class App:
    def __init__(self, name, port):
        self.name = name
        try:
            saved = NAME_FILE.read_text().strip()
            if saved:
                self.name = saved
        except Exception:
            pass
        self.port = port
        self.id = uuid.uuid4().hex[:8]
        self.ip = get_local_ip()
        self.peers = {}        # id -> {id, name, ip, port, _sname, online}
        self.uploads = {}      # fileId -> {path, name, size}
        self.browser_ws = set()
        self.aiozc = None
        self.browser = None
        self.outbox = self._load_json(OUTBOX_FILE, {})   # ip -> [items] (store-and-forward)
        self._flushing = set()                           # ips currently being flushed
        cfg = self._load_json(CONFIG_FILE, {})
        self.autosave = cfg.get("autosave", True)        # save incoming files to ~/Downloads/LanDrop
        self.terminal_enabled = cfg.get("terminal_enabled", False)
        self.terminal_pin = cfg.get("terminal_pin") or None
        self.shares = self._load_json(SHARES_FILE, {})            # host side: shareId -> {name, root}
        self.workspaces = self._load_json(WORKSPACES_FILE, {})    # client side: wsId -> {...}
        self._dedup_workspaces()
        self.sync_sessions = {}   # wsId -> SyncSession
        self.fuse_stops = {}      # wsId -> stop callable
        self.share_activity = {}  # shareId -> {"last": ts, "reads": n, "writes": n}
        self.share_versions = {}  # shareId -> float (bumps on any change in the shared folder)
        self.share_observers = {} # shareId -> watchdog Observer

    def _dedup_workspaces(self):
        seen = {}
        for wid in list(self.workspaces.keys()):
            key = self.workspaces[wid].get("local")
            if key in seen:
                del self.workspaces[wid]
            else:
                seen[key] = wid
        self._save_workspaces()

    def _touch_share(self, sid, kind):
        a = self.share_activity.setdefault(sid, {"last": 0, "reads": 0, "writes": 0})
        a["last"] = time.time()
        a[kind] = a.get(kind, 0) + 1

    def _start_share_watch(self, sid):
        sh = self.shares.get(sid)
        if not sh or sid in self.share_observers:
            return
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler
        except Exception:
            return
        self.share_versions[sid] = time.time()
        versions = self.share_versions

        class _H(FileSystemEventHandler):
            def on_any_event(self, event):
                versions[sid] = time.time()

        try:
            obs = Observer()
            obs.schedule(_H(), sh["root"], recursive=True)
            obs.start()
            self.share_observers[sid] = obs
        except Exception:
            pass

    def _stop_share_watch(self, sid):
        obs = self.share_observers.pop(sid, None)
        if obs:
            try:
                obs.stop()
            except Exception:
                pass
        self.share_versions.pop(sid, None)

    def start_share_watchers(self):
        for sid in list(self.shares):
            self._start_share_watch(sid)

    @staticmethod
    def _load_json(path, default):
        try:
            return json.loads(path.read_text())
        except Exception:
            return default if not isinstance(default, dict) else dict(default)

    def _save_config(self):
        try:
            CONFIG_FILE.write_text(json.dumps({
                "autosave": self.autosave,
                "terminal_enabled": self.terminal_enabled,
                "terminal_pin": self.terminal_pin,
            }))
        except Exception:
            pass

    # ---------- discovery ----------
    async def start_discovery(self):
        self.aiozc = AsyncZeroconf()
        info = ServiceInfo(
            SERVICE_TYPE,
            f"{self.name}-{self.id}.{SERVICE_TYPE}",
            addresses=[socket.inet_aton(self.ip)],
            port=self.port,
            properties={"name": self.name, "id": self.id},
        )
        await self.aiozc.async_register_service(info)
        self.browser = AsyncServiceBrowser(
            self.aiozc.zeroconf, SERVICE_TYPE, handlers=[self._on_change]
        )

    def _on_change(self, zeroconf, service_type, name, state_change):
        asyncio.ensure_future(self._handle_change(service_type, name, state_change))

    async def _handle_change(self, service_type, name, state_change):
        if state_change is ServiceStateChange.Removed:
            return  # heartbeat marks offline; keep the entry visible
        info = AsyncServiceInfo(service_type, name)
        await info.async_request(self.aiozc.zeroconf, 3000)
        props = {}
        for k, v in (info.properties or {}).items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            props[key] = val
        pid = props.get("id")
        if not pid or pid == self.id:
            return
        addrs = info.parsed_addresses()
        ip = addrs[0] if addrs else None
        if not ip:
            return
        self._upsert_peer(pid, props.get("name", "Unknown"), ip, info.port, name, online=True)
        self._save_known()
        await self.broadcast_peers()

    def _upsert_peer(self, pid, name, ip, port, sname, online=True):
        for old in [k for k, v in self.peers.items() if k != pid and v.get("ip") == ip]:
            self.peers.pop(old, None)
        existing = self.peers.get(pid, {})
        self.peers[pid] = {
            "id": pid, "name": name, "ip": ip, "port": port,
            "_sname": sname, "online": online if online is not None else existing.get("online", True),
        }

    # ---------- remembered peers ----------
    def _load_known(self):
        try:
            return json.loads(KNOWN_FILE.read_text())
        except Exception:
            return []

    def _save_known(self):
        seen = {}
        for p in self.peers.values():
            if p.get("ip"):
                seen[p["ip"]] = p.get("port", self.port)
        try:
            KNOWN_FILE.write_text(json.dumps([{"ip": k, "port": v} for k, v in seen.items()]))
        except Exception:
            pass

    async def reconnect_known(self):
        for entry in self._load_known():
            ip = entry.get("ip")
            port = int(entry.get("port") or self.port)
            if ip and ip != self.ip:
                tid = f"manual:{ip}"
                if tid not in self.peers:
                    self.peers[tid] = {"id": tid, "name": ip, "ip": ip, "port": port,
                                       "_sname": tid, "online": False}
        if self.peers:
            await self.broadcast_peers()

    # ---------- heartbeat (online/offline) ----------
    async def _probe(self, ip, port, timeout=2.0):
        try:
            async with ClientSession(timeout=ClientTimeout(total=timeout)) as s:
                async with s.get(f"http://{ip}:{port}/whoami") as r:
                    if r.status == 200:
                        return await r.json()
        except Exception:
            pass
        return None

    async def heartbeat(self):
        while True:
            await asyncio.sleep(5)
            changed = False
            came_online = []
            for p in list(self.peers.values()):
                was_online = p.get("online", False)
                info = await self._probe(p["ip"], p["port"])
                if info:
                    rid = info.get("id")
                    nm = info.get("name", p["name"])
                    if rid and rid != p["id"]:
                        self._upsert_peer(rid, nm, p["ip"], p["port"], p.get("_sname", "manual"), online=True)
                        changed = True
                        target = self.peers[rid]
                    else:
                        if not was_online or p["name"] != nm:
                            changed = True
                        p["online"] = True
                        p["name"] = nm
                        target = p
                    if target.get("terminal") != info.get("terminal", False):
                        changed = True
                    target["terminal"] = info.get("terminal", False)
                    if not was_online:
                        came_online.append(target)
                else:
                    if was_online:
                        changed = True
                    p["online"] = False
            if changed:
                await self.broadcast_peers()
            if self.sync_sessions or self.fuse_stops:
                await self._broadcast_workspaces()
            if self.shares:
                await self._broadcast_shares()
            for peer in came_online:
                await self._flush(peer)

    # ---------- subnet scan ----------
    async def scan(self):
        base = self.ip.rsplit(".", 1)[0]
        sem = asyncio.Semaphore(64)
        found = []

        async def check(i):
            ip = f"{base}.{i}"
            if ip == self.ip:
                return
            async with sem:
                info = await self._probe(ip, self.port, timeout=1.0)
            if info and info.get("id") and info["id"] != self.id:
                self._upsert_peer(info["id"], info.get("name", ip), ip, self.port, f"scan:{ip}", online=True)
                found.append((info["id"], ip))

        await asyncio.gather(*[check(i) for i in range(1, 255)])
        self._save_known()
        await self.broadcast_peers()
        for pid, ip in found:
            if pid in self.peers:
                await self._post_peer(self.peers[pid], {"type": "hello", "from": self.name, "fromId": self.id})
        await self._broadcast({"type": "scandone", "found": len(found)})

    # ---------- browser fan-out ----------
    def _peer_list(self):
        return [{"id": p["id"], "name": p["name"], "ip": p.get("ip"), "port": p.get("port", self.port),
                 "online": p.get("online", True), "terminal": p.get("terminal", False)}
                for p in self.peers.values()]

    async def broadcast_peers(self):
        await self._broadcast({"type": "peers", "peers": self._peer_list()})

    async def _broadcast(self, data):
        msg = json.dumps(data)
        dead = set()
        for ws in self.browser_ws:
            try:
                await ws.send_str(msg)
            except Exception:
                dead.add(ws)
        self.browser_ws -= dead

    # ---------- http handlers ----------
    async def index(self, request):
        return web.FileResponse(BASE_DIR / "index.html")

    async def whoami(self, request):
        return web.json_response({"id": self.id, "name": self.name, "terminal": self.terminal_enabled})

    async def qr(self, request):
        try:
            import qrcode
            import qrcode.image.svg
            img = qrcode.make(f"http://{self.ip}:{self.port}", image_factory=qrcode.image.svg.SvgImage)
            buf = BytesIO()
            img.save(buf)
            return web.Response(body=buf.getvalue(), content_type="image/svg+xml")
        except Exception as e:
            return web.Response(status=501, text=f"QR unavailable: {e}")

    async def ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.browser_ws.add(ws)
        await ws.send_str(json.dumps({"type": "self", "name": self.name, "id": self.id,
                                      "ip": self.ip, "port": self.port, "autosave": self.autosave,
                                      "terminalEnabled": self.terminal_enabled,
                                      "terminalHasPin": bool(self.terminal_pin)}))
        await ws.send_str(json.dumps({"type": "peers", "peers": self._peer_list()}))
        await self._broadcast_shares()
        await self._broadcast_workspaces()
        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    await self._on_browser_msg(json.loads(msg.data))
        finally:
            self.browser_ws.discard(ws)
        return ws

    async def _on_browser_msg(self, data):
        action = data.get("action")
        if action == "addpeer":
            await self._add_peer_by_ip(data.get("ip"), int(data.get("port") or self.port))
            return
        if action == "removepeer":
            if self.peers.pop(data.get("to"), None):
                self._save_known()
                await self.broadcast_peers()
            return
        if action == "setname":
            nm = (data.get("name") or "").strip()
            if nm:
                self.name = nm
                try:
                    NAME_FILE.write_text(nm)
                except Exception:
                    pass
            return
        if action == "scan":
            asyncio.ensure_future(self.scan())
            return
        if action == "setautosave":
            self.autosave = bool(data.get("on"))
            self._save_config()
            return
        if action == "setterminal":
            self.terminal_enabled = bool(data.get("enabled"))
            pin = str(data.get("pin") or "").strip()
            self.terminal_pin = pin or None
            self._save_config()
            await self._broadcast({"type": "termstate", "enabled": self.terminal_enabled,
                                   "hasPin": bool(self.terminal_pin)})
            return
        if action == "reveal":
            self._reveal_folder()
            return
        if action == "clipsend":
            for pid in (data.get("toList") or [data.get("to")]):
                peer = self.peers.get(pid)
                if peer:
                    await self._send_clipboard(peer)
            return
        if action == "react":
            for pid in (data.get("toList") or [data.get("to")]):
                peer = self.peers.get(pid)
                if peer:
                    await self._post_peer(peer, {"type": "reaction", "msgId": data.get("msgId"),
                                                 "emoji": data.get("emoji"), "from": self.name, "fromId": self.id})
            return
        if action == "addshare":
            path = os.path.expanduser((data.get("path") or "").strip())
            if not path or not os.path.isdir(path):
                await self._broadcast({"type": "error", "text": f"Not a folder: {path}"})
                return
            sid = uuid.uuid4().hex[:12]
            self.shares[sid] = {"name": os.path.basename(path.rstrip("/")) or path, "root": str(Path(path).resolve())}
            self._save_shares()
            self._start_share_watch(sid)
            await self._broadcast_shares()
            await self._broadcast({"type": "toast", "text": f"Sharing {self.shares[sid]['name']}"})
            return
        if action == "removeshare":
            self._stop_share_watch(data.get("id"))
            self.shares.pop(data.get("id"), None)
            self._save_shares()
            await self._broadcast_shares()
            return
        if action == "listshares":
            peer = self.peers.get(data.get("to"))
            if peer:
                res = await self._remote_shares(peer, data.get("pin"))
                await self._broadcast({"type": "peershares", "peerId": peer["id"], **res})
            return
        if action == "openproject":
            await self._open_project(data)
            return
        if action == "closeproject":
            await self._close_project(data.get("id"))
            return
        peer = self.peers.get(data.get("to"))
        if not peer:
            await self._broadcast({"type": "status", "msgId": data.get("msgId"), "state": "failed"})
            return
        if action == "chat":
            await self._deliver_or_queue(peer, {
                "kind": "chat", "msgId": data.get("msgId"), "text": data.get("text"),
                "replyTo": data.get("replyTo"),
            })
        elif action == "file":
            meta = self.uploads.get(data.get("fileId"))
            if not meta:
                await self._broadcast({"type": "status", "msgId": data.get("msgId"), "state": "failed"})
                return
            await self._deliver_or_queue(peer, {
                "kind": "file", "msgId": data.get("msgId"),
                "name": meta["name"], "size": meta["size"], "path": meta["path"],
            })

    async def _add_peer_by_ip(self, ip, port, quiet=False):
        ip = (ip or "").strip()
        if not ip:
            return
        info = await self._probe(ip, port, timeout=5.0)
        if not info:
            if not quiet:
                await self._broadcast({"type": "error", "text": f"Can't reach {ip}:{port}"})
            return
        pid = info.get("id")
        if not pid:
            return
        if pid == self.id:
            await self._broadcast({"type": "error", "text": "That IP is this same Mac."})
            return
        self._upsert_peer(pid, info.get("name", ip), ip, port, f"manual:{ip}", online=True)
        self.peers[pid]["terminal"] = info.get("terminal", False)
        self._save_known()
        await self.broadcast_peers()
        await self._post_peer(self.peers[pid], {"type": "hello", "from": self.name, "fromId": self.id})
        await self._flush(self.peers[pid])

    async def _push_file(self, peer, meta, quiet=False):
        if not Path(meta["path"]).exists():
            return None
        url = f"http://{peer['ip']}:{peer['port']}/peer/upload"
        headers = {"X-Filename": urllib.parse.quote(meta["name"])}
        # total=None allows arbitrarily large files, but sock_read fails a
        # stalled connection instead of hanging forever at the clock icon.
        timeout = ClientTimeout(total=None, sock_connect=15, sock_read=120)
        try:
            with open(meta["path"], "rb") as f:
                async with ClientSession(timeout=timeout) as s:
                    async with s.post(url, data=f, headers=headers) as r:
                        if r.status == 200:
                            return (await r.json()).get("id")
        except Exception as e:
            if not quiet:
                await self._broadcast({"type": "error", "text": f"File send failed: {e}"})
        return None

    async def peer_upload(self, request):
        fid = uuid.uuid4().hex
        path = UPLOAD_DIR / fid
        name = urllib.parse.unquote(request.headers.get("X-Filename", "file"))
        size = 0
        with open(path, "wb") as f:
            async for chunk in request.content.iter_chunked(1024 * 256):
                size += len(chunk)
                f.write(chunk)
        self.uploads[fid] = {"path": str(path), "name": name, "size": size}
        if self.autosave:
            # copy to ~/Downloads in a worker thread so a large file doesn't
            # block the event loop (and delay this response to the sender).
            asyncio.get_event_loop().run_in_executor(None, self._autosave_copy, str(path), name)
        return web.json_response({"id": fid})

    @staticmethod
    def _safe_name(name):
        # strip any path components a peer might send
        name = Path(name).name or "file"
        return name.replace("/", "_").replace("\\", "_")

    def _autosave_copy(self, src, name):
        try:
            SAVE_DIR.mkdir(parents=True, exist_ok=True)
            base = self._safe_name(name)
            dest = SAVE_DIR / base
            if dest.exists():
                stem, suffix, i = dest.stem, dest.suffix, 1
                while dest.exists():
                    dest = SAVE_DIR / f"{stem} ({i}){suffix}"
                    i += 1
            shutil.copy2(src, dest)
        except Exception:
            pass

    def _reveal_folder(self):
        try:
            SAVE_DIR.mkdir(parents=True, exist_ok=True)
            if sys.platform == "darwin":
                subprocess.Popen(["open", str(SAVE_DIR)])
            elif sys.platform.startswith("linux"):
                subprocess.Popen(["xdg-open", str(SAVE_DIR)])
        except Exception:
            pass

    # ---------- clipboard sync (macOS) ----------
    def _clip_read(self):
        """Returns ('text', str) | ('image', path) | None."""
        try:
            out = subprocess.run(["pbpaste"], capture_output=True, timeout=5)
            text = out.stdout.decode("utf-8", errors="replace")
            if text.strip():
                return ("text", text)
        except Exception:
            pass
        try:
            tmp = UPLOAD_DIR / (uuid.uuid4().hex + ".png")
            script = ('set thePng to (the clipboard as «class PNGf»)\n'
                      f'set f to open for access POSIX file "{tmp}" with write permission\n'
                      'write thePng to f\nclose access f')
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            if tmp.exists() and tmp.stat().st_size > 0:
                return ("image", str(tmp))
        except Exception:
            pass
        return None

    def _clip_write_text(self, text):
        try:
            subprocess.run(["pbcopy"], input=(text or "").encode("utf-8"), timeout=5)
        except Exception:
            pass

    def _clip_write_image(self, path):
        try:
            script = f'set the clipboard to (read (POSIX file "{path}") as «class PNGf»)'
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
        except Exception:
            pass

    async def _send_clipboard(self, peer):
        clip = self._clip_read()
        if not clip:
            await self._broadcast({"type": "error", "text": "Clipboard is empty"})
            return
        kind, value = clip
        if kind == "text":
            ok = await self._post_peer(peer, {"type": "clip", "clipKind": "text",
                                              "from": self.name, "fromId": self.id, "text": value})
        else:
            meta = {"path": value, "name": "clipboard.png", "size": Path(value).stat().st_size}
            remote_id = await self._push_file(peer, meta)
            ok = False
            if remote_id:
                ok = await self._post_peer(peer, {"type": "clip", "clipKind": "image",
                                                  "from": self.name, "fromId": self.id, "fileId": remote_id})
        await self._broadcast({"type": "toast",
                               "text": f"Clipboard sent to {peer['name']}" if ok else f"Could not reach {peer['name']}"})

    async def _post_peer(self, peer, payload, quiet=False):
        payload = {**payload, "fromIp": self.ip, "fromPort": self.port}
        url = f"http://{peer['ip']}:{peer['port']}/peer/message"
        try:
            async with ClientSession(timeout=ClientTimeout(total=10)) as s:
                async with s.post(url, json=payload) as r:
                    return r.status == 200
        except Exception as e:
            if not quiet:
                await self._broadcast({"type": "error", "text": f"Could not reach {peer['name']}: {e}"})
            return False

    # ---------- store-and-forward ----------
    def _save_outbox(self):
        try:
            OUTBOX_FILE.write_text(json.dumps(self.outbox))
        except Exception:
            pass

    def _enqueue(self, peer, item):
        self.outbox.setdefault(peer["ip"], []).append({**item, "_port": peer.get("port", self.port)})
        self._save_outbox()

    async def _try_deliver(self, peer, item, quiet=True):
        """Attempt one delivery. Returns True on success."""
        if not peer.get("online", True):
            return False
        if item["kind"] == "chat":
            return await self._post_peer(peer, {
                "type": "chat", "from": self.name, "fromId": self.id,
                "text": item.get("text"), "msgId": item.get("msgId"),
                "replyTo": item.get("replyTo"),
            }, quiet=quiet)
        # file
        remote_id = await self._push_file(peer, {
            "path": item["path"], "name": item["name"], "size": item["size"],
        }, quiet=quiet)
        if not remote_id:
            return False
        return await self._post_peer(peer, {
            "type": "file", "from": self.name, "fromId": self.id,
            "name": item["name"], "size": item["size"],
            "url": f"/download/{remote_id}", "msgId": item.get("msgId"),
        }, quiet=quiet)

    async def _deliver_or_queue(self, peer, item):
        ok = await self._try_deliver(peer, item, quiet=False)
        if ok:
            await self._broadcast({"type": "status", "msgId": item.get("msgId"), "state": "sent"})
        else:
            self._enqueue(peer, item)
            await self._broadcast({"type": "status", "msgId": item.get("msgId"), "state": "queued"})

    async def _flush(self, peer):
        ip = peer["ip"]
        if ip in self._flushing or ip not in self.outbox:
            return
        self._flushing.add(ip)
        try:
            remaining = []
            for item in self.outbox.get(ip, []):
                ok = await self._try_deliver(peer, item, quiet=True)
                if ok:
                    await self._broadcast({"type": "status", "msgId": item.get("msgId"), "state": "sent"})
                else:
                    remaining.append(item)
            if remaining:
                self.outbox[ip] = remaining
            else:
                self.outbox.pop(ip, None)
            self._save_outbox()
        finally:
            self._flushing.discard(ip)

    async def pending(self, request):
        # A peer pulls items we queued for it. Works even when WE can't reach
        # THEM (e.g. their firewall blocks our outbound) — they reach us.
        rip = request.remote
        items = self.outbox.pop(rip, [])
        if items:
            self._save_outbox()
        out = []
        for item in items:
            if item.get("kind") == "chat":
                out.append({"type": "chat", "from": self.name, "fromId": self.id,
                            "text": item.get("text"), "msgId": item.get("msgId"),
                            "replyTo": item.get("replyTo")})
            elif item.get("kind") == "file":
                if not Path(item["path"]).exists():
                    continue
                fid = uuid.uuid4().hex
                self.uploads[fid] = {"path": item["path"], "name": item["name"], "size": item["size"]}
                out.append({"type": "file", "from": self.name, "fromId": self.id,
                            "name": item["name"], "size": item["size"],
                            "url": f"http://{self.ip}:{self.port}/download/{fid}", "msgId": item.get("msgId")})
            if item.get("msgId"):
                await self._broadcast({"type": "status", "msgId": item["msgId"], "state": "delivered"})
        return web.json_response({"items": out, "fromId": self.id, "name": self.name,
                                  "ip": self.ip, "port": self.port})

    async def poll_pending(self):
        # Pull anything peers have queued for us (the firewall-friendly direction).
        await asyncio.sleep(3)
        while True:
            for peer in list(self.peers.values()):
                url = f"http://{peer['ip']}:{peer['port']}/pending?for={self.id}"
                try:
                    async with ClientSession(timeout=ClientTimeout(total=8)) as s:
                        async with s.get(url) as r:
                            if r.status != 200:
                                continue
                            data = await r.json()
                except Exception:
                    continue
                for item in data.get("items", []):
                    await self._receive_pulled(item, peer)
            await asyncio.sleep(4)

    async def _receive_pulled(self, item, peer):
        if item.get("type") == "chat":
            await self._broadcast({**item, "fromIp": peer["ip"], "fromPort": peer["port"]})
            return
        if item.get("type") == "file":
            fid = uuid.uuid4().hex
            path = UPLOAD_DIR / fid
            try:
                async with ClientSession(timeout=ClientTimeout(total=None, sock_connect=15, sock_read=120)) as s:
                    async with s.get(item["url"]) as r:
                        if r.status != 200:
                            return
                        with open(path, "wb") as f:
                            async for chunk in r.content.iter_chunked(1024 * 256):
                                f.write(chunk)
            except Exception:
                return
            size = path.stat().st_size
            self.uploads[fid] = {"path": str(path), "name": item["name"], "size": size}
            if self.autosave:
                asyncio.get_event_loop().run_in_executor(None, self._autosave_copy, str(path), item["name"])
            await self._broadcast({"type": "file", "from": item.get("from"), "fromId": item.get("fromId"),
                                   "name": item["name"], "size": size, "url": f"/download/{fid}",
                                   "msgId": item.get("msgId"), "fromIp": peer["ip"], "fromPort": peer["port"]})

    async def peer_message(self, request):
        data = await request.json()
        fid = data.get("fromId")
        mtype = data.get("type")
        if fid and fid != self.id and data.get("fromIp") and fid not in self.peers:
            self._upsert_peer(fid, data.get("from", "Unknown"), data["fromIp"],
                              int(data.get("fromPort") or self.port), f"manual:{data['fromIp']}", online=True)
            self._save_known()
            await self.broadcast_peers()
            asyncio.ensure_future(self._flush(self.peers[fid]))
        if mtype == "hello":
            return web.json_response({"ok": True})
        if mtype == "ack":
            await self._broadcast({"type": "status", "msgId": data.get("ackId"), "state": "delivered"})
            return web.json_response({"ok": True})
        if mtype == "reaction":
            await self._broadcast(data)
            return web.json_response({"ok": True})
        if mtype == "clip":
            if data.get("clipKind") == "text":
                self._clip_write_text(data.get("text", ""))
            elif data.get("clipKind") == "image":
                meta = self.uploads.get(data.get("fileId"))
                if meta:
                    self._clip_write_image(meta["path"])
            await self._broadcast({"type": "clip", "from": data.get("from", "Someone"),
                                   "clipKind": data.get("clipKind")})
            return web.json_response({"ok": True})
        await self._broadcast(data)
        # send delivery ack back to the sender
        if data.get("msgId") and fid and fid in self.peers:
            asyncio.ensure_future(self._post_peer(
                self.peers[fid], {"type": "ack", "ackId": data["msgId"], "from": self.name, "fromId": self.id}))
        return web.json_response({"ok": True})

    async def upload(self, request):
        reader = await request.multipart()
        field = await reader.next()
        filename = field.filename or "file"
        fid = uuid.uuid4().hex
        path = UPLOAD_DIR / fid
        size = 0
        with open(path, "wb") as f:
            while True:
                chunk = await field.read_chunk(1024 * 256)
                if not chunk:
                    break
                size += len(chunk)
                f.write(chunk)
        self.uploads[fid] = {"path": str(path), "name": filename, "size": size}
        return web.json_response({"id": fid, "name": filename, "size": size})

    async def termws(self, request):
        # Remote shell — OFF by default, PIN-gated. Runs on THIS machine.
        if not self.terminal_enabled:
            return web.Response(status=403, text="Terminal disabled on this device")
        if self.terminal_pin and request.query.get("pin") != self.terminal_pin:
            return web.Response(status=403, text="Incorrect PIN")
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        master, slave = os.openpty()
        shell = os.environ.get("SHELL", "/bin/zsh")
        env = {**os.environ, "TERM": "xterm-256color"}
        try:
            proc = await asyncio.create_subprocess_exec(
                shell, "-l", stdin=slave, stdout=slave, stderr=slave,
                preexec_fn=os.setsid, env=env)
        except Exception as e:
            os.close(master)
            os.close(slave)
            await ws.send_str(f"\r\nFailed to start shell: {e}\r\n")
            await ws.close()
            return ws
        os.close(slave)
        os.set_blocking(master, False)
        loop = asyncio.get_event_loop()

        def on_master_read():
            try:
                data = os.read(master, 65536)
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                data = b""
            if data:
                asyncio.ensure_future(ws.send_bytes(data))
            else:
                try:
                    loop.remove_reader(master)
                except Exception:
                    pass

        loop.add_reader(master, on_master_read)
        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.BINARY:
                    os.write(master, msg.data)
                elif msg.type == web.WSMsgType.TEXT:
                    try:
                        d = json.loads(msg.data)
                        if isinstance(d, dict) and "resize" in d:
                            cols, rows = d["resize"]
                            fcntl.ioctl(master, termios.TIOCSWINSZ,
                                        struct.pack("HHHH", int(rows), int(cols), 0, 0))
                    except Exception:
                        pass
        finally:
            try:
                loop.remove_reader(master)
            except Exception:
                pass
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                os.close(master)
            except Exception:
                pass
        return ws

    # ---------- file shares (host side) ----------
    def _save_shares(self):
        try:
            SHARES_FILE.write_text(json.dumps(self.shares))
        except Exception:
            pass

    def _save_workspaces(self):
        try:
            WORKSPACES_FILE.write_text(json.dumps(self.workspaces))
        except Exception:
            pass

    def _fs_auth(self, request):
        return not self.terminal_pin or request.query.get("pin") == self.terminal_pin

    def _share_root(self, sid):
        sh = self.shares.get(sid or "")
        return Path(sh["root"]) if sh else None

    @staticmethod
    def _resolve(root, rel):
        rel = (rel or "").lstrip("/")
        try:
            target = (root / rel).resolve()
        except Exception:
            return None
        root_r = root.resolve()
        if target == root_r or str(target).startswith(str(root_r) + os.sep):
            return target
        return None

    async def shares_list(self, request):
        if not self._fs_auth(request):
            return web.json_response({"error": "pin"}, status=403)
        return web.json_response({"shares": [{"id": k, "name": v["name"]} for k, v in self.shares.items()]})

    async def fs_version(self, request):
        if not self._fs_auth(request):
            return web.Response(status=403)
        sid = request.query.get("share")
        if sid not in self.shares:
            return web.Response(status=404)
        return web.json_response({"version": self.share_versions.get(sid, 0)})

    async def fs_manifest(self, request):
        if not self._fs_auth(request):
            return web.Response(status=403)
        import remotefs
        root = self._share_root(request.query.get("share"))
        if not root or not root.is_dir():
            return web.Response(status=404)
        self._touch_share(request.query.get("share"), "reads")
        files = {}
        for p in root.rglob("*"):
            if p.is_file():
                rel = p.relative_to(root).as_posix()
                try:
                    st = p.stat()
                except OSError:
                    continue
                if remotefs.is_ignored(rel, st.st_size):
                    continue
                files[rel] = [int(st.st_mtime), st.st_size]
        return web.json_response({"files": files})

    async def fs_list(self, request):
        if not self._fs_auth(request):
            return web.Response(status=403)
        root = self._share_root(request.query.get("share"))
        target = self._resolve(root, request.query.get("path", "")) if root else None
        if not target or not target.exists():
            return web.Response(status=404)
        entries = []
        if target.is_dir():
            for c in sorted(target.iterdir()):
                try:
                    st = c.stat()
                except OSError:
                    continue
                entries.append({"name": c.name, "dir": c.is_dir(), "size": st.st_size, "mtime": int(st.st_mtime)})
        return web.json_response({"entries": entries})

    async def fs_stat(self, request):
        if not self._fs_auth(request):
            return web.Response(status=403)
        root = self._share_root(request.query.get("share"))
        target = self._resolve(root, request.query.get("path", "")) if root else None
        if not target or not target.exists():
            return web.Response(status=404)
        st = target.stat()
        return web.json_response({"dir": target.is_dir(), "size": st.st_size, "mtime": int(st.st_mtime)})

    async def fs_read(self, request):
        if not self._fs_auth(request):
            return web.Response(status=403)
        root = self._share_root(request.query.get("share"))
        target = self._resolve(root, request.query.get("path", "")) if root else None
        if not target or not target.is_file():
            return web.Response(status=404)
        self._touch_share(request.query.get("share"), "reads")
        return web.FileResponse(target)

    async def fs_write(self, request):
        if not self._fs_auth(request):
            return web.Response(status=403)
        root = self._share_root(request.query.get("share"))
        target = self._resolve(root, request.query.get("path", "")) if root else None
        if not target:
            return web.Response(status=400)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "wb") as f:
            async for chunk in request.content.iter_chunked(1024 * 256):
                f.write(chunk)
        sid = request.query.get("share")
        self._touch_share(sid, "writes")
        self.share_versions[sid] = time.time()
        st = target.stat()
        return web.json_response({"mtime": int(st.st_mtime), "size": st.st_size})

    async def fs_mkdir(self, request):
        if not self._fs_auth(request):
            return web.Response(status=403)
        root = self._share_root(request.query.get("share"))
        target = self._resolve(root, request.query.get("path", "")) if root else None
        if not target:
            return web.Response(status=400)
        target.mkdir(parents=True, exist_ok=True)
        return web.json_response({"ok": True})

    async def fs_delete(self, request):
        if not self._fs_auth(request):
            return web.Response(status=403)
        root = self._share_root(request.query.get("share"))
        target = self._resolve(root, request.query.get("path", "")) if root else None
        if target and target.exists():
            try:
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
            except OSError:
                pass
        return web.json_response({"ok": True})

    # ---------- workspaces (client side) ----------
    async def _broadcast_shares(self):
        now = time.time()
        shares = []
        for k, v in self.shares.items():
            a = self.share_activity.get(k, {})
            shares.append({"id": k, "name": v["name"], "root": v["root"],
                           "active": bool(a.get("last") and now - a["last"] < 15),
                           "writes": a.get("writes", 0), "reads": a.get("reads", 0)})
        await self._broadcast({"type": "myshares", "shares": shares})

    async def _broadcast_workspaces(self):
        out = []
        for w in self.workspaces.values():
            sess = self.sync_sessions.get(w["id"])
            status = sess.status if sess else ("mounted" if w["id"] in self.fuse_stops else "stopped")
            out.append({**w, "status": status})
        await self._broadcast({"type": "workspaces", "workspaces": out})

    async def _remote_shares(self, peer, pin):
        url = f"http://{peer['ip']}:{peer['port']}/shares"
        if pin:
            url += "?pin=" + urllib.parse.quote(pin)
        try:
            async with ClientSession(timeout=ClientTimeout(total=8)) as s:
                async with s.get(url) as r:
                    if r.status == 403:
                        return {"error": "pin"}
                    if r.status == 200:
                        return await r.json()
        except Exception:
            pass
        return {"error": "unreachable"}

    async def _open_project(self, data):
        import remotefs
        peer = self.peers.get(data.get("to"))
        if not peer:
            return
        name = self._safe_name(data.get("name") or "project")
        mode = data.get("mode", "sync")
        pin = data.get("pin", "")
        share_id = data.get("shareId")
        wsid = uuid.uuid4().hex[:8]
        local = WS_ROOT / name
        # replace any existing workspace for the same local folder (no duplicates)
        for wid, w in list(self.workspaces.items()):
            if w.get("local") == str(local):
                await self._close_project(wid)
        base = f"http://{peer['ip']}:{peer['port']}"
        try:
            if mode == "fuse":
                stop = remotefs.mount_fuse(base, share_id, pin, str(local))
                self.fuse_stops[wsid] = stop
            else:
                sess = remotefs.SyncSession(base, share_id, pin, str(local),
                                            log=lambda m, n=name: self._broadcast({"type": "toast", "text": f"[{n}] {m}"}))
                await sess.start()
                self.sync_sessions[wsid] = sess
        except Exception as e:
            await self._broadcast({"type": "error", "text": str(e)})
            return
        self.workspaces[wsid] = {"id": wsid, "name": name, "mode": mode, "local": str(local),
                                 "peer": peer["id"], "peerName": peer["name"], "shareId": share_id, "pin": pin}
        self._save_workspaces()
        await self._broadcast_workspaces()
        await self._broadcast({"type": "toast", "text": f"Project ready — cd {local}"})

    async def _close_project(self, wsid):
        sess = self.sync_sessions.pop(wsid, None)
        if sess:
            await sess.stop()
        stop = self.fuse_stops.pop(wsid, None)
        if stop:
            try:
                stop()
            except Exception:
                pass
        self.workspaces.pop(wsid, None)
        self._save_workspaces()
        await self._broadcast_workspaces()

    async def resume_workspaces(self):
        import remotefs
        for w in list(self.workspaces.values()):
            peer = self.peers.get(w.get("peer"))
            ip = peer["ip"] if peer else None
            port = peer["port"] if peer else None
            if not ip:
                # fall back to any known address for that peer name later; skip for now
                continue
            base = f"http://{ip}:{port}"
            try:
                if w["mode"] == "sync":
                    sess = remotefs.SyncSession(base, w["shareId"], w.get("pin", ""), w["local"],
                                                log=lambda m, n=w["name"]: self._broadcast({"type": "toast", "text": f"[{n}] {m}"}))
                    await sess.start()
                    self.sync_sessions[w["id"]] = sess
            except Exception:
                pass

    async def upload_folder(self, request):
        reader = await request.multipart()
        workdir = Path(tempfile.mkdtemp(prefix="ldfolder_"))
        folder_name = "folder"
        try:
            while True:
                field = await reader.next()
                if field is None:
                    break
                if field.name == "folderName":
                    folder_name = (await field.text()).strip() or "folder"
                    continue
                rel = field.filename or uuid.uuid4().hex
                # prevent path escape; keep it inside workdir
                dest = (workdir / rel).resolve()
                if not str(dest).startswith(str(workdir.resolve())):
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    while True:
                        chunk = await field.read_chunk(1024 * 256)
                        if not chunk:
                            break
                        f.write(chunk)
            fid = uuid.uuid4().hex
            zip_path = UPLOAD_DIR / fid
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
                for p in workdir.rglob("*"):
                    if p.is_file():
                        z.write(p, p.relative_to(workdir))
            name = self._safe_name(folder_name) + ".zip"
            size = zip_path.stat().st_size
            self.uploads[fid] = {"path": str(zip_path), "name": name, "size": size}
            return web.json_response({"id": fid, "name": name, "size": size})
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    async def download(self, request):
        fid = request.match_info["id"]
        meta = self.uploads.get(fid)
        if not meta or not Path(meta["path"]).exists():
            return web.Response(status=404, text="Not found")
        return web.FileResponse(meta["path"], headers={
            "Content-Disposition": f'attachment; filename="{meta["name"]}"'
        })


async def serve(name, port, announce=True):
    app_obj = App(name, port)
    web_app = web.Application(client_max_size=1024 ** 4)  # effectively unlimited (1 TB)
    web_app.add_routes([
        web.get("/", app_obj.index),
        web.get("/whoami", app_obj.whoami),
        web.get("/qr", app_obj.qr),
        web.get("/ws", app_obj.ws_handler),
        web.post("/peer/message", app_obj.peer_message),
        web.get("/pending", app_obj.pending),
        web.post("/peer/upload", app_obj.peer_upload),
        web.post("/upload", app_obj.upload),
        web.post("/uploadfolder", app_obj.upload_folder),
        web.get("/download/{id}", app_obj.download),
        web.get("/termws", app_obj.termws),
        web.get("/shares", app_obj.shares_list),
        web.get("/fs/manifest", app_obj.fs_manifest),
        web.get("/fs/list", app_obj.fs_list),
        web.get("/fs/stat", app_obj.fs_stat),
        web.get("/fs/read", app_obj.fs_read),
        web.post("/fs/write", app_obj.fs_write),
        web.post("/fs/mkdir", app_obj.fs_mkdir),
        web.post("/fs/delete", app_obj.fs_delete),
        web.get("/fs/version", app_obj.fs_version),
    ])
    vendor_dir = BASE_DIR / "vendor"
    if vendor_dir.is_dir():
        web_app.router.add_static("/vendor/", path=str(vendor_dir))
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await app_obj.start_discovery()
    await app_obj.reconnect_known()
    asyncio.ensure_future(app_obj.heartbeat())
    asyncio.ensure_future(app_obj.poll_pending())
    asyncio.ensure_future(app_obj.resume_workspaces())
    app_obj.start_share_watchers()

    if announce:
        print(f"\n  Lan Drop  —  '{app_obj.name}'")
        print(f"  Open this Mac:  http://localhost:{port}")
        print(f"  Other devices:  http://{app_obj.ip}:{port}")
        print("  Press Ctrl+C to stop.\n")
    await asyncio.Event().wait()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=socket.gethostname())
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    await serve(args.name, args.port)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n  Stopped.")
