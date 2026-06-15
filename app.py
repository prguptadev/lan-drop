import argparse
import asyncio
import json
import socket
import urllib.parse
import uuid
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
            for p in list(self.peers.values()):
                info = await self._probe(p["ip"], p["port"])
                if info:
                    rid = info.get("id")
                    nm = info.get("name", p["name"])
                    if rid and rid != p["id"]:
                        self._upsert_peer(rid, nm, p["ip"], p["port"], p.get("_sname", "manual"), online=True)
                        changed = True
                    else:
                        if not p.get("online") or p["name"] != nm:
                            changed = True
                        p["online"] = True
                        p["name"] = nm
                else:
                    if p.get("online"):
                        changed = True
                    p["online"] = False
            if changed:
                await self.broadcast_peers()

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
        return [{"id": p["id"], "name": p["name"], "ip": p.get("ip"), "online": p.get("online", True)}
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
        return web.json_response({"id": self.id, "name": self.name})

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
                                      "ip": self.ip, "port": self.port}))
        await ws.send_str(json.dumps({"type": "peers", "peers": self._peer_list()}))
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
        peer = self.peers.get(data.get("to"))
        if not peer:
            await self._broadcast({"type": "status", "msgId": data.get("msgId"), "state": "failed"})
            return
        if action == "chat":
            ok = await self._post_peer(peer, {
                "type": "chat", "from": self.name, "fromId": self.id,
                "text": data.get("text"), "msgId": data.get("msgId"),
            })
            await self._broadcast({"type": "status", "msgId": data.get("msgId"),
                                   "state": "sent" if ok else "failed"})
        elif action == "file":
            meta = self.uploads.get(data.get("fileId"))
            if not meta:
                return
            # push the bytes to the peer (same direction as chat), then point its
            # browser at its own local copy — no reverse connection needed.
            remote_id = await self._push_file(peer, meta)
            ok = False
            if remote_id:
                ok = await self._post_peer(peer, {
                    "type": "file", "from": self.name, "fromId": self.id,
                    "name": meta["name"], "size": meta["size"],
                    "url": f"/download/{remote_id}", "msgId": data.get("msgId"),
                })
            await self._broadcast({"type": "status", "msgId": data.get("msgId"),
                                   "state": "sent" if ok else "failed"})

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
        self._save_known()
        await self.broadcast_peers()
        await self._post_peer(self.peers[pid], {"type": "hello", "from": self.name, "fromId": self.id})

    async def _push_file(self, peer, meta):
        url = f"http://{peer['ip']}:{peer['port']}/peer/upload"
        headers = {"X-Filename": urllib.parse.quote(meta["name"])}
        try:
            with open(meta["path"], "rb") as f:
                async with ClientSession(timeout=ClientTimeout(total=None)) as s:
                    async with s.post(url, data=f, headers=headers) as r:
                        if r.status == 200:
                            return (await r.json()).get("id")
        except Exception as e:
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
        return web.json_response({"id": fid})

    async def _post_peer(self, peer, payload):
        payload = {**payload, "fromIp": self.ip, "fromPort": self.port}
        url = f"http://{peer['ip']}:{peer['port']}/peer/message"
        try:
            async with ClientSession(timeout=ClientTimeout(total=10)) as s:
                async with s.post(url, json=payload) as r:
                    return r.status == 200
        except Exception as e:
            await self._broadcast({"type": "error", "text": f"Could not reach {peer['name']}: {e}"})
            return False

    async def peer_message(self, request):
        data = await request.json()
        fid = data.get("fromId")
        mtype = data.get("type")
        if fid and fid != self.id and data.get("fromIp") and fid not in self.peers:
            self._upsert_peer(fid, data.get("from", "Unknown"), data["fromIp"],
                              int(data.get("fromPort") or self.port), f"manual:{data['fromIp']}", online=True)
            self._save_known()
            await self.broadcast_peers()
        if mtype == "hello":
            return web.json_response({"ok": True})
        if mtype == "ack":
            await self._broadcast({"type": "status", "msgId": data.get("ackId"), "state": "delivered"})
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
    web_app = web.Application(client_max_size=0)  # unlimited upload size
    web_app.add_routes([
        web.get("/", app_obj.index),
        web.get("/whoami", app_obj.whoami),
        web.get("/qr", app_obj.qr),
        web.get("/ws", app_obj.ws_handler),
        web.post("/peer/message", app_obj.peer_message),
        web.post("/peer/upload", app_obj.peer_upload),
        web.post("/upload", app_obj.upload),
        web.get("/download/{id}", app_obj.download),
    ])
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await app_obj.start_discovery()
    await app_obj.reconnect_known()
    asyncio.ensure_future(app_obj.heartbeat())

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
