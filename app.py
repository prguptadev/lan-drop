import argparse
import asyncio
import json
import socket
import uuid
from pathlib import Path

from aiohttp import web, ClientSession, ClientTimeout
from zeroconf import ServiceInfo, ServiceStateChange
from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser, AsyncServiceInfo

SERVICE_TYPE = "_tridentdrop._tcp.local."
BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)
NAME_FILE = BASE_DIR / ".displayname"


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
        self.peers = {}        # id -> {id, name, ip, port, _sname}
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
            gone = [pid for pid, p in self.peers.items() if p.get("_sname") == name]
            for pid in gone:
                self.peers.pop(pid, None)
            if gone:
                await self.broadcast_peers()
            return

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
        self._upsert_peer(pid, props.get("name", "Unknown"), ip, info.port, name)
        await self.broadcast_peers()

    def _upsert_peer(self, pid, name, ip, port, sname):
        # drop stale entries for the same ip (e.g. the peer restarted with a new id)
        for old in [k for k, v in self.peers.items() if k != pid and v.get("ip") == ip]:
            self.peers.pop(old, None)
        self.peers[pid] = {"id": pid, "name": name, "ip": ip, "port": port, "_sname": sname}

    # ---------- browser fan-out ----------
    def _peer_list(self):
        return [{"id": p["id"], "name": p["name"], "ip": p.get("ip")} for p in self.peers.values()]

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

    async def ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.browser_ws.add(ws)
        await ws.send_str(json.dumps({"type": "self", "name": self.name, "id": self.id}))
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
        peer = self.peers.get(data.get("to"))
        if not peer:
            return
        if action == "chat":
            await self._post_peer(peer, {
                "type": "chat", "from": self.name, "fromId": self.id,
                "text": data.get("text"),
            })
        elif action == "file":
            meta = self.uploads.get(data.get("fileId"))
            if not meta:
                return
            url = f"http://{self.ip}:{self.port}/download/{data['fileId']}"
            await self._post_peer(peer, {
                "type": "file", "from": self.name, "fromId": self.id,
                "name": meta["name"], "size": meta["size"], "url": url,
            })

    async def _add_peer_by_ip(self, ip, port):
        ip = (ip or "").strip()
        if not ip:
            return
        url = f"http://{ip}:{port}/whoami"
        try:
            async with ClientSession(timeout=ClientTimeout(total=5)) as s:
                async with s.get(url) as r:
                    info = await r.json()
        except Exception as e:
            await self._broadcast({"type": "error", "text": f"Can't reach {ip}:{port} — {e}"})
            return
        pid = info.get("id")
        if not pid:
            return
        if pid == self.id:
            await self._broadcast({"type": "error", "text": "That IP is this same Mac."})
            return
        self._upsert_peer(pid, info.get("name", ip), ip, port, f"manual:{ip}")
        await self.broadcast_peers()
        # tell the other side who we are so it adds us back (bidirectional)
        await self._post_peer(self.peers[pid], {"type": "hello", "from": self.name, "fromId": self.id})

    async def whoami(self, request):
        return web.json_response({"id": self.id, "name": self.name})

    async def _post_peer(self, peer, payload):
        payload = {**payload, "fromIp": self.ip, "fromPort": self.port}
        url = f"http://{peer['ip']}:{peer['port']}/peer/message"
        try:
            async with ClientSession(timeout=ClientTimeout(total=10)) as s:
                await s.post(url, json=payload)
        except Exception as e:
            await self._broadcast({"type": "error", "text": f"Could not reach {peer['name']}: {e}"})

    async def peer_message(self, request):
        data = await request.json()
        # auto-learn the sender as a peer so replies work both ways
        fid = data.get("fromId")
        if fid and fid != self.id and data.get("fromIp") and fid not in self.peers:
            self._upsert_peer(fid, data.get("from", "Unknown"), data["fromIp"],
                              int(data.get("fromPort") or self.port), f"manual:{data['fromIp']}")
            await self.broadcast_peers()
        if data.get("type") == "hello":
            return web.json_response({"ok": True})  # handshake only, don't show in chat
        await self._broadcast(data)
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
    web_app = web.Application(client_max_size=0)  # 0 = unlimited upload size
    web_app.add_routes([
        web.get("/", app_obj.index),
        web.get("/whoami", app_obj.whoami),
        web.get("/ws", app_obj.ws_handler),
        web.post("/peer/message", app_obj.peer_message),
        web.post("/upload", app_obj.upload),
        web.get("/download/{id}", app_obj.download),
    ])
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await app_obj.start_discovery()

    if announce:
        print(f"\n  Trident Drop  —  '{app_obj.name}'")
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
