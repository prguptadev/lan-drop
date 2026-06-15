"""Remote project bridge for Lan Drop.

Two ways to let a peer's CLI work on this-or-that machine's project:
  * SyncSession  - mirror a remote shared folder locally and keep it in sync
                   two-way (no extra software needed).
  * mount_fuse   - mount the remote folder live via FUSE (needs macFUSE +
                   fusepy; nothing is copied to disk).

The "host" side (the Mac that owns the files) exposes a small HTTP file API
(/fs/*) implemented in app.py. This module is the "client" side that consumes
that API.
"""
import asyncio
import os
import urllib.parse
from pathlib import Path

from aiohttp import ClientSession, ClientTimeout


def _sig(path: Path):
    try:
        st = path.stat()
        return (int(st.st_mtime), st.st_size)
    except OSError:
        return None


def local_manifest(root: Path):
    """rel-path -> (mtime, size) for every file under root."""
    out = {}
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            s = _sig(p)
            if s:
                out[rel] = s
    return out


class SyncSession:
    """Two-way folder sync against a peer's /fs API.

    Change detection is done against a per-side baseline so we never compare
    mtimes across two machines (which is unreliable). A file is "changed on
    host" when its host signature differs from the last host baseline, and
    "changed locally" when its local signature differs from the last local
    baseline. Conflicts (both changed) resolve host-wins and are logged.
    """

    INTERVAL = 2.0

    def __init__(self, base_url, share, pin, local_dir, log=None):
        self.base = base_url.rstrip("/")
        self.share = share
        self.pin = pin or ""
        self.local = Path(local_dir)
        self.log = log or (lambda m: None)
        self.base_host = {}   # rel -> (mtime, size) last seen on host
        self.base_local = {}  # rel -> (mtime, size) last seen locally
        self._task = None
        self._stop = False
        self.status = "starting"

    # ---- lifecycle ----
    async def start(self):
        self.local.mkdir(parents=True, exist_ok=True)
        self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()

    async def _run(self):
        try:
            await self._initial()
            self.status = "synced"
            while not self._stop:
                await asyncio.sleep(self.INTERVAL)
                try:
                    await self._cycle()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.status = "error"
                    await self._emit(f"sync error: {e}")
        except asyncio.CancelledError:
            pass

    async def _emit(self, msg):
        r = self.log(msg)
        if asyncio.iscoroutine(r):
            await r

    # ---- http helpers ----
    def _url(self, ep, **params):
        params["share"] = self.share
        if self.pin:
            params["pin"] = self.pin
        return f"{self.base}/fs/{ep}?" + urllib.parse.urlencode(params)

    async def _get_manifest(self):
        async with ClientSession(timeout=ClientTimeout(total=30)) as s:
            async with s.get(self._url("manifest")) as r:
                if r.status != 200:
                    raise RuntimeError(f"host returned {r.status}")
                data = await r.json()
        return {k: tuple(v) for k, v in data.get("files", {}).items()}

    async def _pull(self, rel):
        dest = self.local / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        async with ClientSession(timeout=ClientTimeout(total=None)) as s:
            async with s.get(self._url("read", path=rel)) as r:
                if r.status != 200:
                    raise RuntimeError(f"read {rel}: {r.status}")
                with open(dest, "wb") as f:
                    async for chunk in r.content.iter_chunked(1024 * 256):
                        f.write(chunk)

    async def _push(self, rel):
        src = self.local / rel
        async with ClientSession(timeout=ClientTimeout(total=None)) as s:
            with open(src, "rb") as f:
                async with s.post(self._url("write", path=rel), data=f) as r:
                    if r.status != 200:
                        raise RuntimeError(f"write {rel}: {r.status}")
                    return await r.json()

    async def _del_host(self, rel):
        async with ClientSession(timeout=ClientTimeout(total=15)) as s:
            await s.post(self._url("delete", path=rel))

    # ---- sync logic ----
    async def _initial(self):
        host = await self._get_manifest()
        loc = local_manifest(self.local)
        for rel, hsig in host.items():
            if rel not in loc:
                await self._pull(rel)
        # adopt baselines
        loc = local_manifest(self.local)
        self.base_host = dict(host)
        self.base_local = {rel: loc.get(rel) for rel in host}
        # push any purely-local files that the host doesn't have
        for rel in list(loc):
            if rel not in host:
                meta = await self._push(rel)
                self.base_host[rel] = (int(meta.get("mtime", 0)), int(meta.get("size", 0)))
                self.base_local[rel] = loc[rel]
        await self._emit(f"initial sync done ({len(self.base_host)} files)")

    async def _cycle(self):
        host = await self._get_manifest()
        loc = local_manifest(self.local)
        rels = set(host) | set(loc) | set(self.base_host) | set(self.base_local)
        pulled = pushed = deleted = 0
        for rel in rels:
            h = host.get(rel)
            l = loc.get(rel)
            h_changed = h != self.base_host.get(rel)
            l_changed = l != self.base_local.get(rel)
            if h and l:
                if h_changed and not l_changed:
                    await self._pull(rel); pulled += 1
                    self.base_host[rel] = h; self.base_local[rel] = _sig(self.local / rel)
                elif l_changed and not h_changed:
                    meta = await self._push(rel); pushed += 1
                    self.base_host[rel] = (int(meta.get("mtime", 0)), int(meta.get("size", 0)))
                    self.base_local[rel] = l
                elif h_changed and l_changed:
                    await self._emit(f"conflict on {rel} — keeping host copy")
                    await self._pull(rel); pulled += 1
                    self.base_host[rel] = h; self.base_local[rel] = _sig(self.local / rel)
            elif h and not l:
                if self.base_local.get(rel) is not None:
                    await self._del_host(rel); deleted += 1
                    self.base_host.pop(rel, None); self.base_local.pop(rel, None)
                else:
                    await self._pull(rel); pulled += 1
                    self.base_host[rel] = h; self.base_local[rel] = _sig(self.local / rel)
            elif l and not h:
                if self.base_host.get(rel) is not None:
                    p = self.local / rel
                    try:
                        p.unlink()
                    except OSError:
                        pass
                    deleted += 1
                    self.base_host.pop(rel, None); self.base_local.pop(rel, None)
                else:
                    meta = await self._push(rel); pushed += 1
                    self.base_host[rel] = (int(meta.get("mtime", 0)), int(meta.get("size", 0)))
                    self.base_local[rel] = l
            else:
                self.base_host.pop(rel, None); self.base_local.pop(rel, None)
        if pulled or pushed or deleted:
            await self._emit(f"synced (+{pulled} ↓ {pushed} ↑ {deleted} ✕)")


def mount_fuse(base_url, share, pin, mountpoint, log=None):
    """Mount a remote share via FUSE. Returns a stop() callable.

    Requires macFUSE (https://macfuse.github.io) and `pip install fusepy`.
    Raises RuntimeError with guidance if unavailable.
    """
    try:
        from fuse import FUSE, Operations, FuseOSError
        import errno
    except Exception:
        raise RuntimeError("FUSE route needs: install macFUSE, then `pip install fusepy`")

    import requests  # simple sync client inside the FUSE thread
    import threading

    def url(ep, **p):
        p["share"] = share
        if pin:
            p["pin"] = pin
        return f"{base_url.rstrip('/')}/fs/{ep}?" + urllib.parse.urlencode(p)

    class RemoteFS(Operations):
        def getattr(self, path, fh=None):
            r = requests.get(url("stat", path=path.lstrip("/")), timeout=10)
            if r.status_code != 200:
                raise FuseOSError(errno.ENOENT)
            st = r.json()
            mode = 0o040755 if st["dir"] else 0o100644
            return {"st_mode": mode, "st_size": st.get("size", 0),
                    "st_mtime": st.get("mtime", 0), "st_ctime": st.get("mtime", 0),
                    "st_atime": st.get("mtime", 0), "st_nlink": 1}

        def readdir(self, path, fh):
            r = requests.get(url("list", path=path.lstrip("/")), timeout=10)
            entries = [".", ".."]
            if r.status_code == 200:
                entries += [e["name"] for e in r.json().get("entries", [])]
            return entries

        def read(self, path, size, offset, fh):
            r = requests.get(url("read", path=path.lstrip("/")), timeout=60)
            return r.content[offset:offset + size]

        def write(self, path, data, offset, fh):
            # read-modify-write (simple; fine for source files)
            cur = requests.get(url("read", path=path.lstrip("/")), timeout=60)
            buf = bytearray(cur.content if cur.status_code == 200 else b"")
            if offset > len(buf):
                buf.extend(b"\x00" * (offset - len(buf)))
            buf[offset:offset + len(data)] = data
            requests.post(url("write", path=path.lstrip("/")), data=bytes(buf), timeout=60)
            return len(data)

        def create(self, path, mode, fi=None):
            requests.post(url("write", path=path.lstrip("/")), data=b"", timeout=15)
            return 0

        def truncate(self, path, length, fh=None):
            cur = requests.get(url("read", path=path.lstrip("/")), timeout=60)
            buf = (cur.content if cur.status_code == 200 else b"")[:length]
            requests.post(url("write", path=path.lstrip("/")), data=buf, timeout=60)
            return 0

        def unlink(self, path):
            requests.post(url("delete", path=path.lstrip("/")), timeout=15)

        def mkdir(self, path, mode):
            requests.post(url("mkdir", path=path.lstrip("/")), timeout=15)

        def rmdir(self, path):
            requests.post(url("delete", path=path.lstrip("/")), timeout=15)

    os.makedirs(mountpoint, exist_ok=True)
    stop_flag = {"fuse": None}

    def run():
        FUSE(RemoteFS(), mountpoint, nothreads=True, foreground=True)

    t = threading.Thread(target=run, daemon=True)
    t.start()

    def stop():
        try:
            import subprocess
            subprocess.run(["umount", mountpoint], timeout=10)
        except Exception:
            pass

    return stop
