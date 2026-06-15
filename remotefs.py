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

import aiohttp
from aiohttp import ClientSession, ClientTimeout


IGNORE_DIRS = {".git", "node_modules", ".venv", "venv", "__pycache__", "dist", "build",
               ".next", "target", ".idea", ".gradle", ".cache", ".pytest_cache", ".mypy_cache",
               ".DS_Store", ".terraform", "vendor", ".turbo", ".parcel-cache"}

# Skip big blobs/archives/media/model weights — code sync should stay lean.
IGNORE_EXT = {".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".rar", ".7z", ".iso", ".dmg",
              ".pkg", ".mp4", ".mov", ".mkv", ".avi", ".webm", ".mp3", ".wav", ".bin",
              ".ckpt", ".safetensors", ".pt", ".pth", ".h5", ".pkl", ".onnx", ".parquet",
              ".model", ".weights", ".sqlite", ".db", ".img"}
MAX_FILE_BYTES = 100 * 1024 * 1024  # 100 MB per-file cap


def is_ignored(rel, size=None):
    if any(part in IGNORE_DIRS for part in rel.split("/")):
        return True
    ext = ("." + rel.rsplit(".", 1)[-1].lower()) if "." in rel else ""
    if ext in IGNORE_EXT:
        return True
    if size is not None and size > MAX_FILE_BYTES:
        return True
    return False


def _sig(path: Path):
    try:
        st = path.stat()
        return (int(st.st_mtime), st.st_size)
    except OSError:
        return None


def local_manifest(root: Path):
    """rel-path -> (mtime, size) for every file under root (ignored dirs skipped)."""
    out = {}
    for p in root.rglob("*"):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            s = _sig(p)
            if not s or is_ignored(rel, s[1]):
                continue
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
        self._initialized = False
        self.status = "starting"
        self.files = 0
        self._last_error = None

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
            while not self._stop:
                try:
                    if not self._initialized:
                        self.status = "connecting…"
                        await self._initial()
                        self._initialized = True
                    await self._cycle()
                    self.files = len(self.base_host)
                    self.status = (f"error: {self._last_error}" if self._last_error
                                   else f"synced · {self.files} files")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.status = f"paused: {e}"
                    await self._emit(f"sync paused — {e}")
                await asyncio.sleep(self.INTERVAL)
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
        try:
            async with ClientSession(timeout=ClientTimeout(total=30)) as s:
                async with s.get(self._url("manifest")) as r:
                    if r.status == 403:
                        raise RuntimeError("host refused (wrong PIN)")
                    if r.status == 404:
                        raise RuntimeError("shared folder not found on host")
                    if r.status != 200:
                        raise RuntimeError(f"host HTTP {r.status}")
                    data = await r.json()
        except aiohttp.ClientError as e:
            raise RuntimeError(f"can't reach host ({e.__class__.__name__})")
        return {k: tuple(v) for k, v in data.get("files", {}).items()}

    async def _pull(self, rel):
        dest = self.local / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with ClientSession(timeout=ClientTimeout(total=None, sock_connect=15, sock_read=120)) as s:
                async with s.get(self._url("read", path=rel)) as r:
                    if r.status != 200:
                        raise RuntimeError(f"read HTTP {r.status}")
                    with open(dest, "wb") as f:
                        async for chunk in r.content.iter_chunked(1024 * 256):
                            f.write(chunk)
        except aiohttp.ClientError as e:
            raise RuntimeError(f"read failed ({e.__class__.__name__})")

    async def _push(self, rel):
        src = self.local / rel
        try:
            async with ClientSession(timeout=ClientTimeout(total=None, sock_connect=15, sock_read=120)) as s:
                with open(src, "rb") as f:
                    async with s.post(self._url("write", path=rel), data=f) as r:
                        if r.status == 403:
                            raise RuntimeError("write refused (wrong PIN)")
                        if r.status != 200:
                            raise RuntimeError(f"write HTTP {r.status}")
                        return await r.json()
        except aiohttp.ClientError as e:
            raise RuntimeError(f"write failed ({e.__class__.__name__})")

    async def _del_host(self, rel):
        async with ClientSession(timeout=ClientTimeout(total=15)) as s:
            await s.post(self._url("delete", path=rel))

    # ---- sync logic ----
    async def _initial(self):
        self.status = "scanning host…"
        host = await self._get_manifest()
        loc = local_manifest(self.local)
        to_pull = [rel for rel in host if rel not in loc]
        total = len(to_pull)
        for i, rel in enumerate(to_pull, 1):
            self.status = f"pulling {i}/{total}…"
            await self._pull(rel)
        # adopt baselines
        loc = local_manifest(self.local)
        self.base_host = dict(host)
        self.base_local = {rel: loc.get(rel) for rel in host}
        # push any purely-local files that the host doesn't have
        extra = [rel for rel in loc if rel not in host]
        for i, rel in enumerate(extra, 1):
            self.status = f"pushing {i}/{len(extra)}…"
            meta = await self._push(rel)
            self.base_host[rel] = (int(meta.get("mtime", 0)), int(meta.get("size", 0)))
            self.base_local[rel] = loc[rel]
        await self._emit(f"initial sync done ({len(self.base_host)} files)")

    async def _cycle(self):
        host = await self._get_manifest()  # raises if host unreachable/wrong pin
        loc = local_manifest(self.local)
        rels = set(host) | set(loc) | set(self.base_host) | set(self.base_local)
        pulled = pushed = deleted = 0
        errors = []
        for rel in rels:
            try:
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
            except Exception as e:
                errors.append(f"{rel}: {e}")
        if pulled or pushed or deleted:
            await self._emit(f"synced (+{pulled} ↓ {pushed} ↑ {deleted} ✕)")
        if errors:
            self._last_error = errors[0]
            await self._emit(f"{len(errors)} file(s) failed — {errors[0]}")
        else:
            self._last_error = None


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
