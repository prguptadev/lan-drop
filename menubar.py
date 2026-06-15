import asyncio
import socket
import subprocess
import threading
import webbrowser

import rumps

from app import serve, get_local_ip

PORT = 8765


def computer_name():
    try:
        return subprocess.check_output(["scutil", "--get", "ComputerName"]).decode().strip()
    except Exception:
        return socket.gethostname()


def run_server(name, port):
    asyncio.run(serve(name, port, announce=False))


class LanDrop(rumps.App):
    def __init__(self):
        super().__init__("Lan Drop", title="📩")
        self.dev_name = computer_name()
        self.port = PORT
        self.status = rumps.MenuItem("Starting…")
        self.menu = [
            rumps.MenuItem("Open Chat", callback=self.open_chat),
            None,                       # separator
            self.status,
            rumps.MenuItem(f"This Mac: {self.dev_name}", callback=None),
        ]
        threading.Thread(target=run_server, args=(self.dev_name, self.port), daemon=True).start()
        rumps.Timer(self._mark_online, 2).start()

    def _mark_online(self, timer):
        self.status.title = f"Online · http://localhost:{self.port}"
        timer.stop()

    def open_chat(self, _):
        webbrowser.open(f"http://localhost:{self.port}")


if __name__ == "__main__":
    LanDrop().run()
