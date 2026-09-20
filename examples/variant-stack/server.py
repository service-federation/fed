"""Storage, catalog, and frontend HTTP services using only the standard library."""

import argparse
import html
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from string import Template
from urllib.error import URLError
from urllib.request import urlopen

WEB_DIR = Path(__file__).resolve().parent / "web"
PAGE_TEMPLATE = Template((WEB_DIR / "index.html").read_text(encoding="utf-8"))
ITEM_TEMPLATE = Template((WEB_DIR / "item.html").read_text(encoding="utf-8"))
STYLESHEET = (WEB_DIR / "styles.css").read_text(encoding="utf-8")
UNHEALTHY_MARKER = Path(".fed/catalog-unhealthy")

LIVE_ITEMS = [
    {"name": "Notebook", "stock": 12},
    {"name": "Pencil", "stock": 24},
]
FIXTURE_ITEMS = [
    {"name": "Sample notebook", "stock": 3},
    {"name": "Sample pencil", "stock": 0},
]


def fetch_json(url):
    with urlopen(url, timeout=1) as response:
        return json.load(response)


def catalog():
    implementation = os.environ.get("IMPLEMENTATION", "live")
    if implementation == "live":
        items = fetch_json(os.environ["STORAGE_URL"] + "/items")["items"]
    else:
        items = FIXTURE_ITEMS

    return {
        "implementation": implementation,
        "label": os.environ["DEMO_LABEL"],
        "items": items,
    }


def render_page(data):
    rows = [
        ITEM_TEMPLATE.substitute(
            name=html.escape(item["name"]),
            stock=html.escape(str(item["stock"])),
        )
        for item in data["items"]
    ]

    return PAGE_TEMPLATE.substitute(
        label=html.escape(data["label"]),
        implementation=html.escape(data["implementation"]),
        rows="".join(rows).rstrip(),
    )


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        role = self.server.role
        try:
            if role == "catalog" and UNHEALTHY_MARKER.exists():
                self.send_json(
                    503, {"error": "Catalog paused by the demo health switch"}
                )
            elif self.path == "/health":
                self.check_health()
            elif role == "storage" and self.path == "/items":
                self.send_json(200, {"items": LIVE_ITEMS})
            elif role == "catalog" and self.path == "/catalog":
                self.send_json(200, catalog())
            elif role == "frontend" and self.path == "/styles.css":
                self.send_text(200, STYLESHEET, "text/css")
            elif role == "frontend" and self.path in ("/", "/catalog"):
                data = fetch_json(os.environ["CATALOG_URL"] + "/catalog")
                if self.path == "/":
                    self.send_text(200, render_page(data), "text/html")
                else:
                    self.send_json(200, data)
            else:
                self.send_json(404, {"error": "Not found"})
        except (URLError, TimeoutError, OSError) as error:
            self.send_json(503, {"error": f"Downstream service unavailable: {error}"})

    def check_health(self):
        # Readiness includes the real downstream dependency.
        if self.server.role == "catalog":
            catalog()
        elif self.server.role == "frontend":
            fetch_json(os.environ["CATALOG_URL"] + "/health")
        self.send_json(200, {"status": "ok", "service": self.server.role})

    def send_json(self, status, data):
        self.send_text(status, json.dumps(data), "application/json")

    def send_text(self, status, text, content_type):
        payload = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class DemoServer(ThreadingHTTPServer):
    def __init__(self, role, port):
        self.role = role
        super().__init__(("127.0.0.1", port), Handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=("storage", "catalog", "frontend"))
    args = parser.parse_args()

    with DemoServer(args.role, int(os.environ["PORT"])) as server:
        print(
            f"{args.role} listening at http://127.0.0.1:{server.server_port}",
            flush=True,
        )
        server.serve_forever()


if __name__ == "__main__":
    main()
