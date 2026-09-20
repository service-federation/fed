"""Small real HTTP services for the fed variant-stack example; no dependencies."""

import html
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
from urllib.error import URLError
from urllib.request import urlopen

ROLE = sys.argv[1]
IMPLEMENTATION = os.environ.get("IMPLEMENTATION", "live")
UNHEALTHY = Path(".fed/catalog-unhealthy")
LIVE_ITEMS = [{"name": "Notebook", "stock": 12}, {"name": "Pencil", "stock": 24}]
FIXTURE_ITEMS = [{"name": "Sample notebook", "stock": 3}, {"name": "Sample pencil", "stock": 0}]


def fetch(url):
    with urlopen(url, timeout=1) as response:
        return json.load(response)


def catalog():
    items = (
        fetch(os.environ["STORAGE_URL"] + "/items")["items"]
        if IMPLEMENTATION == "live"
        else FIXTURE_ITEMS
    )
    return {"implementation": IMPLEMENTATION, "label": os.environ["DEMO_LABEL"], "items": items}


def page(data):
    rows = "".join(
        f'<tr><td>{html.escape(item["name"])}</td><td>{item["stock"]}</td></tr>'
        for item in data["items"]
    )
    return f"""<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Variant stack · Catalog</title>
<style>
body {{ font: 18px/1.6 system-ui,sans-serif; color:#193126; background:#f4f7f2; margin:0; }}
main {{ max-width:680px; margin:8vh auto; padding:24px; }}
h1 {{ font-size:clamp(28px,6vw,44px); line-height:1.15; margin:16px 0; }}
.badge {{ display:inline-block; padding:4px 14px; background:#d6eddb; border-radius:24px; }}
table {{ width:100%; border-collapse:collapse; margin:28px 0; background:white; }}
th,td {{ text-align:left; padding:16px; border-bottom:1px solid #d9e3d8; }}
a {{ color:#225c37; }} code {{ font-size:0.85em; }}
</style><main>
<p>{html.escape(data['label'])}</p><h1>One catalog.<br>Two implementations.</h1>
<p class="badge">Active implementation: <strong>{html.escape(data['implementation'])}</strong></p>
<table><caption>Current catalog</caption><thead><tr><th>Item</th><th>In stock</th></tr></thead>
<tbody>{rows}</tbody></table>
<p>The frontend calls the same catalog URL for either implementation.
Switch the variant, then reload this page to see the result.</p>
<p><a href="/catalog">View the catalog JSON</a> · <a href="/">Reload catalog</a></p>
</main></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if ROLE == "catalog" and UNHEALTHY.exists():
                return self.send(503, {"error": "Catalog paused by the demo health switch"})
            if self.path == "/health":
                # Readiness includes the real downstream dependency.
                if ROLE == "catalog":
                    catalog()
                elif ROLE == "frontend":
                    fetch(os.environ["CATALOG_URL"] + "/health")
                return self.send(200, {"status": "ok", "service": ROLE})
            if ROLE == "storage" and self.path == "/items":
                return self.send(200, {"items": LIVE_ITEMS})
            if ROLE == "catalog" and self.path == "/catalog":
                return self.send(200, catalog())
            if ROLE == "frontend" and self.path in ("/", "/catalog"):
                data = fetch(os.environ["CATALOG_URL"] + "/catalog")
                return self.send(200, page(data) if self.path == "/" else data)
            self.send(404, {"error": "Not found"})
        except (URLError, TimeoutError, OSError) as error:
            self.send(503, {"error": f"Downstream service unavailable: {error}"})

    def send(self, status, body):
        is_html = isinstance(body, str)
        payload = (body if is_html else json.dumps(body)).encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8" if is_html else "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    if ROLE not in ("storage", "catalog", "frontend"):
        raise SystemExit("Usage: python3 server.py storage|catalog|frontend")
    server = ThreadingHTTPServer(("127.0.0.1", int(os.environ["PORT"])), Handler)
    print(f"{ROLE} listening at http://127.0.0.1:{server.server_port}", flush=True)
    server.serve_forever()
