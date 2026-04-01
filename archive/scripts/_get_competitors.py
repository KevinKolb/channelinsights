"""
Klue Competitor Cards Fetcher

Opens a local web page where you paste JSON copied from your logged-in
Klue browser session. Saves all cards to competitors.json.

Usage:
    python _get_competitors.py
"""

import json
import os
import subprocess
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "competitors.json")
BASE_URL = "https://app.klue.com/extract/cards.json"
PORT = 8741

# Shared state between handler and main thread
_all_cards: list = []
_total_items: int = 0
_current_page: int = 1
_page_size: int = 100   # will be updated after first response
_done = threading.Event()


# =============================================================================
# HTML UI
# =============================================================================

def page_html(page: int, limit: int, collected: int, total: int) -> str:
    klue_url = f"{BASE_URL}?page={page}&limit={limit}"
    progress = f"{collected} of {total} cards collected so far." if total else ""
    console_script = (
        f"fetch('{klue_url}')"
        ".then(r=>r.text())"
        f".then(t=>fetch('http://localhost:{PORT}/submit',{{method:'POST',"
        "headers:{'Content-Type':'application/x-www-form-urlencoded'},"
        "body:'data='+encodeURIComponent(t)}}))"
        ".then(r=>r.text())"
        ".then(html=>{{document.open();document.write(html);document.close()}})"
        ".catch(e=>console.error(e))"
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Klue Card Fetcher</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 700px; margin: 60px auto; padding: 0 20px; color: #222; }}
    h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
    .sub {{ color: #666; margin-bottom: 24px; font-size: 0.9rem; }}
    .step {{ background: #f5f5f5; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }}
    .step p {{ margin: 0 0 8px; }}
    a.url {{ font-family: monospace; font-size: 0.85rem; word-break: break-all; }}
    textarea {{ width: 100%; height: 180px; font-family: monospace; font-size: 0.8rem;
                border: 1px solid #ccc; border-radius: 6px; padding: 10px; box-sizing: border-box; }}
    button {{ margin-top: 12px; padding: 10px 28px; background: #2563eb; color: #fff;
              border: none; border-radius: 6px; font-size: 1rem; cursor: pointer; }}
    button:hover {{ background: #1d4ed8; }}
    .progress {{ color: #2563eb; font-weight: 600; margin-bottom: 16px; }}
    .error {{ color: #dc2626; background: #fef2f2; border-radius: 6px; padding: 12px 16px; }}
    .console-wrap {{ position: relative; }}
    .console-script {{ font-family: monospace; font-size: 0.78rem; background: #1e1e1e; color: #d4d4d4;
                       padding: 12px; border-radius: 6px; white-space: pre-wrap; word-break: break-all; }}
    .copy-btn {{ position: absolute; top: 8px; right: 8px; padding: 4px 12px; font-size: 0.78rem;
                 background: #374151; color: #fff; border: none; border-radius: 4px; cursor: pointer; }}
    .copy-btn:hover {{ background: #4b5563; }}
    .tip {{ font-size: 0.85rem; color: #555; margin: 6px 0 0; }}
  </style>
</head>
<body>
  <h1>Klue Card Fetcher</h1>
  <p class="sub">Fetches cards from your logged-in Klue session and saves them locally.</p>
  {"<p class='progress'>" + progress + (f" (~{-(-( total - collected) // max(collected // max(page-1,1), 1))} pages left)" if collected and total and page > 1 else "") + "</p>" if progress else ""}
  <div class="step">
    <p><strong>Step 1</strong> — Open this URL in Chrome while logged into Klue:</p>
    <a class="url" href="{klue_url}" target="_blank">{klue_url}</a>
  </div>
  <div class="step">
    <p><strong>Step 2 (recommended)</strong> — On that Klue page, open DevTools
    (<kbd>F12</kbd>), go to the <strong>Console</strong> tab, paste this, and press Enter:</p>
    <div class="console-wrap">
      <div class="console-script" id="cs">{console_script}</div>
      <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('cs').textContent).then(()=>{{this.textContent='Copied!';setTimeout(()=>this.textContent='Copy',1500)}})">Copy</button>
    </div>
    <p class="tip">This sends the full JSON directly — avoids Chrome's copy-paste truncation for large responses.</p>
  </div>
  <div class="step">
    <p><strong>Step 2 (alternative)</strong> — Or select all (<kbd>Ctrl+A</kbd>), copy (<kbd>Ctrl+C</kbd>), then paste below
    <em>(may fail for very large pages)</em>:</p>
    <form method="POST" action="/submit">
      <textarea name="data" placeholder="Paste JSON here..." required></textarea>
      <br>
      <button type="submit">Submit</button>
    </form>
  </div>
</body>
</html>"""


def done_html(count: int, path: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Done — Klue Card Fetcher</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 700px; margin: 60px auto; padding: 0 20px; color: #222; }}
    .box {{ background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 24px 28px; }}
    h1 {{ color: #16a34a; margin: 0 0 8px; }}
    p {{ margin: 4px 0; color: #444; }}
    code {{ font-size: 0.85rem; background: #e5e7eb; padding: 2px 6px; border-radius: 4px; }}
  </style>
</head>
<body>
  <div class="box">
    <h1>Done!</h1>
    <p>Saved <strong>{count}</strong> cards to:</p>
    <p><code>{path}</code></p>
  </div>
</body>
</html>"""


def error_html(msg: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"><title>Error</title>
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 700px; margin: 60px auto; padding: 0 20px; }}
    .box {{ background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 24px 28px; }}
    h1 {{ color: #dc2626; margin: 0 0 8px; }}
    pre {{ font-size: 0.85rem; white-space: pre-wrap; word-break: break-all; }}
    a {{ color: #2563eb; }}
  </style>
</head>
<body>
  <div class="box">
    <h1>Error</h1>
    <pre>{msg}</pre>
    <p><a href="/">Go back and try again</a></p>
  </div>
</body>
</html>"""


# =============================================================================
# HTTP SERVER
# =============================================================================

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass  # suppress console noise

    def send_html(self, html: str, status: int = 200):
        body = html.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.send_html(page_html(_current_page, _page_size, len(_all_cards), _total_items))

    def do_POST(self):
        global _total_items, _current_page, _page_size

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode()

        from urllib.parse import parse_qs, unquote_plus
        fields = parse_qs(body)
        raw = unquote_plus(fields.get("data", [""])[0]).strip()

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            self.send_html(error_html(f"Could not parse JSON: {e}\n\nMake sure you copied the full page response."))
            return

        items = data.get("items", [])
        if not items and not data.get("totalItems"):
            self.send_html(error_html(f"No 'items' key found in response. Got keys: {list(data.keys())}"))
            return

        _total_items = data.get("totalItems", len(items))
        _all_cards.extend(items)

        # Learn actual page size from first response
        if _current_page == 1 and items:
            _page_size = len(items)

        if len(_all_cards) >= _total_items:
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(_all_cards, f, indent=2, ensure_ascii=False)
            self.send_html(done_html(len(_all_cards), OUTPUT_FILE))
            _done.set()
        else:
            _current_page += 1
            self.send_html(page_html(_current_page, _page_size, len(_all_cards), _total_items))


# =============================================================================
# MAIN
# =============================================================================

def notify(title: str, message: str) -> None:
    script = (
        "Add-Type -AssemblyName System.Windows.Forms; "
        f"[System.Windows.Forms.MessageBox]::Show('{message}', '{title}')"
    )
    subprocess.run(["powershell", "-NoProfile", "-Command", script], check=False)


def main() -> None:
    server = HTTPServer(("localhost", PORT), Handler)
    url = f"http://localhost:{PORT}"
    print(f"Opening {url} ...")
    threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    print("Waiting for data... (close this terminal or Ctrl+C to cancel)")
    try:
        while not _done.is_set():
            server.handle_request()
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(0)

    server.server_close()
    print(f"\nSaved {len(_all_cards)} cards to {OUTPUT_FILE}")
    notify("Klue Fetch Complete", f"Saved {len(_all_cards)} cards to competitors.json")


if __name__ == "__main__":
    main()
