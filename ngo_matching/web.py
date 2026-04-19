from __future__ import annotations

from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

from .storage import DataStore


def _html_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(title)}</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 24px auto;
      max-width: 900px;
      padding: 0 16px;
      color: #1f2937;
    }}
    h1, h2 {{ margin-bottom: 8px; }}
    .card {{
      border: 1px solid #d1d5db;
      border-radius: 10px;
      padding: 16px;
      margin: 16px 0;
      background: #f9fafb;
    }}
    label {{ display: block; margin-top: 8px; font-weight: 600; }}
    input {{
      width: 100%;
      padding: 10px;
      margin-top: 6px;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      box-sizing: border-box;
    }}
    button {{
      margin-top: 12px;
      padding: 10px 14px;
      border-radius: 8px;
      border: none;
      background: #2563eb;
      color: white;
      font-weight: 600;
      cursor: pointer;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
      background: white;
    }}
    th, td {{
      border: 1px solid #e5e7eb;
      padding: 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{ background: #f3f4f6; }}
    .error {{ color: #b91c1c; font-weight: 600; }}
    .ok {{ color: #065f46; font-weight: 600; }}
    .muted {{ color: #6b7280; }}
  </style>
</head>
<body>
{body}
</body>
</html>"""


def _home_page(error: str = "") -> str:
    error_html = f'<p class="error">{escape(error)}</p>' if error else ""
    return _html_page(
        "NGO Matching Login",
        f"""
<h1>NGO Matching Lookup</h1>
<p class="muted">Participants can use name as password to see table number.
Controller can use controller key to see full matching table.</p>
{error_html}
<div class="card">
  <h2>Participant Login</h2>
  <form method="post" action="/participant-login">
    <label for="participant_name">Name password</label>
    <input id="participant_name" name="participant_name" required />
    <button type="submit">View my table</button>
  </form>
</div>
<div class="card">
  <h2>Controller Login</h2>
  <form method="post" action="/controller-login">
    <label for="controller_key">Controller key</label>
    <input id="controller_key" name="controller_key" required />
    <button type="submit">View full matching table</button>
  </form>
</div>
""",
    )


def _participant_result_page(name: str, table_no: int, members: list[str]) -> str:
    members_html = ", ".join(escape(member) for member in members)
    return _html_page(
        "Participant Table Assignment",
        f"""
<h1>Table Assignment</h1>
<p class="ok">{escape(name)}, your table number is <strong>{table_no}</strong>.</p>
<p><strong>Group members:</strong> {members_html}</p>
<p><a href="/">Back to login</a></p>
""",
    )


def _controller_table_page(rows: list[dict[str, object]]) -> str:
    grouped: dict[int, list[str]] = {}
    for row in rows:
        grouped.setdefault(int(row["group_index"]), []).append(str(row["name"]))

    table_rows = "".join(
        f"<tr><td>{group_idx}</td><td>{escape(', '.join(names))}</td></tr>"
        for group_idx, names in sorted(grouped.items())
    )
    body = f"""
<h1>Full Matching Table</h1>
<table>
  <thead>
    <tr><th>Table #</th><th>Members</th></tr>
  </thead>
  <tbody>
    {table_rows}
  </tbody>
</table>
<p><a href="/">Back to login</a></p>
"""
    return _html_page("Controller Matching Table", body)


def _parse_form_data(handler: BaseHTTPRequestHandler) -> dict[str, str]:
    length = int(handler.headers.get("Content-Length", "0"))
    payload = handler.rfile.read(length).decode("utf-8")
    parsed = parse_qs(payload)
    return {k: v[0] for k, v in parsed.items() if v}


class MatchingWebHandler(BaseHTTPRequestHandler):
    store: DataStore

    def _respond_html(self, html: str, status: int = 200) -> None:
        content = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/":
            self._respond_html(_home_page("Unknown route"), status=404)
            return
        self._respond_html(_home_page())

    def do_POST(self) -> None:  # noqa: N802
        data = _parse_form_data(self)
        if self.path == "/participant-login":
            name = data.get("participant_name", "")
            if not name.strip():
                self._respond_html(_home_page("Name is required."), status=400)
                return
            table = self.store.find_table_for_participant_name(name)
            if table is None:
                self._respond_html(
                    _home_page(
                        "No active table found for this name. Ensure matching has run and table is not reset."
                    ),
                    status=404,
                )
                return
            self._respond_html(
                _participant_result_page(
                    name=str(table["name"]),
                    table_no=int(table["group_index"]),
                    members=[str(member) for member in table["members"]],
                )
            )
            return

        if self.path == "/controller-login":
            key = data.get("controller_key", "")
            if not key:
                self._respond_html(_home_page("Controller key is required."), status=400)
                return
            if not self.store.verify_controller_key(key):
                self._respond_html(_home_page("Invalid controller key."), status=403)
                return
            rows = self.store.list_current_matching_table()
            if not rows:
                self._respond_html(
                    _home_page("Current matching table is empty. Run matching first."),
                    status=404,
                )
                return
            self._respond_html(_controller_table_page(rows))
            return

        self._respond_html(_home_page("Unknown route"), status=404)


def run_web_app(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> None:
    store = DataStore(db_path=db_path)

    class Handler(MatchingWebHandler):
        pass

    Handler.store = store
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Web app running on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
