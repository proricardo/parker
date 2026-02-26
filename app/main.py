import asyncio
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import threading
import time
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from playwright.sync_api import sync_playwright
from warcio.warcwriter import WARCWriter

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
STORAGE_DIR = DATA_DIR / "storage"
DB_PATH = DATA_DIR / "parker.db"
LOG_PATH = DATA_DIR / "parker.log"

# Garante diretórios base antes de configurar logging em arquivo
# (evita FileNotFoundError na importação do módulo, especialmente no Windows).
DATA_DIR.mkdir(parents=True, exist_ok=True)
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    encoding="utf-8",
)
logger = logging.getLogger("parker")

DEFAULT_CONFIG = {
    "max_storage_gb": "5",
    "max_concurrent_captures": "2",
    "capture_timeout_sec": "90",
    "max_capture_retries": "2",
    "blocked_domains": "",
    "disk_alert_pct": "85",
}

app = FastAPI(title="Parker")
app.mount("/static", StaticFiles(directory=BASE_DIR / "app" / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))

CAPTURE_SEMAPHORE = threading.Semaphore(int(DEFAULT_CONFIG["max_concurrent_captures"]))


@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    STORAGE_DIR.mkdir(exist_ok=True)
    with db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS captures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                domain TEXT NOT NULL,
                title TEXT,
                description TEXT,
                status TEXT NOT NULL,
                http_status INTEGER,
                total_size INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                finished_at TEXT,
                retries INTEGER DEFAULT 0,
                include_pdf INTEGER DEFAULT 0,
                headers_json TEXT,
                cookies_json TEXT,
                error_message TEXT,
                text_content TEXT,
                links_json TEXT,
                schedule_id INTEGER,
                integrity_state TEXT DEFAULT 'unknown'
            );
            CREATE TABLE IF NOT EXISTS artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                capture_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                path TEXT NOT NULL,
                size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(capture_id) REFERENCES captures(id)
            );
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS capture_tags (
                capture_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                PRIMARY KEY(capture_id, tag_id),
                FOREIGN KEY(capture_id) REFERENCES captures(id),
                FOREIGN KEY(tag_id) REFERENCES tags(id)
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                capture_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                interval_hours INTEGER NOT NULL,
                last_run_at TEXT,
                active INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS integrity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                capture_id INTEGER NOT NULL,
                checked_at TEXT NOT NULL,
                result TEXT NOT NULL,
                details TEXT
            );
            CREATE TABLE IF NOT EXISTS app_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        for key, value in DEFAULT_CONFIG.items():
            conn.execute(
                "INSERT OR IGNORE INTO app_config(key, value) VALUES(?, ?)",
                (key, value),
            )


def config_value(key: str) -> str:
    with db_conn() as conn:
        row = conn.execute("SELECT value FROM app_config WHERE key=?", (key,)).fetchone()
        return row["value"] if row else DEFAULT_CONFIG[key]


def set_event(capture_id: int, level: str, message: str) -> None:
    logger.info("capture=%s level=%s message=%s", capture_id, level, message)
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO events(capture_id, created_at, level, message) VALUES(?,?,?,?)",
            (capture_id, now_iso(), level, message),
        )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def persist_artifact(conn: sqlite3.Connection, capture_id: int, kind: str, path: Path) -> None:
    size = path.stat().st_size
    checksum = sha256_file(path)
    conn.execute(
        "INSERT INTO artifacts(capture_id, kind, path, size, checksum, created_at) VALUES(?,?,?,?,?,?)",
        (capture_id, kind, str(path.relative_to(BASE_DIR)), size, checksum, now_iso()),
    )


def capture_worker(capture_id: int, url: str, include_pdf: bool, headers_json: str, cookies_json: str) -> None:
    with CAPTURE_SEMAPHORE:
        max_retries = int(config_value("max_capture_retries"))
        timeout_sec = int(config_value("capture_timeout_sec"))
        blocked_domains = [d.strip() for d in config_value("blocked_domains").split(",") if d.strip()]
        domain = urlparse(url).netloc
        if domain in blocked_domains:
            with db_conn() as conn:
                conn.execute("UPDATE captures SET status=?, error_message=?, finished_at=? WHERE id=?", ("failed", "Domínio bloqueado", now_iso(), capture_id))
            set_event(capture_id, "error", "Captura bloqueada por política de domínio")
            return

        capture_folder = STORAGE_DIR / f"capture_{capture_id}"
        capture_folder.mkdir(exist_ok=True)

        for attempt in range(0, max_retries + 1):
            partial_failures: List[str] = []
            try:
                set_event(capture_id, "info", f"Tentativa {attempt + 1} iniciada")
                screenshot_path = capture_folder / "snapshot.png"
                html_path = capture_folder / "snapshot.html"
                pdf_path = capture_folder / "snapshot.pdf"
                warc_path = capture_folder / "snapshot.warc"

                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(extra_http_headers=json.loads(headers_json or "{}"))
                    cookie_list = json.loads(cookies_json or "[]")
                    if cookie_list:
                        context.add_cookies(cookie_list)
                    page = context.new_page()
                    response = page.goto(url, wait_until="networkidle", timeout=timeout_sec * 1000)
                    status_code = response.status if response else None
                    for _ in range(8):
                        page.mouse.wheel(0, 2400)
                        page.wait_for_timeout(400)
                    page.screenshot(path=str(screenshot_path), full_page=True)
                    html_content = page.content()
                    html_path.write_text(html_content, encoding="utf-8")
                    if include_pdf:
                        page.pdf(path=str(pdf_path), print_background=True)
                    context.close()
                    browser.close()

                set_event(capture_id, "info", "Artefatos visuais gerados")

                try:
                    with warc_path.open("wb") as stream:
                        writer = WARCWriter(stream, gzip=False)
                        payload = html_path.read_bytes()
                        headers = [("Content-Type", "text/html; charset=utf-8")]
                        record = writer.create_warc_record(url, "response", payload=payload, http_headers=None, warc_headers_dict={"WARC-Target-URI": url})
                        writer.write_record(record)
                except Exception as warc_error:
                    partial_failures.append(f"Falha WARC: {warc_error}")

                soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "html.parser")
                title = soup.title.text.strip() if soup.title else "(sem título)"
                description = ""
                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc:
                    description = meta_desc.get("content", "")
                links = [a.get("href") for a in soup.find_all("a", href=True)][:500]
                text_content = soup.get_text(" ", strip=True)

                with db_conn() as conn:
                    conn.execute("DELETE FROM artifacts WHERE capture_id=?", (capture_id,))
                    persist_artifact(conn, capture_id, "html", html_path)
                    persist_artifact(conn, capture_id, "screenshot", screenshot_path)
                    if warc_path.exists():
                        persist_artifact(conn, capture_id, "warc", warc_path)
                    if include_pdf and pdf_path.exists():
                        persist_artifact(conn, capture_id, "pdf", pdf_path)
                    total_size = conn.execute("SELECT COALESCE(SUM(size),0) AS t FROM artifacts WHERE capture_id=?", (capture_id,)).fetchone()["t"]
                    status = "partial" if partial_failures else "success"
                    conn.execute(
                        """UPDATE captures SET status=?, http_status=?, total_size=?, title=?, description=?, finished_at=?,
                           retries=?, text_content=?, links_json=?, error_message=? WHERE id=?""",
                        (status, status_code, total_size, title, description, now_iso(), attempt, text_content, json.dumps(links), "\n".join(partial_failures), capture_id),
                    )
                set_event(capture_id, "info", "Captura finalizada")
                return
            except Exception as exc:
                set_event(capture_id, "error", f"Erro na tentativa {attempt + 1}: {exc}")
                if attempt == max_retries:
                    with db_conn() as conn:
                        conn.execute(
                            "UPDATE captures SET status=?, error_message=?, finished_at=?, retries=? WHERE id=?",
                            ("failed", str(exc), now_iso(), attempt, capture_id),
                        )
                else:
                    time.sleep(1)


def enqueue_capture(url: str, include_pdf: bool, headers: str, cookies: str, schedule_id: Optional[int] = None) -> int:
    domain = urlparse(url).netloc
    with db_conn() as conn:
        c = conn.execute(
            """INSERT INTO captures(url, domain, status, created_at, include_pdf, headers_json, cookies_json, schedule_id)
               VALUES(?,?,?,?,?,?,?,?)""",
            (url, domain, "pending", now_iso(), 1 if include_pdf else 0, headers, cookies, schedule_id),
        )
        capture_id = c.lastrowid
    t = threading.Thread(target=capture_worker, args=(capture_id, url, include_pdf, headers, cookies), daemon=True)
    t.start()
    return capture_id


def scheduler_loop() -> None:
    while True:
        try:
            with db_conn() as conn:
                rows = conn.execute("SELECT * FROM schedules WHERE active=1").fetchall()
            for row in rows:
                last_run = datetime.fromisoformat(row["last_run_at"]) if row["last_run_at"] else None
                due = not last_run or datetime.now(timezone.utc) - last_run >= timedelta(hours=row["interval_hours"])
                if due:
                    enqueue_capture(row["url"], False, "{}", "[]", row["id"])
                    with db_conn() as conn:
                        conn.execute("UPDATE schedules SET last_run_at=? WHERE id=?", (now_iso(), row["id"]))
            integrity_check_once()
        except Exception as exc:
            logger.exception("scheduler error: %s", exc)
        time.sleep(60)


def integrity_check_once() -> None:
    with db_conn() as conn:
        captures = conn.execute("SELECT id FROM captures WHERE status in ('success','partial')").fetchall()
    for cap in captures:
        cid = cap["id"]
        with db_conn() as conn:
            arts = conn.execute("SELECT * FROM artifacts WHERE capture_id=?", (cid,)).fetchall()
            ok = True
            details = []
            for art in arts:
                path = BASE_DIR / art["path"]
                if not path.exists():
                    ok = False
                    details.append(f"Ausente: {art['kind']}")
                    continue
                chk = sha256_file(path)
                if chk != art["checksum"]:
                    ok = False
                    details.append(f"Checksum inválido: {art['kind']}")
            result = "ok" if ok else "corrupted"
            conn.execute("UPDATE captures SET integrity_state=? WHERE id=?", (result, cid))
            conn.execute(
                "INSERT INTO integrity_logs(capture_id, checked_at, result, details) VALUES(?,?,?,?)",
                (cid, now_iso(), result, "; ".join(details)),
            )


@app.on_event("startup")
def startup_event() -> None:
    init_db()
    threading.Thread(target=scheduler_loop, daemon=True, name="scheduler").start()


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    with db_conn() as conn:
        total = conn.execute("SELECT COUNT(*) c FROM captures").fetchone()["c"]
        size = conn.execute("SELECT COALESCE(SUM(total_size),0) s FROM captures").fetchone()["s"]
        domains = conn.execute("SELECT COUNT(DISTINCT domain) d FROM captures").fetchone()["d"]
        recent = conn.execute("SELECT COUNT(*) c FROM captures WHERE created_at >= ?", ((datetime.now(timezone.utc)-timedelta(days=7)).isoformat(),)).fetchone()["c"]
    max_storage_bytes = float(config_value("max_storage_gb")) * 1024**3
    usage_pct = (size / max_storage_bytes * 100) if max_storage_bytes else 0
    alert = usage_pct >= float(config_value("disk_alert_pct"))
    return templates.TemplateResponse("dashboard.html", {"request": request, "total": total, "size": size, "domains": domains, "recent": recent, "alert": alert, "usage_pct": round(usage_pct, 2)})


@app.get("/captures", response_class=HTMLResponse)
def list_captures(request: Request, q: str = "", domain: str = "", status: str = "", tag: str = "", page: int = 1, sort: str = "created_at_desc"):
    page_size = 10
    filters = ["1=1"]
    params: List[Any] = []
    if q:
        filters.append("(title LIKE ? OR description LIKE ? OR text_content LIKE ?)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if domain:
        filters.append("domain=?")
        params.append(domain)
    if status:
        filters.append("status=?")
        params.append(status)
    if tag:
        filters.append("id IN (SELECT capture_id FROM capture_tags ct JOIN tags t ON t.id=ct.tag_id WHERE t.name=?)")
        params.append(tag)
    order = "created_at DESC" if sort == "created_at_desc" else "created_at ASC" if sort == "created_at_asc" else "total_size DESC"

    with db_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) c FROM captures WHERE {' AND '.join(filters)}", params).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM captures WHERE {' AND '.join(filters)} ORDER BY {order} LIMIT ? OFFSET ?",
            params + [page_size, (page - 1) * page_size],
        ).fetchall()
        tags = conn.execute("SELECT name FROM tags ORDER BY name").fetchall()
    highlights = {}
    if q:
        for r in rows:
            txt = r["text_content"] or ""
            pos = txt.lower().find(q.lower())
            if pos >= 0:
                start, end = max(0, pos - 80), min(len(txt), pos + 80)
                snippet = txt[start:end].replace(q, f"<mark>{q}</mark>")
                highlights[r["id"]] = snippet

    return templates.TemplateResponse("captures.html", {"request": request, "captures": rows, "q": q, "domain": domain, "status": status, "page": page, "total_pages": max(1, (total + page_size - 1) // page_size), "highlights": highlights, "tags": tags})


@app.post("/captures")
def create_capture(url: str = Form(...), include_pdf: Optional[str] = Form(None), headers_json: str = Form("{}"), cookies_json: str = Form("[]")):
    capture_id = enqueue_capture(url, include_pdf == "on", headers_json, cookies_json)
    return RedirectResponse(f"/captures/{capture_id}", status_code=303)


@app.post("/captures/{capture_id}/delete")
def delete_capture(capture_id: int):
    with db_conn() as conn:
        artifacts = conn.execute("SELECT path FROM artifacts WHERE capture_id=?", (capture_id,)).fetchall()
        conn.execute("DELETE FROM capture_tags WHERE capture_id=?", (capture_id,))
        conn.execute("DELETE FROM artifacts WHERE capture_id=?", (capture_id,))
        conn.execute("DELETE FROM events WHERE capture_id=?", (capture_id,))
        conn.execute("DELETE FROM integrity_logs WHERE capture_id=?", (capture_id,))
        conn.execute("DELETE FROM captures WHERE id=?", (capture_id,))
    for art in artifacts:
        p = BASE_DIR / art["path"]
        if p.exists():
            p.unlink()
    folder = STORAGE_DIR / f"capture_{capture_id}"
    if folder.exists():
        shutil.rmtree(folder, ignore_errors=True)
    return RedirectResponse("/captures", status_code=303)


@app.post("/captures/{capture_id}/tags")
def add_tag(capture_id: int, tag: str = Form(...)):
    with db_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO tags(name) VALUES(?)", (tag,))
        tag_id = conn.execute("SELECT id FROM tags WHERE name=?", (tag,)).fetchone()["id"]
        conn.execute("INSERT OR IGNORE INTO capture_tags(capture_id, tag_id) VALUES(?,?)", (capture_id, tag_id))
    return RedirectResponse(f"/captures/{capture_id}", status_code=303)


@app.post("/captures/{capture_id}/tags/remove")
def remove_tag(capture_id: int, tag: str = Form(...)):
    with db_conn() as conn:
        conn.execute("DELETE FROM capture_tags WHERE capture_id=? AND tag_id=(SELECT id FROM tags WHERE name=?)", (capture_id, tag))
    return RedirectResponse(f"/captures/{capture_id}", status_code=303)


@app.get("/captures/{capture_id}", response_class=HTMLResponse)
def capture_detail(request: Request, capture_id: int):
    with db_conn() as conn:
        cap = conn.execute("SELECT * FROM captures WHERE id=?", (capture_id,)).fetchone()
        if not cap:
            raise HTTPException(404)
        artifacts = conn.execute("SELECT * FROM artifacts WHERE capture_id=?", (capture_id,)).fetchall()
        tags = conn.execute("SELECT t.name FROM tags t JOIN capture_tags ct ON ct.tag_id=t.id WHERE ct.capture_id=?", (capture_id,)).fetchall()
        siblings = conn.execute("SELECT id, created_at, status FROM captures WHERE url=? ORDER BY created_at DESC", (cap["url"],)).fetchall()
    return templates.TemplateResponse("capture_detail.html", {"request": request, "cap": cap, "artifacts": artifacts, "tags": tags, "siblings": siblings})


@app.get("/captures/{capture_id}/events")
def capture_events(capture_id: int):
    def generate():
        last_id = 0
        while True:
            with db_conn() as conn:
                rows = conn.execute("SELECT * FROM events WHERE capture_id=? AND id>? ORDER BY id", (capture_id, last_id)).fetchall()
                for row in rows:
                    last_id = row["id"]
                    yield f"data: {row['created_at']} [{row['level']}] {row['message']}\n\n"
                done = conn.execute("SELECT status FROM captures WHERE id=?", (capture_id,)).fetchone()
            if done and done["status"] in ("success", "failed", "partial") and not rows:
                break
            time.sleep(1)
    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/artifacts/{artifact_id}/download")
def download_artifact(artifact_id: int):
    with db_conn() as conn:
        art = conn.execute("SELECT * FROM artifacts WHERE id=?", (artifact_id,)).fetchone()
    if not art:
        raise HTTPException(404)
    path = BASE_DIR / art["path"]
    return FileResponse(path, filename=path.name)


@app.get("/artifacts/{artifact_id}/view")
def view_artifact(artifact_id: int):
    with db_conn() as conn:
        art = conn.execute("SELECT * FROM artifacts WHERE id=?", (artifact_id,)).fetchone()
    if not art:
        raise HTTPException(404)
    path = BASE_DIR / art["path"]
    if art["kind"] == "html":
        return HTMLResponse(path.read_text(encoding="utf-8", errors="ignore"))
    return FileResponse(path)


@app.get("/settings", response_class=HTMLResponse)
def settings(request: Request):
    with db_conn() as conn:
        cfg = conn.execute("SELECT key, value FROM app_config ORDER BY key").fetchall()
        schedules = conn.execute("SELECT * FROM schedules ORDER BY id DESC").fetchall()
    return templates.TemplateResponse("settings.html", {"request": request, "cfg": cfg, "schedules": schedules})


@app.post("/settings")
def update_settings(max_storage_gb: str = Form(...), max_concurrent_captures: str = Form(...), capture_timeout_sec: str = Form(...), max_capture_retries: str = Form(...), blocked_domains: str = Form(""), disk_alert_pct: str = Form("85")):
    updates = {
        "max_storage_gb": max_storage_gb,
        "max_concurrent_captures": max_concurrent_captures,
        "capture_timeout_sec": capture_timeout_sec,
        "max_capture_retries": max_capture_retries,
        "blocked_domains": blocked_domains,
        "disk_alert_pct": disk_alert_pct,
    }
    with db_conn() as conn:
        for k, v in updates.items():
            conn.execute("UPDATE app_config SET value=? WHERE key=?", (v, k))
    global CAPTURE_SEMAPHORE
    CAPTURE_SEMAPHORE = threading.Semaphore(int(max_concurrent_captures))
    return RedirectResponse("/settings", status_code=303)


@app.post("/schedules")
def create_schedule(url: str = Form(...), interval_hours: int = Form(...)):
    with db_conn() as conn:
        conn.execute("INSERT INTO schedules(url, interval_hours, active) VALUES(?,?,1)", (url, interval_hours))
    return RedirectResponse("/settings", status_code=303)


@app.post("/schedules/{schedule_id}/toggle")
def toggle_schedule(schedule_id: int):
    with db_conn() as conn:
        row = conn.execute("SELECT active FROM schedules WHERE id=?", (schedule_id,)).fetchone()
        conn.execute("UPDATE schedules SET active=? WHERE id=?", (0 if row["active"] else 1, schedule_id))
    return RedirectResponse("/settings", status_code=303)


@app.get("/backup/export")
def export_backup():
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = DATA_DIR / f"backup_{stamp}.zip"
    with zipfile.ZipFile(backup_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(DB_PATH, arcname="parker.db")
        for p in STORAGE_DIR.rglob("*"):
            if p.is_file():
                z.write(p, arcname=str(Path("storage") / p.relative_to(STORAGE_DIR)))
    return FileResponse(backup_path, filename=backup_path.name)
