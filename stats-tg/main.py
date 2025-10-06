#!/usr/bin/env python3
import asyncio
import logging
import os
import pathlib
import re
from collections import defaultdict
from contextlib import suppress
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Iterable

import anyio
import aiosqlite
import pytz
import httpx
from dateutil import parser as dateparser
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool
from starlette.templating import Jinja2Templates
from urllib.parse import quote

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    JobQueue,
)
from telegram.helpers import escape_markdown

# =========================
# Конфигурация и утилиты
# =========================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_BASE_URL = os.getenv("API_BASE_URL", "").rstrip("/")
API_SEARCH_PATH = "/events/search"
TIMEZONE_NAME = os.getenv("TIMEZONE", "Europe/Moscow")
LOCAL_TZ = pytz.timezone(TIMEZONE_NAME)
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
SSL_CERT_PATH = os.getenv("SSL_CERT_PATH", "/etc/certs/ssl.pem")
SSL_KEY_PATH = os.getenv("SSL_KEY_PATH", "/etc/certs/ssl.key")
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/chats.db")

_raw_hdr = os.getenv("API_AUTH_HEADER", "").strip()
API_HEADERS: Dict[str, str] = {}
if _raw_hdr:
    for part in _raw_hdr.split(";"):
        if ":" in part:
            k, v = part.split(":", 1)
            API_HEADERS[k.strip()] = v.strip()

QR_IDS_ENV = [s.strip() for s in os.getenv("QR_IDS", "").split(",") if s.strip()]

EVENT_VISIT = {"Link", "Visit"}
EVENT_SUB = {"Subscribe", "Subscription", "Follow"}
EVENT_UNSUB = {"Unsubscribe", "Unfollow"}

STATS_RE = re.compile(
    r"^/stats\s+(\S+)\s+(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})\s*-\s*(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})$",
    re.IGNORECASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("qr-bot+api")

templates = Jinja2Templates(directory="templates")
templates.env.globals["EVENT_VISIT"] = EVENT_VISIT
templates.env.globals["EVENT_SUB"] = EVENT_SUB
templates.env.globals["EVENT_UNSUB"] = EVENT_UNSUB


def ensure_config():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set")
    if not API_BASE_URL:
        raise RuntimeError("API_BASE_URL is not set")


def parse_local_dt(s: str) -> datetime:
    dt = datetime.strptime(s, "%d.%m.%Y %H:%M")
    return LOCAL_TZ.localize(dt)


def fmt_local(dt: datetime) -> str:
    dt_loc = dt.astimezone(LOCAL_TZ)
    return dt_loc.strftime("%d.%m.%Y %H:%M")


templates.env.globals["fmt_local"] = fmt_local


def to_iso(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def build_stats_link(qr_id: str, dt_from: datetime, dt_to: datetime) -> str:
    if not PUBLIC_BASE_URL:
        return ""
    f_iso = to_iso(dt_from)
    t_iso = to_iso(dt_to)
    f_q = quote(f_iso, safe="")
    t_q = quote(t_iso, safe="")
    return f"{PUBLIC_BASE_URL}/stats/{qr_id}/from={f_q}&to={t_q}"


# =========================
# Async HTTP клиент к stats
# =========================

_http_client: Optional[httpx.AsyncClient] = None

async def api_search_async(
    qr_id: Optional[str], dt_from: datetime, dt_to: datetime
) -> Dict[str, Any]:
    assert _http_client is not None, "HTTP client not initialized"
    url = f"{API_BASE_URL}{API_SEARCH_PATH}"
    payload: Dict[str, Any] = {"from": to_iso(dt_from), "to": to_iso(dt_to)}
    if qr_id:
        payload["qr_id"] = qr_id
    log.debug("POST %s payload=%s headers=%s", url, payload, API_HEADERS)
    r = await _http_client.post(url, json=payload, headers=API_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or "events" not in data:
        raise ValueError("Unexpected API response shape")
    return data


# =========================
# Вспомогательные функции
# =========================

def extract_qr_ids_from_events(events: List[Dict[str, Any]]) -> List[str]:
    qs = []
    for ev in events:
        p = ev.get("payload") or {}
        qr = p.get("qr_id") or p.get("qrId")
        if qr and qr not in qs:
            qs.append(qr)
    return qs


def group_user_events(events: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    users: Dict[int, Dict[str, Any]] = {}
    for ev in events:
        info = ev.get("vk_user_info") or {}
        uid = info.get("id")
        if uid is None:
            continue
        bucket = users.setdefault(
            uid,
            {
                "first_name": info.get("first_name") or "",
                "last_name": info.get("last_name") or "",
                "events": [],
                "subscribed_in_range": False,
                "link": f"https://vk.com/id{uid}",
            },
        )
        ts_raw = ev.get("timestamp")
        try:
            ts = dateparser.isoparse(ts_raw)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        etype = ev.get("event_type")
        payload = ev.get("payload") or {}

        if etype in EVENT_SUB:
            bucket["subscribed_in_range"] = True

        bucket["events"].append((ts, etype, payload))

    for b in users.values():
        b["events"].sort(key=lambda t: t[0])
    return users


def summarize_counts(events: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    visits = subs = unsubs = 0
    for e in events:
        et = e.get("event_type")
        if et in EVENT_VISIT:
            visits += 1
        elif et in EVENT_SUB:
            subs += 1
        elif et in EVENT_UNSUB:
            unsubs += 1
    return visits, subs, unsubs

def summarize_counts_by_qr(events: List[Dict[str, Any]]) -> Dict[str, Tuple[int, int, int]]:
    """
    Считает V/S/U по каждому qr (строго из e.payload.qr_id).
    События без qr_id кладём в ключ "unknown".
    """
    buckets = defaultdict(lambda: [0, 0, 0])  # [visits, subs, unsubs]
    for e in events:
        q = e.get("payload", {}).get("qr_id")
        if q is None:
            continue
        et = e.get("event_type")
        if et in EVENT_VISIT:
            buckets[q][0] += 1
        elif et in EVENT_SUB:
            buckets[q][1] += 1
        elif et in EVENT_UNSUB:
            buckets[q][2] += 1

    return {q: (v[0], v[1], v[2]) for q, v in buckets.items()}

def escape_md(text: str) -> str:
    return escape_markdown(text, version=2)


def build_stats_message(
    qr_id: str, dt_from: datetime, dt_to: datetime, events: List[Dict[str, Any]]
) -> str:
    users = group_user_events(events)
    visits, subs, unsubs = summarize_counts(events)
    lines = []
    lines.append(
        f"📍 Статистика по {qr_id.upper().replace('QR', 'QR-') if not qr_id.upper().startswith('QR') else qr_id.upper()}"
    )
    lines.append(f"📅 Период : {fmt_local(dt_from)} - {fmt_local(dt_to)}")

    if users:
        for idx, (uid, b) in enumerate(users.items(), start=1):
            fio = f"{b['last_name']} {b['first_name']}".strip() or f"ID {uid}"
            lines.append(f"{idx}. {fio}")
            for ts, etype, payload in b["events"]:
                if etype in EVENT_VISIT:
                    lines.append(f"{fmt_local(ts)} - посещение")
                elif etype in EVENT_SUB:
                    lines.append(f"{fmt_local(ts)} - подписка")
                elif etype in EVENT_UNSUB:
                    lines.append(f"{fmt_local(ts)} - отписка")
                else:
                    lines.append(f"{fmt_local(ts)} - событие: {etype}")
            lines.append(f"Подписался: {'да' if b['subscribed_in_range'] else 'нет'}")
            lines.append(f'Ссылка: "{b["link"]}"')
    else:
        lines.append("Нет событий за указанный период.")

    lines.append("Итог:")
    lines.append(f"Посещений: {visits}")
    lines.append(f"Подписок: {subs}")
    lines.append(f"Отписок: {unsubs}")
    return "\n".join(lines)


def build_summary_message(
    qr_id: str, dt_from: datetime, dt_to: datetime, events: List[Dict[str, Any]]
) -> str:
    visits, subs, unsubs = summarize_counts(events)
    link = build_stats_link(qr_id, dt_from, dt_to)
    pretty_link = escape_md(link) if link else ""

    lines = [
        f"📍 *{escape_md(qr_id.upper().replace('QR', 'QR-'))}*",
        f"📅 Период: {escape_md(fmt_local(dt_from))} – {escape_md(fmt_local(dt_to))}",
        "",
        "*Итог:*",
        f"Посещений: *{visits}*",
        f"Подписок: *{subs}*",
        f"Отписок: *{unsubs}*",
    ]
    if pretty_link:
        lines.append("")
        lines.append(pretty_link)
    return "\n".join(lines)


def split_message(s: str, limit: int = 4000) -> Iterable[str]:
    if len(s) <= limit:
        return [s]
    parts = []
    buf = []
    ln = 0
    for line in s.splitlines():
        if ln + len(line) + 1 > limit and buf:
            parts.append("\n".join(buf))
            buf = []
            ln = 0
        buf.append(line)
        ln += len(line) + 1
    if buf:
        parts.append("\n".join(buf))
    return parts


# =========================
# FastAPI (async)
# =========================

class EventOutDict(Dict[str, Any]): ...
class UserOutDict(Dict[str, Any]): ...
class StatsOutDict(Dict[str, Any]): ...

app = FastAPI(title="QR Stats API")

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/stats/{qr_id}/from={from_iso}&to={to_iso}", response_class=HTMLResponse)
async def http_stats(qr_id: str, from_iso: str, to_iso: str, request: Request):
    try:
        dt_from = dateparser.isoparse(from_iso)
        dt_to = dateparser.isoparse(to_iso)
        if dt_from.tzinfo is None:
            dt_from = LOCAL_TZ.localize(dt_from)
        if dt_to.tzinfo is None:
            dt_to = LOCAL_TZ.localize(dt_to)
        if dt_to <= dt_from:
            raise ValueError("Invalid period")
    except Exception as e:
        return templates.TemplateResponse(
            "error.html",
            {"request": request, "title": "Ошибка в дате", "message": str(e)},
            status_code=400,
        )

    try:
        data = await api_search_async(qr_id, dt_from, dt_to)
        events = data.get("events") or []

        users = group_user_events(events)
        visits, subs, unsubs = summarize_counts(events)

        context = {
            "request": request,
            "qr_id": qr_id,
            "dt_from": dt_from,
            "dt_to": dt_to,
            "fmt_from": fmt_local(dt_from),
            "fmt_to": fmt_local(dt_to),
            "visits": visits,
            "subs": subs,
            "unsubs": unsubs,
            "users": users,
            "events_count": len(events),
        }
        # Jinja рендер — синхронный; выносим в поток, чтобы не блокировать loop
        return await run_in_threadpool(templates.TemplateResponse, "stats.html", context)

    except httpx.HTTPError as e:
        return templates.TemplateResponse(
            "error.html",
            {"request": request, "title": "Ошибка API", "message": str(e)},
            status_code=502,
        )
    except Exception as e:
        log.exception("http_stats failed")
        return templates.TemplateResponse(
            "error.html",
            {"request": request, "title": "Ошибка сервера", "message": str(e)},
            status_code=500,
        )


# =========================
# БД (aiosqlite)
# =========================

DB_PATH = str(pathlib.Path(DATABASE_PATH).resolve())
pathlib.Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS digest_chats (
                chat_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await conn.commit()

async def add_chat_async(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO digest_chats(chat_id) VALUES (?)", (chat_id,)
        )
        await conn.commit()

async def remove_chat_async(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("DELETE FROM digest_chats WHERE chat_id = ?", (chat_id,))
        await conn.commit()

async def list_chats_async() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT chat_id FROM digest_chats ORDER BY chat_id")
        rows = await cur.fetchall()
        return [r[0] for r in rows]


# =========================
# Telegram (async)
# =========================

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    m = STATS_RE.match(text)
    if not m:
        await update.message.reply_text(
            "Неверный формат. Пример:\n/stats qr1 07.09.2025 14:00 - 08.09.2025 11:00"
        )
        return

    qr_id, from_s, to_s = m.groups()
    try:
        dt_from = parse_local_dt(from_s)
        dt_to = parse_local_dt(to_s)
        if dt_to <= dt_from:
            raise ValueError("Период пустой")
    except Exception:
        await update.message.reply_text(
            "Не удалось распознать даты. Проверьте формат DD.MM.YYYY HH:MM."
        )
        return

    try:
        data = await api_search_async(qr_id, dt_from, dt_to)
        events = data.get("events") or []
        msg = build_summary_message(qr_id, dt_from, dt_to, events)
        await update.message.reply_text(
            msg, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True
        )
    except Exception as e:
        log.exception("stats_cmd failed")
        await update.message.reply_text(f"Ошибка при получении данных: {e}")

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я считаю статистику по QR.\n"
        "Формат запроса:\n"
        "/stats qr1 07.09.2025 14:00 - 08.09.2025 11:00\n\n"
        "Каждый день в 21:00 МСК я отправляю сводку по всем QR."
    )

async def daily_digest(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.data.get("chat_id")

    now_loc = datetime.now(LOCAL_TZ)
    day_start = now_loc.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1) - timedelta(milliseconds=1)

    try:
        results = await api_search_async(None, day_start, day_end)
        events = results.get("events") or []
        per_qr = summarize_counts_by_qr(events)
        lines = ["Статистика по всем QR за сегодня."]
        for qr_id, (visits, subs, unsubs) in per_qr.items():
            lines.append(f"{qr_id}:")
            lines.append(f"Посещений: {visits}")
            lines.append(f"Подписок: {subs}")
            lines.append(f"Отписок: {unsubs}")
        if not per_qr:
            lines.append("Событий не найдено.")
        msg = "\n".join(lines)

        for chunk in split_message(msg):
            await context.bot.send_message(chat_id=chat_id, text=chunk)
    except Exception as e:
        log.exception("daily_digest failed")
        await context.bot.send_message(
            chat_id=chat_id, text=f"Ошибка при составлении сводки: {e}"
        )

async def enable_digest_for_chat(update, context):
    await add_chat_async(update.effective_chat.id)
    if not context.job_queue:
        await update.message.reply_text("⛔️ Планировщик не инициализирован.")
        return

    chat_id = update.effective_chat.id

    # убираем старую job, если была
    for j in context.job_queue.get_jobs_by_name(f"digest_{chat_id}"):
        j.schedule_removal()

    # планируем
    context.job_queue.run_daily(
        callback=daily_digest,
        time=time(hour=21, minute=0, tzinfo=LOCAL_TZ),
        name=f"digest_{chat_id}",
        data={"chat_id": chat_id},
        job_kwargs={"misfire_grace_time": 300, "coalesce": True},
    )
    await update.message.reply_text("✅ Ежедневная сводка включена (21:00 МСК).")

async def disable_digest_for_chat(update, context):
    chat_id = update.effective_chat.id
    await remove_chat_async(chat_id)
    removed = 0
    if context.job_queue:
        for j in context.job_queue.get_jobs_by_name(f"digest_{chat_id}"):
            j.schedule_removal()
            removed += 1
    await update.message.reply_text(
        "🛑 Ежедневная сводка отключена."
        if removed or True
        else "ℹ️ Сводка уже была отключена."
    )

# === создание и запуск Telegram-приложения (в lifecycle FastAPI) ===
_tg_app: Optional[Application] = None

async def _start_telegram():
    global _tg_app
    _tg_app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .job_queue(JobQueue())
        .build()
    )

    await init_db()
    for chat_id in await list_chats_async():
        _tg_app.job_queue.run_daily(
            callback=daily_digest,
            time=time(hour=21, minute=0, tzinfo=LOCAL_TZ),
            name=f"digest_{chat_id}",
            data={"chat_id": chat_id},
        )
        log.info("Restored daily digest for chat %s", chat_id)

    _tg_app.add_handler(CommandHandler("start", start_cmd, block=False))
    _tg_app.add_handler(CommandHandler("startdigest", enable_digest_for_chat))
    _tg_app.add_handler(CommandHandler("stopdigest", disable_digest_for_chat))
    _tg_app.add_handler(CommandHandler("stats", stats_cmd, block=False))

    await _tg_app.initialize()
    await _tg_app.start()
    await _tg_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    log.info("Telegram bot started")

async def _stop_telegram():
    if _tg_app:
        await _tg_app.updater.stop()
        await _tg_app.stop()
        await _tg_app.shutdown()
        log.info("Telegram bot stopped")

# =========================
# FastAPI lifecycle (один event loop)
# =========================

@app.on_event("startup")
async def on_startup():
    ensure_config()
    # HTTP-клиент
    global _http_client
    _http_client = httpx.AsyncClient()
    # Telegram в фоне
    await _start_telegram()

@app.on_event("shutdown")
async def on_shutdown():
    # гасим бота
    await _stop_telegram()
    # закрываем http-клиент
    global _http_client
    if _http_client:
        await _http_client.aclose()
        _http_client = None
    # гасим задачу, если осталась
    tg = getattr(app.state, "tg_task", None)
    if tg:
        tg.cancel()
        with suppress(asyncio.CancelledError):
            await tg
    log.info("Shutdown complete")
