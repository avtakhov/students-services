#!/usr/bin/env python3
import asyncio
import html
import logging
import os
import pathlib
import re
from collections import defaultdict
from contextlib import suppress
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Iterable, AsyncIterator
from urllib.parse import quote, urlparse

import aiosqlite
import httpx
import pytz
from dateutil import parser as dateparser
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool
from starlette.templating import Jinja2Templates
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
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

QR_IDS_ENV = [s.strip() for s in os.getenv("QR_IDS", "").split(",") if
              s.strip()]

EVENT_VISIT = {"Link", "Visit"}
EVENT_SUB = {"Subscribe", "Subscription", "Follow"}
EVENT_UNSUB = {"Unsubscribe", "Unfollow"}

STATS_RE = re.compile(
    r"^/stats\s+(\S+)\s+(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})\s*-\s*(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})$",
    re.IGNORECASE,
)
STATS_TODAY_RE = re.compile(r"^/stats\s+(\S+)$", re.IGNORECASE)

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


LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0"}


def is_localhost_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host in LOCAL_HOSTS
    except Exception:
        return False


def make_markdown_link(text: str, url: str) -> str:
    # не экранируем text — он статический "Полная статистика"
    # url уже percent-encoded в build_stats_link
    return f"[{text}]({url})"


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
    base = PUBLIC_BASE_URL
    if not base.startswith(("http://", "https://")):
        base = "http://" + base
    f_iso = to_iso(dt_from)
    t_iso = to_iso(dt_to)
    f_q = quote(f_iso, safe="")
    t_q = quote(t_iso, safe="")
    return f"{base}/stats/{qr_id}?from={f_q}&to={t_q}"


# =========================
# Async HTTP клиент к stats
# =========================

_http_client: Optional[httpx.AsyncClient] = None


async def api_search_async(
    qr_id: Optional[str],
    dt_from: datetime,
    dt_to: datetime,
    *,
    limit: int = 100,
    start_cursor: Optional[str] = None,
    max_pages: Optional[int] = None,
    max_events: Optional[int] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """
    Асинхронный генератор событий (dict) из API, с автопагинацией.
    """
    assert _http_client is not None, "HTTP client not initialized"
    url = f"{API_BASE_URL}{API_SEARCH_PATH}"

    cursor = start_cursor
    total_events = 0
    page_count = 0

    while True:
        payload: Dict[str, Any] = {
            "from": to_iso(dt_from),
            "to": to_iso(dt_to),
            "qr_id": qr_id,
            "limit": limit,
        }
        if cursor is not None:
            payload["cursor"] = cursor

        log.debug("POST %s payload=%s headers=%s", url, payload, API_HEADERS)
        r = await _http_client.post(
            url, json=payload, headers=API_HEADERS, timeout=30
        )
        r.raise_for_status()
        data = r.json()

        if not isinstance(data, dict) or "events" not in data:
            raise ValueError(f"Unexpected API response shape: {data}")

        events = data.get("events", [])
        has_more = bool(data.get("has_more", False))
        cursor = data.get("next_cursor")

        log.debug(
            "Fetched page %d: %d events, has_more=%s, next_cursor=%s",
            page_count + 1, len(events), has_more, cursor,
        )

        for event in events:
            yield event
            total_events += 1
            if max_events is not None and total_events >= max_events:
                log.info("Reached max_events=%s; stopping", max_events)
                return

        page_count += 1
        if not has_more or not cursor:
            break
        if max_pages is not None and page_count >= max_pages:
            log.info("Reached max_pages=%s; stopping pagination", max_pages)
            break

    log.debug(
        "Completed fetching %d events in %d pages", total_events, page_count
    )


# =========================
# Вспомогательные функции (стрим-агрегация)
# =========================

def escape_md(text: str) -> str:
    return escape_markdown(text, version=2)


def _inc_counts_for_event(
    event: Dict[str, Any], counts: Dict[str, int]
) -> None:
    et = event.get("event_type")
    if et in EVENT_VISIT:
        counts["visits"] += 1
    elif et in EVENT_SUB:
        counts["subs"] += 1
    elif et in EVENT_UNSUB:
        counts["unsubs"] += 1


def _add_event_to_users(
    users: Dict[int, Dict[str, Any]], event: Dict[str, Any]
) -> None:
    info = event.get("vk_user_info") or {}
    uid = info.get("id")
    if uid is None:
        return

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

    ts_raw = event.get("timestamp")
    try:
        ts = dateparser.isoparse(ts_raw)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception:
        return

    etype = event.get("event_type")
    payload = event.get("payload") or {}

    if etype in EVENT_SUB:
        bucket["subscribed_in_range"] = True

    bucket["events"].append((ts, etype, payload))


async def fold_users_and_counts(
    qr_id: Optional[str],
    dt_from: datetime,
    dt_to: datetime,
) -> Tuple[Dict[int, Dict[str, Any]], int, int, int, int]:
    """
    Стримит события и инкрементально собирает:
    - users: структура как раньше (с events по пользователям)
    - visits/subs/unsubs
    - events_count
    """
    users: Dict[int, Dict[str, Any]] = {}
    counts = {"visits": 0, "subs": 0, "unsubs": 0}
    events_count = 0

    async for ev in api_search_async(qr_id, dt_from, dt_to):
        _add_event_to_users(users, ev)
        _inc_counts_for_event(ev, counts)
        events_count += 1

    for b in users.values():
        b["events"].sort(key=lambda t: t[0])

    return users, counts["visits"], counts["subs"], counts[
        "unsubs"], events_count


async def any_event_with_qr(
    qr_id: str,
    dt_from: datetime,
    dt_to: datetime,
) -> bool:
    """Проверяет стримингом, встречается ли указанный qr_id в событиях за период."""
    qr_l = qr_id.lower()
    async for ev in api_search_async(qr_id, dt_from, dt_to, limit=500):
        payload = ev.get("payload") or {}
        ev_qr = (payload.get("qr_id") or payload.get("qrId") or "").lower()
        if ev_qr == qr_l:
            return True
    return False


async def summarize_counts_by_qr_stream(
    dt_from: datetime,
    dt_to: datetime,
    qr_id: Optional[str] = None,
) -> Dict[str, Tuple[int, int, int]]:
    """
    Потоковый аналог summarize_counts_by_qr: без хранения всех events.
    """
    buckets = defaultdict(lambda: [0, 0, 0])  # [visits, subs, unsubs]
    async for ev in api_search_async(qr_id, dt_from, dt_to):
        q = ev.get("payload", {}).get("qr_id")
        if q is None:
            continue
        et = ev.get("event_type")
        if et in EVENT_VISIT:
            buckets[q][0] += 1
        elif et in EVENT_SUB:
            buckets[q][1] += 1
        elif et in EVENT_UNSUB:
            buckets[q][2] += 1
    return {q: (v[0], v[1], v[2]) for q, v in buckets.items()}


def build_summary_message_stream_markdown(
    qr_id: str,
    dt_from: datetime,
    dt_to: datetime,
    visits: int,
    subs: int,
    unsubs: int,
    link_markdown: str | None = None,
) -> str:
    lines = [
        f"📍 *{escape_md(qr_id.upper().replace('QR', 'QR-'))}*",
        f"📅 Период: {escape_md(fmt_local(dt_from))} – {escape_md(fmt_local(dt_to))}",
        "",
        "*Итог:*",
        f"Посещений: *{visits}*",
        f"Подписок: *{subs}*",
        f"Отписок: *{unsubs}*",
    ]
    if link_markdown:
        lines.append("")
        # важно: не прогонять через escape_md — иначе сломаем синтаксис ссылки
        lines.append(link_markdown)
    return "\n".join(lines)


def build_summary_message_stream_plain(
    qr_id: str,
    dt_from: datetime,
    dt_to: datetime,
    visits: int,
    subs: int,
    unsubs: int,
    url: str | None = None,
) -> str:
    lines = [
        f"📍 {qr_id.upper().replace('QR', 'QR-')}",
        f"📅 Период: {fmt_local(dt_from)} – {fmt_local(dt_to)}",
        "",
        "Итог:",
        f"Посещений: {visits}",
        f"Подписок: {subs}",
        f"Отписок: {unsubs}",
    ]
    if url:
        lines.append("")
        lines.append(url)  # отправляем «как есть»
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

class EventOutDict(Dict[str, Any]):
    ...


class UserOutDict(Dict[str, Any]):
    ...


class StatsOutDict(Dict[str, Any]):
    ...


app = FastAPI(title="QR Stats API")


@app.get("/health")
async def health():
    return {"ok": True}


@app.get(
    "/stats/{qr_id}", response_class=HTMLResponse
)
async def http_stats(
    qr_id: str,
    request: Request,
    from_iso: str = Query(..., alias="from"),
    to_iso: str = Query(..., alias="to"),
):
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
        users, visits, subs, unsubs, events_count = await fold_users_and_counts(
            qr_id, dt_from, dt_to
        )

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
            "events_count": events_count,
        }
        # Jinja рендер — синхронный; выносим в поток, чтобы не блокировать loop
        return await run_in_threadpool(
            templates.TemplateResponse, "stats.html", context
        )

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
        await conn.execute(
            "DELETE FROM digest_chats WHERE chat_id = ?", (chat_id,)
        )
        await conn.commit()


async def list_chats_async() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute(
            "SELECT chat_id FROM digest_chats ORDER BY chat_id"
        )
        rows = await cur.fetchall()
        return [r[0] for r in rows]


# =========================
# Telegram (async)
# =========================

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    m = STATS_RE.match(text)
    if m:
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
    else:
        # Формат без даты (/stats qr1) — за сегодня
        m_today = STATS_TODAY_RE.match(text)
        if m_today:
            qr_id = m_today.group(1)
            now_loc = datetime.now(LOCAL_TZ)
            dt_from = now_loc.replace(hour=0, minute=0, second=0, microsecond=0)
            dt_to = now_loc
        else:
            await update.message.reply_text(
                "Неверный формат. Примеры:\n"
                "/stats qr1 — статистика за сегодня\n"
                "/stats qr1 07.09.2025 14:00 - 08.09.2025 11:00 — за период"
            )
            return

    try:
        users, visits, subs, unsubs, events_count = await fold_users_and_counts(
            qr_id, dt_from, dt_to
        )

        # Проверка существования QR если пусто
        if events_count == 0:
            check_from = datetime.now(LOCAL_TZ) - timedelta(days=365)
            check_to = datetime.now(LOCAL_TZ)
            qr_found_ever = await any_event_with_qr(qr_id, check_from, check_to)
            if not qr_found_ever:
                await update.message.reply_text(
                    f"❌ QR-код '{qr_id}' не найден в системе.\n"
                    f"Проверьте правильность написания."
                )
                return

        link = build_stats_link(qr_id, dt_from, dt_to)

        if link and not is_localhost_url(link):
            link_md = make_markdown_link("Полная статистика", link)
            msg = build_summary_message_stream_markdown(
                qr_id, dt_from, dt_to, visits, subs, unsubs,
                link_markdown=link_md
            )
            await update.message.reply_text(
                msg, parse_mode=ParseMode.MARKDOWN_V2,
                disable_web_page_preview=True
            )
        else:
            # localhost / пустая ссылка — отправляем plain text «как есть»
            msg = build_summary_message_stream_plain(
                qr_id, dt_from, dt_to, visits, subs, unsubs, url=link or ""
            )
            await update.message.reply_text(msg, disable_web_page_preview=True)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            await update.message.reply_text(
                f"❌ QR-код '{qr_id}' не найден в системе."
            )
        else:
            log.exception("stats_cmd failed with HTTP error")
            await update.message.reply_text(f"Ошибка при получении данных: {e}")
    except Exception as e:
        log.exception("stats_cmd failed")
        await update.message.reply_text(f"Ошибка при получении данных: {e}")


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я считаю статистику по QR.\n"
        "Формат запроса:\n"
        "/stats qr1 — статистика за сегодня\n"
        "/stats qr1 DD.MM.YYYY HH:MM - DD.MM.YYYY HH:MM — за период\n"
        "Каждый день в 21:00 МСК я отправляю сводку по всем QR (если включено)."
    )


async def daily_digest(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.data.get("chat_id")

    now_loc = datetime.now(LOCAL_TZ)
    day_start = now_loc.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1) - timedelta(milliseconds=1)

    try:
        per_qr = await summarize_counts_by_qr_stream(
            day_start, day_end, qr_id=None
        )

        lines = ["Статистика по всем QR за сегодня.\n"]
        if per_qr:
            for qr_id, (visits, subs, unsubs) in per_qr.items():
                lines.append(f"{qr_id}:")
                lines.append(f"Посещений: {visits}")
                lines.append(f"Подписок: {subs}")
                lines.append(f"Отписок: {unsubs}\n")
        else:
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

    # планируем (21:00 МСК = 00:00 МСК+3 — оставим твои времена)
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
            time=time(hour=21, minute=00, tzinfo=LOCAL_TZ),
            name=f"digest_{chat_id}",
            data={"chat_id": chat_id},
            job_kwargs={"misfire_grace_time": 300, "coalesce": True},
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


@app.on_event("startup")
async def on_startup():
    ensure_config()
    global _http_client
    _http_client = httpx.AsyncClient()
    await _start_telegram()


@app.on_event("shutdown")
async def on_shutdown():
    await _stop_telegram()
    global _http_client
    if _http_client:
        await _http_client.aclose()
        _http_client = None
    tg = getattr(app.state, "tg_task", None)
    if tg:
        tg.cancel()
        with suppress(asyncio.CancelledError):
            await tg
    log.info("Shutdown complete")
