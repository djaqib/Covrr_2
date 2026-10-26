"""Private Telegram Video Vault. Requires BOT_TOKEN and DATABASE_URL."""
import os, io, json, time, asyncio, logging
from datetime import datetime, date, timezone
from functools import wraps
import asyncpg
import functools
import logging
import os
import random
import sqlite3
import time
from dotenv import load_dotenv  # 1. Import it first

# 2. Now call it to load the .env file
load_dotenv()

from telegram import (
    BotCommand, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InputMediaVideo, Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.ext import (
    ApplicationBuilder, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

# -----------------------------
# Config
# -----------------------------
# Set DATA_DIR to a persistent volume (e.g. /data) so dedup history and
# settings survive redeploys. Without a volume they reset on every deploy.
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)

# Admin IDs: set ADMIN_IDS="123,456" as an env var, or edit the fallback below.
_env_ids = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = {int(x) for x in _env_ids.replace(" ", "").split(",") if x.isdigit()} or {
    7599601301,  # KR
    8637601933,
    8976017144,
    7467197752,
}

# Per-user setting defaults (the type of the default decides how it's parsed)
DEFAULTS = {
    "delete_original": True,   # delete the message you sent after processing
    "dedup": True,             # skip media already seen
    "strict_dedup": False,     # also match re-uploads by size/duration/resolution
    "album_size": 10,          # 1-10 items per album
    "timeout": 60,             # seconds of inactivity before auto-send
    "shuffle": False,          # shuffle order of the batch before sending
    "protect": False,          # protect_content: block forwarding/saving of output
    "photo_approval": False,   # hold photos until /approve or /reject
}
TOGGLES = {k for k, v in DEFAULTS.items() if isinstance(v, bool)}
RANGES = {"album_size": (1, 10, 1), "timeout": (15, 600, 15)}  # min, max, step

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, "bot.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# -----------------------------
# Storage (SQLite: settings, dedup history, counters)
# -----------------------------
class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                user_id INTEGER, key TEXT, value TEXT,
                PRIMARY KEY (user_id, key));
            CREATE TABLE IF NOT EXISTS seen (
                user_id INTEGER, key TEXT, ts REAL,
                PRIMARY KEY (user_id, key));
            CREATE TABLE IF NOT EXISTS counters (
                user_id INTEGER, key TEXT, value INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, key));
            """
        )
        self.db.commit()

    # settings
    def get(self, uid, key):
        default = DEFAULTS[key]
        row = self.db.execute(
            "SELECT value FROM settings WHERE user_id=? AND key=?", (uid, key)
        ).fetchone()
        if row is None:
            return default
        return row[0] == "1" if isinstance(default, bool) else int(row[0])

    def set(self, uid, key, value):
        raw = "1" if value is True else "0" if value is False else str(value)
        self.db.execute(
            "INSERT INTO settings VALUES (?,?,?) "
            "ON CONFLICT(user_id, key) DO UPDATE SET value=excluded.value",
            (uid, key, raw),
        )
        self.db.commit()

    # dedup
    def is_dup(self, uid, uniq, fingerprint, strict):
        keys = ["u:" + uniq]
        if strict:
            keys.append("f:" + fingerprint)
        marks = ",".join("?" * len(keys))
        row = self.db.execute(
            f"SELECT 1 FROM seen WHERE user_id=? AND key IN ({marks}) LIMIT 1",
            (uid, *keys),
        ).fetchone()
        return row is not None

    def mark_seen(self, uid, uniq, fingerprint):
        now = time.time()
        self.db.executemany(
            "INSERT OR IGNORE INTO seen VALUES (?,?,?)",
            [(uid, "u:" + uniq, now), (uid, "f:" + fingerprint, now)],
        )
        self.db.commit()

    def seen_count(self, uid):
        return self.db.execute(
            "SELECT COUNT(*) FROM seen WHERE user_id=? AND key LIKE 'u:%'", (uid,)
        ).fetchone()[0]

    def clear_seen(self, uid):
        self.db.execute("DELETE FROM seen WHERE user_id=?", (uid,))
        self.db.commit()

    # counters
    def incr(self, uid, key, n=1):
        self.db.execute(
            "INSERT INTO counters VALUES (?,?,?) "
            "ON CONFLICT(user_id, key) DO UPDATE SET value=value+excluded.value",
            (uid, key, n),
        )
        self.db.commit()

    def counters(self, uid):
        rows = self.db.execute(
            "SELECT key, value FROM counters WHERE user_id=?", (uid,)
        ).fetchall()
        return dict(rows)


store = Store(os.path.join(DATA_DIR, "bot.db"))


# -----------------------------
# Per-user batch queue (in memory)
# -----------------------------
class UserQueue:
    def __init__(self):
        self.items = []          # list of (kind, file_id)
        self.chat_id = None
        self.timer = None        # asyncio TimerHandle for auto-flush
        self.lock = asyncio.Lock()
        self.skipped = 0         # duplicates skipped in the current batch
        self.status_id = None    # message id of the live status message
        self.last_status = 0.0


QUEUES = {}
BG_TASKS = set()


def queue_for(uid):
    return QUEUES.setdefault(uid, UserQueue())


def spawn(coro):
    task = asyncio.create_task(coro)
    BG_TASKS.add(task)
    task.add_done_callback(BG_TASKS.discard)
    return task


# -----------------------------
# Helpers
# -----------------------------
def admin_only(func):
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id not in ADMIN_IDS:
            if update.callback_query:
                await update.callback_query.answer("Access denied.", show_alert=True)
            elif update.effective_message:
                await update.effective_message.reply_text("This bot is private. Access denied.")
            return
        return await func(update, context)
    return wrapper


async def with_retry(fn, tries=3):
    for attempt in range(tries):
        try:
            return await fn()
        except RetryAfter as e:
            wait = e.retry_after
            wait = wait.total_seconds() if hasattr(wait, "total_seconds") else float(wait)
            if attempt == tries - 1:
                raise
            logger.warning("Flood control, sleeping %.0fs", wait + 1)
            await asyncio.sleep(wait + 1)


def extract_media(msg):
    """Return (kind, file_id, file_unique_id, fingerprint) or None."""
    if msg.video:
        v = msg.video
        return "video", v.file_id, v.file_unique_id, f"v{v.duration}-{v.file_size}-{v.width}x{v.height}"
    if msg.document and (msg.document.mime_type or "").startswith("video/"):
        d = msg.document
        return "video", d.file_id, d.file_unique_id, f"d{d.file_size}-{d.mime_type}"
    if msg.photo:
        p = msg.photo[-1]
        return "photo", p.file_id, p.file_unique_id, f"p{p.width}x{p.height}-{p.file_size}"
    return None


async def try_delete(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramError:
        pass


# -----------------------------
# Sending
# -----------------------------
async def send_one(bot, chat_id, item, protect):
    kind, fid = item
    if kind == "photo":
        return await with_retry(lambda: bot.send_photo(chat_id, fid, protect_content=protect))
    try:
        return await with_retry(lambda: bot.send_video(chat_id, fid, protect_content=protect))
    except BadRequest:
        # videos sent as "files" can need send_document
        return await with_retry(lambda: bot.send_document(chat_id, fid, protect_content=protect))


async def send_chunk(bot, chat_id, chunk, protect):
    """Send up to 10 items as one album. Returns how many were sent."""
    if len(chunk) == 1:
        try:
            await send_one(bot, chat_id, chunk[0], protect)
            return 1
        except TelegramError as e:
            logger.warning("Single send failed: %s", e)
            return 0

    media = [
        InputMediaPhoto(fid) if kind == "photo" else InputMediaVideo(fid)
        for kind, fid in chunk
    ]
    try:
        await with_retry(lambda: bot.send_media_group(chat_id, media, protect_content=protect))
        return len(chunk)
    except TelegramError as e:
        logger.warning("Album failed (%s), falling back to one-by-one", e)

    sent = 0
    for item in chunk:
        try:
            await send_one(bot, chat_id, item, protect)
            sent += 1
        except TelegramError as e:
            logger.warning("Item failed: %s", e)
        await asyncio.sleep(0.5)
    return sent


async def flush_queue(bot, uid):
    q = queue_for(uid)
    async with q.lock:
        if q.timer:
            q.timer.cancel()
            q.timer = None

        items, q.items = q.items, []
        skipped, q.skipped = q.skipped, 0
        status_id, q.status_id = q.status_id, None

        if not items and not skipped:
            return 0

        sent = 0
        if items:
            if store.get(uid, "shuffle"):
                random.shuffle(items)
            size = store.get(uid, "album_size")
            protect = store.get(uid, "protect")
            for i in range(0, len(items), size):
                if i:
                    await asyncio.sleep(random.uniform(1.5, 3))
                sent += await send_chunk(bot, q.chat_id, items[i:i + size], protect)
            store.incr(uid, "sent", sent)

        parts = []
        if items:
            parts.append(f"✅ Sent {sent} item(s)")
            if sent < len(items):
                parts.append(f"⚠️ {len(items) - sent} failed")
        if skipped:
            parts.append(f"♻️ {skipped} duplicate(s) skipped")
        summary = "\n".join(parts)

        try:
            if status_id:
                await bot.edit_message_text(summary, q.chat_id, status_id)
            else:
                await bot.send_message(q.chat_id, summary)
        except TelegramError:
            pass
        return sent


async def refresh_status(bot, uid):
    q = queue_for(uid)
    now = time.time()
    if now - q.last_status < 2:  # throttle edits
        return
    q.last_status = now

    text = f"📥 Queued: {len(q.items)}"
    if q.skipped:
        text += f"\n♻️ Duplicates skipped: {q.skipped}"
    text += f"\n⏳ Auto-send after {store.get(uid, 'timeout')}s idle · /flush to send now"
    try:
        if q.status_id:
            await bot.edit_message_text(text, q.chat_id, q.status_id)
        else:
            m = await bot.send_message(q.chat_id, text)
            q.status_id = m.message_id
    except BadRequest as e:
        if "not found" in str(e).lower() or "can't be edited" in str(e).lower():
            q.status_id = None
    except TelegramError as e:
        logger.warning("Status update failed: %s", e)


async def touch(bot, uid):
    """Re-arm the inactivity timer and refresh the status message."""
    q = queue_for(uid)
    if q.timer:
        q.timer.cancel()
    loop = asyncio.get_running_loop()
    q.timer = loop.call_later(
        store.get(uid, "timeout"), lambda: spawn(flush_queue(bot, uid))
    )
    await refresh_status(bot, uid)


async def enqueue(bot, chat_id, uid, kind, fid, uniq, fp):
    q = queue_for(uid)
    q.chat_id = chat_id
    q.items.append((kind, fid))
    store.mark_seen(uid, uniq, fp)
    if len(q.items) >= store.get(uid, "album_size"):
        spawn(flush_queue(bot, uid))
    else:
        await touch(bot, uid)


# -----------------------------
# Media handler
# -----------------------------
@admin_only
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    uid = update.effective_user.id
    media = extract_media(msg)
    if not media:
        return
    kind, fid, uniq, fp = media

    store.incr(uid, "received")
    q = queue_for(uid)
    q.chat_id = msg.chat_id

    # 1) Dedup
    if store.get(uid, "dedup") and store.is_dup(uid, uniq, fp, store.get(uid, "strict_dedup")):
        store.incr(uid, "dupes")
        q.skipped += 1
        if store.get(uid, "delete_original"):
            await try_delete(context.bot, msg.chat_id, msg.message_id)
        await touch(context.bot, uid)
        return

    # 2) Photo approval (single pending slot)
    if kind == "photo" and store.get(uid, "photo_approval"):
        context.user_data["pending_photo"] = {
            "fid": fid, "uniq": uniq, "fp": fp,
            "chat_id": msg.chat_id, "msg_id": msg.message_id,
        }
        await msg.reply_text("📷 Photo held. Use /approve or /reject.")
        return

    # 3) Queue it
    if store.get(uid, "delete_original"):
        await try_delete(context.bot, msg.chat_id, msg.message_id)
    await enqueue(context.bot, msg.chat_id, uid, kind, fid, uniq, fp)


# -----------------------------
# Commands
# -----------------------------
@admin_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 <b>Media Batcher</b>\n"
        "Send me videos or photos. I skip duplicates, strip captions and "
        "forwarding info, and send them back as clean albums.\n\n"
        f"🆔 Your ID: <code>{update.effective_user.id}</code>\n\n"
        "/settings · /help",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>Commands</b>\n"
        "/settings – Settings menu\n"
        "/stats – Your counters\n"
        "/queue – What's waiting to be sent\n"
        "/flush – Send the queue now\n"
        "/clear – Discard the queue\n"
        "/resetdedup – Forget all seen media\n"
        "/approve · /reject – Photo approval mode\n"
        "/start – Show your Telegram ID",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def queue_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = queue_for(update.effective_user.id)
    await update.message.reply_text(
        f"📥 Queued: {len(q.items)}\n♻️ Duplicates skipped this batch: {q.skipped}"
    )


@admin_only
async def flush(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    q = queue_for(uid)
    if not q.items:
        await update.message.reply_text("Nothing queued.")
        return
    q.chat_id = update.effective_chat.id
    await flush_queue(context.bot, uid)


@admin_only
async def clear_queue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    q = queue_for(uid)
    n = len(q.items)
    async with q.lock:
        q.items = []
        q.skipped = 0
        q.status_id = None
        if q.timer:
            q.timer.cancel()
            q.timer = None
    await update.message.reply_text(f"🗑 Discarded {n} queued item(s).")


@admin_only
async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    p = context.user_data.pop("pending_photo", None)
    if not p:
        await update.message.reply_text("No pending photo.")
        return
    if store.get(uid, "delete_original"):
        await try_delete(context.bot, p["chat_id"], p["msg_id"])
    await enqueue(context.bot, p["chat_id"], uid, "photo", p["fid"], p["uniq"], p["fp"])
    await update.message.reply_text("✅ Photo approved and queued.")


@admin_only
async def reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.pop("pending_photo", None):
        await update.message.reply_text("🚫 Photo rejected.")
    else:
        await update.message.reply_text("No pending photo.")


def stats_text(uid):
    c = store.counters(uid)
    q = queue_for(uid)
    return (
        "📊 <b>Stats</b>\n\n"
        f"Received: <b>{c.get('received', 0)}</b>\n"
        f"Sent: <b>{c.get('sent', 0)}</b>\n"
        f"Duplicates skipped: <b>{c.get('dupes', 0)}</b>\n"
        f"Currently queued: <b>{len(q.items)}</b>\n"
        f"Dedup history: <b>{store.seen_count(uid)}</b> items"
    )


@admin_only
async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(stats_text(update.effective_user.id), parse_mode=ParseMode.HTML)


# -----------------------------
# Settings menu (inline keyboard, edits in place)
# -----------------------------
def settings_text(uid):
    return (
        "⚙️ <b>Settings</b>\n"
        "Tap to change — applies instantly.\n\n"
        f"📦 Album size: <b>{store.get(uid, 'album_size')}</b> items\n"
        f"⏱ Auto-send after: <b>{store.get(uid, 'timeout')}s</b> idle\n"
        f"♻️ Dedup history: <b>{store.seen_count(uid)}</b> items\n\n"
        "<i>Strict dedup also catches re-uploads by matching size, length and "
        "resolution (rarely, two different videos could match).</i>"
    )


def settings_kb(uid):
    def tog(label, key):
        on = store.get(uid, key)
        return InlineKeyboardButton(f"{label} {'✅' if on else '❌'}", callback_data=f"t:{key}")

    def stepper(label, key, unit=""):
        return [
            InlineKeyboardButton("➖", callback_data=f"a:{key}:-1"),
            InlineKeyboardButton(f"{label}: {store.get(uid, key)}{unit}", callback_data="noop"),
            InlineKeyboardButton("➕", callback_data=f"a:{key}:1"),
        ]

    return InlineKeyboardMarkup([
        [tog("🗑 Delete originals", "delete_original")],
        [tog("♻️ Dedup", "dedup"), tog("🔍 Strict", "strict_dedup")],
        stepper("📦 Album", "album_size"),
        stepper("⏱ Timeout", "timeout", "s"),
        [tog("🔀 Shuffle", "shuffle"), tog("🔒 Protect", "protect")],
        [tog("📷 Photo approval", "photo_approval")],
        [
            InlineKeyboardButton("📊 Stats", callback_data="m:stats"),
            InlineKeyboardButton("🧹 Clear dedup", callback_data="m:clear_ask"),
        ],
        [InlineKeyboardButton("✖ Close", callback_data="m:close")],
    ])


CONFIRM_KB = InlineKeyboardMarkup([[
    InlineKeyboardButton("Yes, clear it", callback_data="m:clear_yes"),
    InlineKeyboardButton("Cancel", callback_data="m:menu"),
]])
BACK_KB = InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="m:menu")]])


@admin_only
async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await update.message.reply_text(
        settings_text(uid), parse_mode=ParseMode.HTML, reply_markup=settings_kb(uid)
    )


@admin_only
async def resetdedup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⚠️ Forget all seen media? Previously sent items will be accepted again.",
        reply_markup=CONFIRM_KB,
    )


async def edit_menu(cq, text, kb):
    try:
        await cq.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


@admin_only
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cq = update.callback_query
    uid = cq.from_user.id
    data = cq.data or ""
    parts = data.split(":")

    if data == "noop":
        await cq.answer()
        return

    if parts[0] == "t" and len(parts) == 2 and parts[1] in TOGGLES:
        store.set(uid, parts[1], not store.get(uid, parts[1]))
        await cq.answer()
        await edit_menu(cq, settings_text(uid), settings_kb(uid))

    elif parts[0] == "a" and len(parts) == 3 and parts[1] in RANGES:
        lo, hi, step = RANGES[parts[1]]
        value = store.get(uid, parts[1]) + int(parts[2]) * step
        store.set(uid, parts[1], max(lo, min(hi, value)))
        await cq.answer()
        await edit_menu(cq, settings_text(uid), settings_kb(uid))

    elif data == "m:menu":
        await cq.answer()
        await edit_menu(cq, settings_text(uid), settings_kb(uid))

    elif data == "m:stats":
        await cq.answer()
        await edit_menu(cq, stats_text(uid), BACK_KB)

    elif data == "m:clear_ask":
        await cq.answer()
        await edit_menu(
            cq,
            "⚠️ <b>Clear dedup history?</b>\nPreviously sent items will be accepted again.",
            CONFIRM_KB,
        )

    elif data == "m:clear_yes":
        store.clear_seen(uid)
        await cq.answer("Dedup history cleared")
        await edit_menu(cq, settings_text(uid), settings_kb(uid))

    elif data == "m:close":
        await cq.answer()
        try:
            await cq.message.delete()
        except TelegramError:
            pass

    else:
        await cq.answer()


# -----------------------------
# Startup
# -----------------------------
async def post_init(app):
    await app.bot.set_my_commands([
        BotCommand("settings", "Open settings menu"),
        BotCommand("stats", "Show counters"),
        BotCommand("queue", "Show queued items"),
        BotCommand("flush", "Send the queue now"),
        BotCommand("clear", "Discard the queue"),
        BotCommand("resetdedup", "Forget all seen media"),
        BotCommand("approve", "Approve pending photo"),
        BotCommand("reject", "Reject pending photo"),
        BotCommand("help", "Show help"),
        BotCommand("start", "Show your Telegram ID"),
    ])
    logger.info("Post-init tasks completed.")


async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled error", exc_info=context.error)


def main():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN environment variable is missing!")

    app = ApplicationBuilder().token(token).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("settings", settings_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("queue", queue_cmd))
    app.add_handler(CommandHandler("flush", flush))
    app.add_handler(CommandHandler("clear", clear_queue))
    app.add_handler(CommandHandler("resetdedup", resetdedup_cmd))
    app.add_handler(CommandHandler("approve", approve))
    app.add_handler(CommandHandler("reject", reject))
    app.add_handler(CallbackQueryHandler(on_button))

    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.VIDEO | filters.PHOTO, handle_media
    ))

    app.add_error_handler(on_error)

    logger.info("Bot is now polling...")
    app.run_polling()


if __name__ == "__main__":
    main()
from telegram import Update, InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup, InputMediaVideo, BotCommand
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

TOKEN = os.getenv("BOT_TOKEN", "").strip()
DSN = os.getenv("DATABASE_URL", "").strip()
DEFAULT_ADMINS = {7599601301, 8637601933, 8976017144, 7467197752}
ADMINS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()} or DEFAULT_ADMINS
DEFAULT_TIMEOUT = max(10, int(os.getenv("BATCH_TIMEOUT", "60")))
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("video-vault")
last_video = {}
locks = {}
progress = {}

SCHEMA = """
CREATE TABLE IF NOT EXISTS videos (
 id BIGSERIAL PRIMARY KEY, owner_id BIGINT NOT NULL, chat_id BIGINT NOT NULL,
 file_id TEXT NOT NULL, unique_id TEXT, file_name TEXT, caption TEXT,
 file_size BIGINT, duration INTEGER, status TEXT NOT NULL DEFAULT 'queued',
 received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), sent_at TIMESTAMPTZ, last_error TEXT
);
CREATE INDEX IF NOT EXISTS videos_lookup ON videos(owner_id, unique_id);
CREATE INDEX IF NOT EXISTS videos_queue ON videos(owner_id, chat_id, status, received_at, id);
CREATE TABLE IF NOT EXISTS settings (
 owner_id BIGINT PRIMARY KEY, dedup BOOLEAN NOT NULL DEFAULT TRUE,
 auto_mode BOOLEAN NOT NULL DEFAULT TRUE, batch_size INTEGER NOT NULL DEFAULT 10,
 timeout_sec INTEGER NOT NULL DEFAULT 60, progress_every INTEGER NOT NULL DEFAULT 50
);
CREATE TABLE IF NOT EXISTS events (
 id BIGSERIAL PRIMARY KEY, owner_id BIGINT NOT NULL, kind TEXT NOT NULL, detail TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

def admin(fn):
    @wraps(fn)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE):
        u = update.effective_user
        if not u or u.id not in ADMINS:
            q = update.callback_query
            if q:
                await q.answer("Private bot: access denied.", show_alert=True)
            elif update.effective_message:
                await update.effective_message.reply_text("This bot is private. Access denied.")
            return
        return await fn(update, context)
    return wrapped

async def pool(context):
    p = context.application.bot_data.get("pool")
    if not p: raise RuntimeError("PostgreSQL pool is unavailable")
    return p

async def settings(p, uid):
    async with p.acquire() as c:
        await c.execute("INSERT INTO settings(owner_id,timeout_sec) VALUES($1,$2) ON CONFLICT DO NOTHING", uid, DEFAULT_TIMEOUT)
        return await c.fetchrow("SELECT * FROM settings WHERE owner_id=$1", uid)

async def set_setting(p, uid, key, value):
    if key not in {"dedup","auto_mode","batch_size","timeout_sec","progress_every"}: raise ValueError("Invalid setting")
    async with p.acquire() as c:
        await c.execute(f"UPDATE settings SET {key}=$2 WHERE owner_id=$1", uid, value)

def home():
    return Markup([
        [Btn("⚙️ Settings", callback_data="menu:settings"), Btn("📊 Statistics", callback_data="menu:stats")],
        [Btn("🔎 Search", callback_data="menu:search"), Btn("🔁 Retry failed", callback_data="act:retry")],
        [Btn("📦 Finish batch", callback_data="act:finish"), Btn("🛑 Cancel queue", callback_data="act:cancel")],
        [Btn("💾 Backup index", callback_data="act:backup"), Btn("❔ Help", callback_data="menu:help")]
    ])

async def settings_menu(p, uid):
    s = await settings(p, uid)
    return Markup([
        [Btn(f"Deduplication: {'ON ✅' if s['dedup'] else 'OFF ⛔'}", callback_data="set:dedup")],
        [Btn(f"Collection: {'Auto + manual' if s['auto_mode'] else 'Manual only'}", callback_data="set:auto_mode")],
        [Btn(f"Batch size: {s['batch_size']}", callback_data="pick:size")],
        [Btn(f"Timeout: {s['timeout_sec']} sec", callback_data="pick:timeout")],
        [Btn(f"Progress: every {s['progress_every']}", callback_data="pick:progress")],
        [Btn("Photos: ignored 📷", callback_data="photos")],
        [Btn("⬅️ Main menu", callback_data="menu:home")]
    ])

async def edit(q, text, markup=None, mode=None):
    try: await q.edit_message_text(text, reply_markup=markup, parse_mode=mode)
    except TelegramError:
        try: await q.message.reply_text(text, reply_markup=markup, parse_mode=mode)
        except TelegramError: log.exception("Could not show callback result")

async def stats_text(p, uid):
    async with p.acquire() as c:
        r = await c.fetchrow("""SELECT COUNT(*) FILTER(WHERE status<>'skipped') total,
          COUNT(*) FILTER(WHERE status='sent') sent, COUNT(*) FILTER(WHERE status='queued') queued,
          COUNT(*) FILTER(WHERE status='failed') failed FROM videos WHERE owner_id=$1""", uid)
        d = await c.fetchval("SELECT COUNT(*) FROM events WHERE owner_id=$1 AND kind='duplicate'", uid)
    return f"📊 *Collection statistics*\n\nIndexed: {r['total']}\nSent: {r['sent']}\nQueued: {r['queued']}\nFailed: {r['failed']}\nDuplicates skipped: {d}"

async def backup(bot, chat_id, p, uid):
    async with p.acquire() as c:
        vs = await c.fetch("SELECT id,chat_id,file_id,unique_id,file_name,caption,file_size,duration,status,received_at,sent_at,last_error FROM videos WHERE owner_id=$1 ORDER BY id", uid)
        s = await c.fetchrow("SELECT * FROM settings WHERE owner_id=$1", uid)
    def conv(x):
        if isinstance(x, (datetime,date)): return x.isoformat()
        return str(x)
    data = json.dumps({"exported_at":datetime.now(timezone.utc).isoformat(),"owner_id":uid,
      "note":"Index/settings only, not video bytes.","settings":dict(s) if s else {},
      "videos":[dict(v) for v in vs]}, indent=2, default=conv).encode()
    f=io.BytesIO(data); f.name=f"video-vault-backup-{uid}.json"
    await bot.send_document(chat_id, f, caption=f"Backup: {len(vs)} indexed records. Keep this file private.")

def lock_for(key):
    if key not in locks: locks[key] = asyncio.Lock()
    return locks[key]

async def flush(app, uid, chat_id, force=False):
    p=app.bot_data["pool"]; key=(uid,chat_id)
    async with lock_for(key):
        s=await settings(p,uid); size=min(10,max(2,int(s["batch_size"])))
        while True:
            async with p.acquire() as c:
                async with c.transaction():
                    rows=await c.fetch("""SELECT id,file_id FROM videos WHERE owner_id=$1 AND chat_id=$2
                      AND status='queued' ORDER BY received_at,id LIMIT $3 FOR UPDATE SKIP LOCKED""",uid,chat_id,size)
                    if not rows: return
                    if len(rows)<size and not force:
                        last=last_video.get(key)
                        if last is not None and time.monotonic()-last<int(s["timeout_sec"]): return
                    ids=[r["id"] for r in rows]
                    await c.execute("UPDATE videos SET status='sending' WHERE id=ANY($1::bigint[])",ids)
            try:
                if len(rows)==1: await app.bot.send_video(chat_id,rows[0]["file_id"])
                else: await app.bot.send_media_group(chat_id,[InputMediaVideo(r["file_id"]) for r in rows])
                async with p.acquire() as c:
                    await c.execute("UPDATE videos SET status='sent',sent_at=NOW(),last_error=NULL WHERE id=ANY($1::bigint[])",ids)
                async with p.acquire() as c:
                    await c.execute("INSERT INTO events(owner_id,kind,detail) VALUES($1,'sent',$2)",uid,f"{len(rows)} videos")
                if len(rows)<size: return
            except (RetryAfter,TimedOut,NetworkError,TelegramError) as e:
                async with p.acquire() as c:
                    await c.execute("UPDATE videos SET status='failed',last_error=$2 WHERE id=ANY($1::bigint[])",ids,str(e)[:1000])
                log.exception("Batch send failed")
                try: await app.bot.send_message(chat_id,"⚠️ Sending failed. Use /retry after checking the connection.")
                except TelegramError: pass
                return
            except Exception as e:
                async with p.acquire() as c:
                    await c.execute("UPDATE videos SET status='failed',last_error=$2 WHERE id=ANY($1::bigint[])",ids,str(e)[:1000])
                log.exception("Unexpected send failure"); return

@admin
async def start(u,c):
    await settings(await pool(c),u.effective_user.id)
    await u.effective_message.reply_text("🎞️ *Personal Video Vault*\n\nSend videos here. Repeated Telegram files are skipped. Photos are ignored.\n\nFull batches send automatically when enabled; smaller batches send after the timeout or when you press Finish.",parse_mode="Markdown",reply_markup=home())

@admin
async def help_cmd(u,c):
    await u.effective_message.reply_text("Commands:\n/start - Main menu\n/settings - Settings\n/collect - Start manual session\n/finish - Send queue now\n/cancel - Clear queue, keep index\n/search words - Search filename/caption\n/stats - Counts\n/retry - Retry failed sends\n/backup - Export index/settings JSON",reply_markup=home())

@admin
async def settings_cmd(u,c):
    p=await pool(c)
    await u.effective_message.reply_text("⚙️ *Settings*\nTap a row to change it.",parse_mode="Markdown",reply_markup=await settings_menu(p,u.effective_user.id))

@admin
async def collect_cmd(u,c):
    c.user_data["manual"]=True
    await u.effective_message.reply_text("🟢 Manual session started. Send videos, then press Finish batch.",reply_markup=home())

@admin
async def finish_cmd(u,c):
    await flush(c.application,u.effective_user.id,u.effective_chat.id,True)
    c.user_data["manual"]=False
    await u.effective_message.reply_text("Finished sending queued videos.",reply_markup=home())

@admin
async def cancel_cmd(u,c):
    p=await pool(c)
    async with p.acquire() as db:
        r=await db.execute("UPDATE videos SET status='skipped' WHERE owner_id=$1 AND chat_id=$2 AND status='queued'",u.effective_user.id,u.effective_chat.id)
    await u.effective_message.reply_text(f"Cancelled queued records ({r.split()[-1]}). Index entries remain.",reply_markup=home())

@admin
async def stats_cmd(u,c):
    await u.effective_message.reply_text(await stats_text(await pool(c),u.effective_user.id),parse_mode="Markdown",reply_markup=home())

@admin
async def search_cmd(u,c):
    term=" ".join(c.args).strip()
    if not term:
        await u.effective_message.reply_text("Use /search words. Search checks filenames and captions."); return
    p=await pool(c)
    async with p.acquire() as db:
        rows=await db.fetch("""SELECT id,file_name,caption,status FROM videos WHERE owner_id=$1 AND
          (COALESCE(file_name,'') ILIKE $2 OR COALESCE(caption,'') ILIKE $2)
          ORDER BY received_at DESC LIMIT 15""",u.effective_user.id,f"%{term[:150]}%")
    if not rows: await u.effective_message.reply_text("No matches found."); return
    lines=["🔎 Search results:"]
    for r in rows: lines.append(f"#{r['id']} [{r['status']}] {(r['file_name'] or r['caption'] or 'Unnamed video').replace(chr(10),' ')[:100]}")
    await u.effective_message.reply_text("\n".join(lines))

@admin
async def retry_cmd(u,c):
    p=await pool(c)
    async with p.acquire() as db:
        await db.execute("UPDATE videos SET status='queued',last_error=NULL WHERE owner_id=$1 AND chat_id=$2 AND status='failed'",u.effective_user.id,u.effective_chat.id)
    await flush(c.application,u.effective_user.id,u.effective_chat.id,True)
    await u.effective_message.reply_text("Retry attempt finished. Check /stats for remaining failures.",reply_markup=home())

@admin
async def backup_cmd(u,c):
    await backup(c.bot,u.effective_chat.id,await pool(c),u.effective_user.id)

async def video_handler(u,c):
    user=u.effective_user; msg=u.effective_message
    if not user or user.id not in ADMINS:
        if msg: await msg.reply_text("This bot is private. Access denied.")
        return
    if not msg or not msg.video: return
    p=await pool(c); uid=user.id; chat=u.effective_chat.id; key=(uid,chat)
    s=await settings(p,uid); v=msg.video; duplicate=False
    async with p.acquire() as db:
        async with db.transaction():
            # Serialize duplicate check+insert per user; hash collisions only serialize, not corrupt data.
            lock_id=abs(hash((uid,v.file_unique_id))) % (2**62)
            await db.execute("SELECT pg_advisory_xact_lock($1)",lock_id)
            if s["dedup"]:
                old=await db.fetchval("SELECT id FROM videos WHERE owner_id=$1 AND unique_id=$2 LIMIT 1",uid,v.file_unique_id)
                duplicate=old is not None
                if duplicate:
                    await db.execute("INSERT INTO events(owner_id,kind,detail) VALUES($1,'duplicate',$2)",uid,f"duplicate of #{old}")
            if not duplicate:
                rid=await db.fetchval("""INSERT INTO videos(owner_id,chat_id,file_id,unique_id,file_name,caption,file_size,duration)
                  VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id""",uid,chat,v.file_id,v.file_unique_id,v.file_name,msg.caption,v.file_size,v.duration)
    if duplicate:
        try: await msg.delete()
        except TelegramError: pass
        return
    last_video[key]=time.monotonic(); progress[uid]=progress.get(uid,0)+1
    try: await msg.delete()
    except TelegramError: log.info("Input message not deleted; video %s is indexed",rid)
    if progress[uid] % max(1,int(s["progress_every"])) == 0:
        try: await c.bot.send_message(chat,f"📥 Received {progress[uid]} new videos this run. Latest record #{rid}.")
        except TelegramError: log.warning("Progress message failed")
    if s["auto_mode"]: await flush(c.application,uid,chat,False)

async def watcher(c):
    p=c.application.bot_data["pool"]
    try:
        async with p.acquire() as db:
            queued=await db.fetch("SELECT DISTINCT owner_id,chat_id FROM videos WHERE status='queued'")
        now=time.monotonic()
        for r in queued:
            key=(r["owner_id"],r["chat_id"]); s=await settings(p,r["owner_id"]); last=last_video.get(key)
            if s["auto_mode"] and (last is None or now-last>=int(s["timeout_sec"])):
                await flush(c.application,key[0],key[1],True)
    except Exception: log.exception("Batch watcher error")

@admin
async def callback(u,c):
    q=u.callback_query
    await q.answer()
    uid=u.effective_user.id; chat=q.message.chat_id; p=await pool(c); d=q.data or ""
    if d=="menu:home": await edit(q,"🎞️ *Personal Video Vault*\nChoose an action:",home(),"Markdown")
    elif d=="menu:settings": await edit(q,"⚙️ *Settings*\nTap a row to change it.",await settings_menu(p,uid),"Markdown")
    elif d=="menu:stats": await edit(q,await stats_text(p,uid),home(),"Markdown")
    elif d=="menu:help": await edit(q,"Send videos here. Use /collect, /finish, /cancel, /search words, /retry, and /backup.",home())
    elif d=="menu:search": await edit(q,"Send `/search words` to search filenames and captions.",home(),"Markdown")
    elif d.startswith("set:"):
        key=d.split(":")[1]; s=await settings(p,uid); await set_setting(p,uid,key,not s[key])
        await edit(q,"✅ Setting saved.",await settings_menu(p,uid))
    elif d=="pick:size":
        await edit(q,"Choose batch size:",Markup([[Btn("5",callback_data="val:size:5"),Btn("10",callback_data="val:size:10")],[Btn("⬅️ Back",callback_data="menu:settings")]]))
    elif d=="pick:timeout":
        await edit(q,"Choose timeout:",Markup([[Btn("15 sec",callback_data="val:timeout:15"),Btn("30 sec",callback_data="val:timeout:30")],[Btn("60 sec",callback_data="val:timeout:60"),Btn("120 sec",callback_data="val:timeout:120")],[Btn("⬅️ Back",callback_data="menu:settings")]]))
    elif d=="pick:progress":
        await edit(q,"Progress update frequency:",Markup([[Btn("25",callback_data="val:progress:25"),Btn("50",callback_data="val:progress:50"),Btn("100",callback_data="val:progress:100")],[Btn("⬅️ Back",callback_data="menu:settings")]]))
    elif d=="photos": await edit(q,"📷 Photos are ignored, as requested.",Markup([[Btn("⬅️ Back",callback_data="menu:settings")]]))
    elif d.startswith("val:"):
        _,k,raw=d.split(":"); field={"size":"batch_size","timeout":"timeout_sec","progress":"progress_every"}[k]
        await set_setting(p,uid,field,int(raw)); await edit(q,"✅ Setting saved.",await settings_menu(p,uid))
    elif d=="act:finish":
        await flush(c.application,uid,chat,True); await edit(q,"📦 Finished sending queued videos.",home())
    elif d=="act:cancel":
        async with p.acquire() as db:
            r=await db.execute("UPDATE videos SET status='skipped' WHERE owner_id=$1 AND chat_id=$2 AND status='queued'",uid,chat)
        await edit(q,f"🛑 Cancelled queued records ({r.split()[-1]}). Index entries remain.",home())
    elif d=="act:retry":
        async with p.acquire() as db:
            await db.execute("UPDATE videos SET status='queued',last_error=NULL WHERE owner_id=$1 AND chat_id=$2 AND status='failed'",uid,chat)
        await flush(c.application,uid,chat,True); await edit(q,"Retry attempt finished. Check Statistics.",home())
    elif d=="act:backup":
        try: await backup(c.bot,chat,p,uid); await edit(q,"💾 Backup sent as a JSON document.",home())
        except TelegramError: log.exception("Backup failed"); await edit(q,"Backup delivery failed. Try /backup.",home())

async def ignore_photo(update, context):
    return

async def post_init(app):
    if not TOKEN or not DSN: raise RuntimeError("Set BOT_TOKEN and DATABASE_URL environment variables.")
    ssl_mode=os.getenv("PGSSLMODE","require").lower()
    p=await asyncpg.create_pool(dsn=DSN,min_size=1,max_size=5,command_timeout=30,ssl="require" if ssl_mode!="disable" else None)
    async with p.acquire() as db:
        await db.execute(SCHEMA)
        for uid in ADMINS:
            await db.execute("INSERT INTO settings(owner_id,timeout_sec) VALUES($1,$2) ON CONFLICT DO NOTHING",uid,DEFAULT_TIMEOUT)
        await db.execute("UPDATE videos SET status='queued',last_error='Recovered after restart' WHERE status='sending'")
    app.bot_data["pool"]=p
    await app.bot.set_my_commands([BotCommand("start","Main menu"),BotCommand("help","Help"),BotCommand("settings","Settings"),
      BotCommand("collect","Start manual session"),BotCommand("finish","Send queued videos"),BotCommand("cancel","Cancel queue"),
      BotCommand("search","Search index"),BotCommand("stats","Statistics"),BotCommand("retry","Retry failed sends"),BotCommand("backup","Export index")])
    app.job_queue.run_repeating(watcher,interval=5,first=5)

async def post_shutdown(app):
    p=app.bot_data.get("pool")
    if p: await p.close()

def main():
    if not TOKEN or not DSN: raise RuntimeError("Set BOT_TOKEN and DATABASE_URL.")
    app=ApplicationBuilder().token(TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    for name,fn in [("start",start),("help",help_cmd),("settings",settings_cmd),("collect",collect_cmd),
      ("finish",finish_cmd),("cancel",cancel_cmd),("stats",stats_cmd),("search",search_cmd),
      ("retry",retry_cmd),("backup",backup_cmd)]:
        app.add_handler(CommandHandler(name,fn))
    app.add_handler(CallbackQueryHandler(callback))
    app.add_handler(MessageHandler(filters.VIDEO,video_handler))
    app.add_handler(MessageHandler(filters.PHOTO,ignore_photo))
    log.info("Starting private Video Vault bot")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__=="__main__": main()
