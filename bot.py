"""Private Telegram Video Vault. Requires BOT_TOKEN and DATABASE_URL."""
import os, io, json, time, asyncio, logging
from datetime import datetime, date, timezone
from functools import wraps
import asyncpg
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
