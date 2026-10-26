# Personal Telegram Video Vault v2

## Deploy
1. Choose **Python 3.11 or 3.12** in your host.
2. Add `bot.py` and `requirements.txt` to your project.
3. Set environment variables in your host dashboard:
   - `BOT_TOKEN`: token from BotFather
   - `DATABASE_URL`: PostgreSQL connection URL
   - `ADMIN_IDS`: optional comma-separated Telegram numeric IDs; if omitted, the IDs from your original bot are used
   - `BATCH_TIMEOUT`: optional, defaults to `60`
   - `PGSSLMODE`: `require` for hosted PostgreSQL (default); set `disable` only if your provider explicitly requires it
4. Start command: `python bot.py`
5. Deploy and inspect logs if startup fails.

The bot creates its PostgreSQL tables automatically. Keep tokens and database URLs out of public GitHub repositories.

## Features
Private admin checks; persistent index/settings; Telegram `file_unique_id` deduplication; button settings menu; auto/manual batch controls; groups of up to 10; safe one-video sending; timeout watcher; progress messages; search; statistics; retry; JSON index backup. Photos are ignored.

## Important limitations
- Deduplication catches repeated Telegram files using `file_unique_id`. It does not detect different uploads with identical visual content.
- Search checks filenames and captions only.
- Backup exports index/settings metadata, not actual video bytes.
- All permanent state lives in PostgreSQL, so the bot service does not need a persistent local disk. Your PostgreSQL provider must retain data and have enough free-tier capacity. A free host may sleep or stop background services; code cannot override provider limits.
- The original admin IDs are retained as defaults. You can override them with `ADMIN_IDS`.
