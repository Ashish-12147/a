# Advanced Railway Instagram Reels Automation

This is a script-only automation service. It does not use Hermes or any LLM.

## What it does

The service stays online on Railway and provides:

- 4 scheduled Instagram Reel posts per day by default
- Default times: 09:30, 13:30, 17:30, 21:30 IST
- Google Drive source queue
- Numeric Reel ordering
- Persistent SQLite duplicate protection
- Retry / failed / skipped / uncertain states
- Deterministic rotating captions
- Telegram control dashboard
- Telegram success and failure reports
- Daily Telegram report at 22:15 IST
- Manual posting with confirmation
- Pause / resume
- Schedule changes without redeploying
- Next Reel and caption preview
- Dry-run validation
- Manual skip and retry controls
- Health endpoint for Railway

## Required environment variables

Instagram / Drive:

- DRIVE_FOLDER_ID
- INSTAGRAM_USER_ID
- INSTAGRAM_ACCESS_TOKEN

Telegram:

- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

Aliases are also supported:
- INSTAGRAM_TOKEN
- INSTAGRAM_USER_ACCESS_TOKEN
- TELEGRAM_TOKEN
- TG_BOT_TOKEN
- TG_CHAT_ID

For multiple owner chats:

- TELEGRAM_ALLOWED_CHAT_IDS=-100123...,123456...

Never print or paste the access token values publicly.

## Railway volume

Attach a Railway Volume to the service.

Recommended mount path:

    /opt/data

The service automatically uses `RAILWAY_VOLUME_MOUNT_PATH` when Railway provides it.

Persistent files include:

- instagram_reels.sqlite3
- instagram_reel_poster.lock
- temporary Reel download directory

Do not wipe the volume after live posting starts unless you intentionally want to reset local posting history.

## Telegram commands

- /panel
- /status
- /next
- /stats
- /queue
- /refresh
- /history [n]
- /errors [n]
- /postnow
- /dryrun
- /pause
- /resume
- /schedule
- /schedule 09:30 13:30 17:30 21:30
- /skipnext
- /retry REEL_NUMBER
- /report
- /health
- /help

The inline dashboard includes buttons for the main controls.

## Default reports

After every scheduled/manual posting attempt, the bot reports:

- trigger
- resulting state
- Reel number
- filename
- attempt count
- execution duration
- Instagram media ID when available
- permalink when available
- caption
- error when present
- total posted
- next scheduled run

At 22:15 IST it sends a daily summary.

## Important if reusing the old Hermes Telegram bot

Telegram long polling and webhook delivery are mutually exclusive.

If Hermes is still using the same Telegram bot token, stop/disable the Hermes Telegram process or use a separate bot token for this service. This app calls `deleteWebhook` on startup and becomes the long-polling consumer.

## First deployment

1. Upload this project to GitHub or deploy the folder to Railway.
2. Attach a volume at `/opt/data`.
3. Copy/preserve the required environment variables.
4. Deploy.
5. Open the Telegram bot and send `/panel`.
6. Run `/dryrun`.
7. Confirm numeric order and the correct next candidate.
8. Leave the automation running.

No Railway Cron is needed. The internal timezone-aware scheduler runs inside `app.py`.

## Optional variables

- REEL_POST_TIMES=09:30,13:30,17:30,21:30
- DAILY_REPORT_TIME=22:15
- DAILY_REPORT_ENABLED=true
- STARTUP_REPORT_ENABLED=true
- AUTOMATION_TIMEZONE=Asia/Kolkata
- MAX_ATTEMPTS=3
- SEED_POSTED_THROUGH=1
- DRIVE_CATALOG_CACHE_SECONDS=600
- INSTAGRAM_SHARE_TO_FEED=true
- INSTAGRAM_API_VERSION=v23.0
