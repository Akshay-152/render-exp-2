# Audio Uploader: Windows and PowerShell

## Start locally

Open PowerShell in the `render` directory. Configure the required service values in `render/.env` first, then run:

```powershell
Set-Location "C:\programing\antigravity\musix server\render"
python -m pip install -r .\requirements.txt
$env:PORT = "10000"
python .\main.py
```

The terminal should print `http://127.0.0.1:10000`. Open that address in the browser. `/send` serves the same full web app. It includes URL/playlist uploads and the Firestore duplicate cleaner; deleting duplicates removes Firestore documents only, not Cloudinary files.

The browser page calls its own server, so it does not need the old hard-coded Codespaces URL. The standalone `send.html` uses `http://127.0.0.1:10000` when opened from disk; set its API server field only when connecting to a different server.

## Test the API from PowerShell

Starting a job returns immediately. HTTP 200 and a `task_id` mean the server accepted the job; they do not mean the media downloaded or uploaded successfully.

```powershell
$body = @{ urls = @('https://youtu.be/VIDEO_ID') } | ConvertTo-Json -Compress
$job = Invoke-RestMethod `
    -Uri 'http://127.0.0.1:10000/api/start' `
    -Method Post `
    -ContentType 'application/json' `
    -Body $body
$job
```

Poll until processing finishes:

```powershell
do {
    $status = Invoke-RestMethod `
        -Uri "http://127.0.0.1:10000/api/status/$($job.task_id)" `
        -Method Get
    $status | Select-Object status, processed, total, success, failed, current_title
    if (-not $status.done) { Start-Sleep -Seconds 2 }
} until ($status.done)

$status.logs
```

The final status is `done` when all tracks succeed, `partial` when some fail, `failed` when all tracks fail, or `cancelled` when cancellation was requested. The response includes `success`, `failed`, `logs`, and `done`.

Upload progress is weighted by stage: metadata 5%, YouTube download 60%, MP3 conversion 12%, Cloudinary upload 20%, and Firestore save 3%. For a playlist, each song contributes an equal share of the overall 100%; completed and failed songs are counted once, and the current song advances within its stage. The status response includes `overall_percent`, `current_index`, `completed_tracks`, and `track_progress`.

The terminal prints the same stage and playlist percentages at meaningful checkpoints, plus download/upload byte counts and speed where available. Set `$env:LOG_LEVEL = "DEBUG"` only when you need lower-level yt-dlp retry details; routine fallback errors are not printed as final failures.

## Duplicate cleaner jobs

The web cleaner runs scans and deletes in background jobs so the page stays responsive:

```powershell
$scan = Invoke-RestMethod -Uri 'http://127.0.0.1:10000/api/clean/scan' -Method Post
Invoke-RestMethod -Uri "http://127.0.0.1:10000/api/clean/status/$($scan.job_id)"
```

Poll `/api/clean/status/JOB_ID` until `done` is true. The response reports `phase`, `percent`, `processed`, and `total`; completed scans include the `scan_id` and duplicate groups. A scan can be cancelled with `POST /api/clean/cancel/JOB_ID`. Deletion is also asynchronous and still removes Firestore documents only; Cloudinary files are untouched.

While a media file is transferring, the page and terminal report downloaded bytes, percentage, and speed. Low-level yt-dlp errors from clients that are being retried are hidden at the default log level; final failures remain visible. To include yt-dlp diagnostic detail in the terminal, set `$env:LOG_LEVEL = "DEBUG"` before starting the server.

## Local JSON task history

The server writes the latest 25 task snapshots and their recent logs to `render/task_history.json`. The file is created automatically and ignored by Git because logs can contain submitted URLs. It lets the page reconnect to a previous task after a refresh or server restart. A job that was still running when the server stopped is marked failed; background work cannot resume after process shutdown.

Inspect recent task summaries from PowerShell while in the `render` directory:

```powershell
$history = Get-Content .\task_history.json -Raw | ConvertFrom-Json
$history.tasks.PSObject.Properties | ForEach-Object {
    [pscustomobject]@{
        TaskId = $_.Name
        Status = $_.Value.status
        Success = $_.Value.success
        Failed = $_.Value.failed
        Total = $_.Value.total
    }
}
```

Set `$env:TASK_HISTORY_PATH` before starting the server to choose another JSON file location. Render's local filesystem is ephemeral, so this file is for local development and will not provide durable history across redeploys.

## Diagnose HTTP 403

The start request can succeed while a later YouTube download fails. In that case the job log and terminal show the downloader error, and the final status is `failed` or `partial`; this is not a failure of the browser-to-Flask connection.

An `HTTP Error 403: Forbidden` means YouTube denied the media request. The stream URL may have expired, or the session/account or server network may not be allowed to fetch it. Check that yt-dlp is current and that `COOKIES_FILE` points to a readable, fresh Netscape-format cookie export. A valid cookie-file header only confirms its format, not that YouTube accepts the session. Never commit or share cookies.

## Remote access

The server binds to `0.0.0.0`; the terminal prints its LAN address. For Codespaces, use the forwarded port matching `PORT` (10000 by default). Do not expose this app to untrusted networks: the API currently has no authentication, and CORS is permissive.

The Flask development-server warning is expected when running `python .\main.py` locally. The Docker deployment uses Gunicorn instead.
