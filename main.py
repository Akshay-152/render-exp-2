import os
import uuid
import threading
import json
from queue import Queue
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, render_template, request, Response, jsonify
from flask_cors import CORS

import uploader_core
from uploader_core import process_all, expand_all_urls

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Store active tasks
tasks = {}
TASK_HISTORY_PATH = os.getenv(
    "TASK_HISTORY_PATH",
    os.path.join(os.path.dirname(__file__), "task_history.json"),
)
TASK_HISTORY_LIMIT = 25
TRACK_STAGE_WEIGHTS = {"metadata": 5, "download": 60, "compress": 12, "upload": 20, "save": 3}
CLEAN_JOB_LIMIT = 50
_task_history = {}
_task_history_lock = threading.Lock()

# In-memory cache of the last cleaner scan (keyed by a scan id) so the client
# can review duplicates and then request deletions in a second call.
clean_scans = {}
clean_jobs = {}


class Task:
    def __init__(self, task_id, restored=None):
        self.id = task_id
        self.status = "pending"  # pending → expanding → running → done/cancelled/failed
        self.total = 0
        self.processed = 0
        self.current_title = ""
        self.logs = []
        self.success = 0
        self.failed = 0
        self.error = ""
        self.done = False
        self.cancelled = False
        self.download_progress = {}
        self._last_persisted_percent = 0
        self._last_persisted_stage = ""
        self._revision = 0
        self._persisted_track_progress = {}
        self.track_progress = {}
        self.completed_tracks = set()
        self.overall_percent = 0.0
        self.current_index = 0
        # Set by the cancel endpoint; checked by process_all between tracks.
        self.cancel_event = threading.Event()
        self._queue = Queue()
        # Guard shared mutable state updated from multiple worker threads.
        self._lock = threading.Lock()
        if restored:
            self.status = restored.get("status", "failed")
            self.total = restored.get("total", 0)
            self.processed = restored.get("processed", 0)
            self.current_title = restored.get("current_title", "")
            self.logs = restored.get("logs", [])[-50:]
            self.success = restored.get("success", 0)
            self.failed = restored.get("failed", 0)
            self.error = restored.get("error", "")
            self.done = restored.get("done", True)
            self.cancelled = restored.get("cancelled", False)
            self.download_progress = restored.get("download_progress", {})
            self._last_persisted_percent = int(self.download_progress.get("percent") or 0)
            self._last_persisted_stage = restored.get("_last_persisted_stage", "")
            self._revision = restored.get("_revision", 0)
            self._persisted_track_progress = restored.get("_persisted_track_progress", {})
            self.track_progress = restored.get("track_progress", {})
            self.completed_tracks = set(restored.get("completed_tracks", []))
            self.overall_percent = restored.get("overall_percent", 0.0)
            self.current_index = restored.get("current_index", 0)
            if not self.done:
                self.status = "failed"
                self.done = True
                self.error = "Server restarted before this job finished; the job was not resumed."
                self.logs.append(self.error)

    def set_expanding(self):
        with self._lock:
            self.status = "expanding"
            self.done = False

    def update(self, data):
        self._queue.put(data)
        persist = True
        # Thread-safe state updates for concurrent processing.
        with self._lock:
            t = data.get("type")
            if t == "start":
                self.total = data.get("total", 0)
                self.status = "running"
            elif t == "track_done":
                if not data.get("ok") and data.get("title") != "Cancelled":
                    self.failed += 1
                if data.get("title"):
                    self.current_title = data["title"]
                index = data.get("index")
                if index is not None and data.get("title") != "Cancelled":
                    index_key = str(index)
                    self.completed_tracks.add(index)
                    self.track_progress[index_key] = {
                        **self.track_progress.get(index_key, {}),
                        "index": index,
                        "stage": "complete" if data.get("ok") else "failed",
                        "percent": 100.0,
                        "title": data.get("title", ""),
                        "finished": True,
                        "success": bool(data.get("ok")),
                    }
                    self._recalculate_overall_progress()
            elif t == "progress":
                if data.get("processed") is not None:
                    self.processed = data["processed"]
                if data.get("title"):
                    self.current_title = data["title"]
            elif t == "track_start":
                self.download_progress = {}
                self._last_persisted_percent = 0
                self._last_persisted_stage = ""
                self.current_index = data.get("index", 0)
                if data.get("title"):
                    self.current_title = data["title"]
            elif t == "track_stage":
                index_key = str(data.get("index", 0))
                stage = data.get("stage", "")
                percent = data.get("percent")
                index = data.get("index", 0)
                previous = self.track_progress.get(index_key, {})
                persisted = self._persisted_track_progress.get(index_key, {})
                if stage != persisted.get("stage"):
                    self._persisted_track_progress[index_key] = {"stage": stage, "percent": 0}
                    persist = True
                elif percent is None:
                    persist = False
                elif percent >= persisted.get("percent", 0) + 10 or percent >= 100:
                    self._persisted_track_progress[index_key] = {"stage": stage, "percent": int(percent)}
                else:
                    persist = False
                stage_percent = percent if percent is not None else previous.get("percent", 0)
                if stage == previous.get("stage"):
                    stage_percent = max(stage_percent or 0, previous.get("percent", 0))
                self.current_index = data.get("index", self.current_index)
                self.current_title = data.get("title") or self.current_title
                self.track_progress[index_key] = {
                    "index": index,
                    "stage": stage,
                    "percent": max(0.0, min(100.0, float(stage_percent or 0))),
                    "title": self.current_title,
                    "downloaded_bytes": data.get("downloaded_bytes", previous.get("downloaded_bytes", 0)),
                    "total_bytes": data.get("total_bytes", previous.get("total_bytes", 0)),
                    "uploaded_bytes": data.get("uploaded_bytes", previous.get("uploaded_bytes", 0)),
                    "speed": data.get("speed", previous.get("speed")),
                    "eta": data.get("eta", previous.get("eta")),
                }
                if stage in ("complete", "failed"):
                    self.completed_tracks.add(index)
                self._recalculate_overall_progress()
            elif t == "download_progress":
                self.download_progress = {
                    key: data.get(key)
                    for key in ("status", "downloaded_bytes", "total_bytes", "percent", "speed", "eta")
                }
                percent = data.get("percent")
                if percent is None:
                    persist = data.get("status") == "finished"
                elif percent >= self._last_persisted_percent + 10 or data.get("status") == "finished":
                    self._last_persisted_percent = int(percent)
                else:
                    persist = False
                if data.get("title"):
                    self.current_title = data["title"]
            elif t == "finish":
                self.done = True
                self.success = data.get("success", 0)
                self.failed = max(self.failed, data.get("failed", 0))
                self.processed = self.total
                if self.cancelled:
                    self.status = "cancelled"
                elif self.failed >= self.total and self.total > 0:
                    self.status = "failed"
                elif self.failed:
                    self.status = "partial"
                else:
                    self.status = "done"
                if self.status != "cancelled":
                    self.overall_percent = 100.0
            elif t == "error":
                self.status = "failed"
                self.done = True
                self.error = data.get("message", "Task failed")
                uploader_core.logger.error("task %s failed: %s", self.id, self.error)
            elif t == "log":
                message = data.get("message", "")
                self.logs.append(message)
                if len(self.logs) > 50:
                    self.logs = self.logs[-50:]
                if message.startswith("❌"):
                    pass
                elif message.startswith("⚠️"):
                    uploader_core.logger.warning("task %s: %s", self.id, message)
                else:
                    uploader_core.logger.info("task %s: %s", self.id, message)
            self._revision += 1

        if t == "finish":
            uploader_core.logger.info(
                "task %s finished with status=%s uploaded=%s failed=%s total=%s",
                self.id,
                self.status,
                self.success,
                self.failed,
                self.total,
            )
        if t == "track_stage" and persist:
            percent = data.get("percent")
            percent_text = f"{percent:.0f}%" if percent is not None else "starting"
            stage = data.get("stage", "processing")
            track_index = data.get("index", self.current_index)
            detail = ""
            byte_count = data.get("downloaded_bytes")
            byte_total = data.get("total_bytes")
            if byte_count is not None and byte_total:
                detail = f" ({byte_count}/{byte_total} bytes)"
            elif data.get("uploaded_bytes") is not None and data.get("total_bytes"):
                detail = f" ({data['uploaded_bytes']}/{data['total_bytes']} bytes uploaded)"
            speed = data.get("speed")
            if speed:
                detail += f" at {speed / 1024 / 1024:.2f} MiB/s"
            uploader_core.logger.info(
                "task %s: song %s/%s %s %s%s; playlist %.1f%% complete",
                self.id,
                track_index,
                self.total,
                stage,
                percent_text,
                detail,
                self.overall_percent,
            )
        if t == "download_progress" and persist and data.get("percent") is not None:
            speed = data.get("speed")
            speed_text = f", {speed / 1024 / 1024:.2f} MiB/s" if speed else ""
            uploader_core.logger.info(
                "task %s: download %.0f%% (%s/%s bytes%s)",
                self.id,
                data["percent"],
                data.get("downloaded_bytes", 0),
                data.get("total_bytes", 0),
                speed_text,
            )
        if persist:
            _persist_task(self.id, self.snapshot())

    def snapshot(self):
        with self._lock:
            return {
                "status": self.status,
                "total": self.total,
                "processed": self.processed,
                "current_title": self.current_title,
                "success": self.success,
                "failed": self.failed,
                "error": self.error,
                "download_progress": dict(self.download_progress),
                "logs": list(self.logs),
                "done": self.done,
                "cancelled": self.cancelled,
                "_revision": self._revision,
                "_persisted_track_progress": dict(self._persisted_track_progress),
                "track_progress": {key: dict(value) for key, value in self.track_progress.items()},
                "completed_tracks": list(self.completed_tracks),
                "overall_percent": self.overall_percent,
                "current_index": self.current_index,
            }

    def _recalculate_overall_progress(self):
        if not self.total:
            return
        completed = len(self.completed_tracks)
        stage_names = list(TRACK_STAGE_WEIGHTS)
        current_weighted = 0.0
        for index, state in self.track_progress.items():
            if int(index) in self.completed_tracks:
                continue
            stage = state.get("stage")
            if stage not in TRACK_STAGE_WEIGHTS:
                continue
            stage_position = stage_names.index(stage)
            current_weighted += sum(TRACK_STAGE_WEIGHTS[name] for name in stage_names[:stage_position])
            current_weighted += TRACK_STAGE_WEIGHTS[stage] * state.get("percent", 0) / 100
        self.overall_percent = min(100.0, (completed * 100 + current_weighted) / self.total)

    def stream_events(self):
        # Emit an initial state snapshot so a (re)connecting client instantly
        # sees where things stand instead of waiting for the next event.
        yield f"data: {json.dumps({'type': 'state', **self.snapshot()})}\n\n"
        while not self.done or not self._queue.empty():
            try:
                data = self._queue.get(timeout=1)
                yield f"data: {json.dumps(data)}\n\n"
            except Exception:
                yield ": keepalive\n\n"


class CleanerJob:
    def __init__(self, job_id):
        self.id = job_id
        self.status = "queued"
        self.phase = "Waiting to start"
        self.percent = 0
        self.processed = 0
        self.total = 0
        self.scan_id = None
        self.result = None
        self.error = ""
        self.done = False
        self.cancel_event = threading.Event()
        self._lock = threading.Lock()

    def update(self, **values):
        with self._lock:
            for key, value in values.items():
                setattr(self, key, value)

    def snapshot(self):
        with self._lock:
            return {
                "job_id": self.id,
                "status": self.status,
                "phase": self.phase,
                "percent": self.percent,
                "processed": self.processed,
                "total": self.total,
                "scan_id": self.scan_id,
                "result": self.result,
                "error": self.error,
                "done": self.done,
                "cancelled": self.cancel_event.is_set(),
            }


def _register_clean_job(job):
    clean_jobs[job.id] = job
    while len(clean_jobs) > CLEAN_JOB_LIMIT:
        completed_id = next(
            (job_id for job_id, candidate in clean_jobs.items() if candidate.done and job_id != job.id),
            None,
        )
        if completed_id is None:
            break
        clean_jobs.pop(completed_id, None)


def _persist_task(task_id, snapshot):
    with _task_history_lock:
        previous = _task_history.get(task_id)
        if previous and previous.get("_revision", 0) > snapshot.get("_revision", 0):
            return
        _task_history[task_id] = snapshot
        while len(_task_history) > TASK_HISTORY_LIMIT:
            _task_history.pop(next(iter(_task_history)))
        directory = os.path.dirname(TASK_HISTORY_PATH) or "."
        os.makedirs(directory, exist_ok=True)
        temporary_path = f"{TASK_HISTORY_PATH}.{os.getpid()}.{threading.get_ident()}.tmp"
        try:
            with open(temporary_path, "w", encoding="utf-8") as history_file:
                json.dump({"version": 1, "tasks": _task_history}, history_file, indent=2)
                history_file.write("\n")
            os.replace(temporary_path, TASK_HISTORY_PATH)
        except OSError as error:
            uploader_core.logger.warning("Could not persist task history: %s", error)
            try:
                os.remove(temporary_path)
            except OSError:
                pass


def _restore_task_history():
    try:
        with open(TASK_HISTORY_PATH, "r", encoding="utf-8") as history_file:
            records = json.load(history_file).get("tasks", {})
    except FileNotFoundError:
        return
    except (OSError, ValueError) as error:
        uploader_core.logger.warning("Could not load task history: %s", error)
        return

    for task_id, snapshot in list(records.items())[-TASK_HISTORY_LIMIT:]:
        task = Task(task_id, restored=snapshot)
        tasks[task_id] = task
        _task_history[task_id] = task.snapshot()
        if snapshot.get("done") is False:
            uploader_core.logger.warning("Recovered interrupted task %s as failed", task_id)

    if any(snapshot.get("done") is False for snapshot in records.values()):
        for task_id, task in tasks.items():
            if not task.done or task.error == "Server restarted before this job finished; the job was not resumed.":
                _persist_task(task_id, task.snapshot())


_restore_task_history()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/send")
def send_page():
    return render_template("index.html")


def _extract_urls(payload):
    urls_value = payload.get("urls", "")
    if isinstance(urls_value, list):
        urls_text = "\n".join(str(url) for url in urls_value)
    else:
        urls_text = str(urls_value)
    return [line.strip() for line in urls_text.splitlines() if line.strip()]


@app.route("/start", methods=["POST"])
@app.route("/api/start", methods=["POST"])
def start_upload():
    """Start a task and return IMMEDIATELY.

    Playlist expansion (yt-dlp flat-playlist / Spotify API) can take minutes
    for large playlists; doing it inside the request would hit Render's proxy
    timeout and kill the request. Instead the client polls /api/status and
    watches the task go: expanding → running → done.
    """
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    raw_urls = _extract_urls(payload)
    if not raw_urls:
        return jsonify({"error": "No URLs provided"}), 400

    task_id = str(uuid.uuid4())
    task = Task(task_id)
    tasks[task_id] = task

    def run():
        def report_callback(**kwargs):
            task.update(kwargs)
        try:
            task.set_expanding()
            task.update({"type": "log", "message": "🔍 Expanding playlists / resolving URLs..."})
            expanded = expand_all_urls(raw_urls)
            if not expanded:
                task.update({"type": "error", "message": "No valid tracks found"})
                return
            task.update({"type": "log", "message": f"✓ {len(expanded)} track(s) to process"})
            # Honour a cancel that arrived during expansion.
            if task.cancel_event.is_set():
                task.update({"type": "log", "message": "⏹️ Cancelled during expansion."})
                task.update({"type": "finish", "success": 0, "total": len(expanded)})
                return
            # Pass the task's cancel_event so the cancel endpoint can stop it.
            process_all(expanded, report_callback, cancel_event=task.cancel_event)
        except Exception as e:
            task.update({"type": "error", "message": str(e)})

    threading.Thread(target=run, daemon=True).start()
    # Respond instantly — the client starts polling right away.
    return jsonify({"task_id": task_id, "total": None})


@app.route("/progress/<task_id>")
@app.route("/api/progress/<task_id>")
def progress_stream(task_id):
    task = tasks.get(task_id)
    if not task:
        return "Task not found", 404
    return Response(
        task.stream_events(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.route("/status/<task_id>")
@app.route("/api/status/<task_id>")
def status(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "not found"}), 404
    return jsonify(task.snapshot())


# ==========================================================
# CANCEL
# ==========================================================
@app.route("/cancel/<task_id>", methods=["POST"])
@app.route("/api/cancel/<task_id>", methods=["POST"])
def cancel(task_id):
    """Request cancellation of a running task.

    Tracks mid-download finish their current step and clean up; no further
    tracks are submitted. Already-completed tracks stay uploaded.
    """
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "not found"}), 404
    if task.done:
        return jsonify({"error": "task already finished", "status": task.status}), 409

    task.cancelled = True
    task.cancel_event.set()
    task.update({"type": "log", "message": "⏹️ Cancellation requested by user."})
    return jsonify({"ok": True, "task_id": task_id, "message": "Cancellation requested"})


# ==========================================================
# DUPLICATE CLEANER — scan + delete (Firestore only)
# ==========================================================
def _head_size(url):
    if not url:
        return 0
    try:
        r = requests.head(url, timeout=10, allow_redirects=True)
        return int(r.headers.get("Content-Length", 0) or 0)
    except Exception:
        return 0


def _rank_group(group, progress_callback=None):
    """Attach file sizes via parallel HEAD requests and rank best-first."""
    with ThreadPoolExecutor(max_workers=min(uploader_core.MAX_WORKERS, max(1, len(group)))) as ex:
        futs = {ex.submit(_head_size, t["audioUrl"]): t for t in group}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                t["size"] = fut.result()
            except Exception:
                t["size"] = 0
            if progress_callback:
                progress_callback()
    group.sort(key=lambda t: (t["size"], t["createdAt"]), reverse=True)
    for i, t in enumerate(group):
        t["keep"] = (i == 0)
    return group


def _normalize(s):
    s = (s or "").strip().lower()
    s = " ".join(s.split())
    import re
    s = re.sub(r"[\(\[\{][^\)\]\}]*[\)\]\}]", "", s).strip()
    return s


def _scan_duplicates(progress_callback=None, cancel_event=None):
    if progress_callback:
        progress_callback(phase="Fetching Firestore library", percent=5, processed=0, total=0)
    tracks = uploader_core.fetch_all_tracks_rest()
    if cancel_event and cancel_event.is_set():
        raise InterruptedError("Duplicate scan cancelled")
    groups = defaultdict(list)
    for t in tracks:
        groups[(_normalize(t["title"]), _normalize(t["artist"]))].append(t)
    dupes = {k: v for k, v in groups.items() if len(v) > 1}

    duplicate_track_count = sum(len(group) for group in dupes.values())
    ranked_count = [0]
    ranked_lock = threading.Lock()

    def track_ranked():
        with ranked_lock:
            ranked_count[0] += 1
            processed = ranked_count[0]
        if progress_callback:
            percent = 20 + 75 * processed / max(1, duplicate_track_count)
            progress_callback(
                phase="Checking duplicate file sizes",
                percent=percent,
                processed=processed,
                total=duplicate_track_count,
            )

    ranked = {}
    with ThreadPoolExecutor(max_workers=uploader_core.MAX_WORKERS) as ex:
        futs = {ex.submit(_rank_group, g, track_ranked): k for k, g in dupes.items()}
        for fut in as_completed(futs):
            ranked[futs[fut]] = fut.result()
            if cancel_event and cancel_event.is_set():
                raise InterruptedError("Duplicate scan cancelled")

    result_groups = []
    total_deletable = 0
    for (title, artist), group in ranked.items():
        losers = [t for t in group if not t["keep"]]
        total_deletable += len(losers)
        result_groups.append({
            "title": title,
            "artist": artist,
            "keep": group[0],
            "duplicates": losers,
        })

    result = {
        "total_tracks": len(tracks),
        "duplicate_groups": len(result_groups),
        "deletable_tracks": total_deletable,
        "groups": result_groups,
    }
    if progress_callback:
        progress_callback(
            phase="Duplicate scan complete",
            percent=100,
            processed=duplicate_track_count,
            total=duplicate_track_count,
        )
    return result


@app.route("/clean/scan", methods=["POST", "GET"])
@app.route("/api/clean/scan", methods=["POST", "GET"])
def clean_scan():
    """Start an asynchronous duplicate scan; GET retains the legacy sync API."""
    if request.method == "GET":
        try:
            scan_id = str(uuid.uuid4())
            result = _scan_duplicates()
            clean_scans[scan_id] = result
            return jsonify({"scan_id": scan_id, **result})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    job_id = str(uuid.uuid4())
    job = CleanerJob(job_id)
    _register_clean_job(job)

    def run_scan():
        job.update(status="running", phase="Starting duplicate scan")
        try:
            result = _scan_duplicates(
                progress_callback=job.update,
                cancel_event=job.cancel_event,
            )
            if job.cancel_event.is_set():
                job.update(status="cancelled", phase="Scan cancelled", done=True)
                return
            scan_id = str(uuid.uuid4())
            clean_scans[scan_id] = result
            if len(clean_scans) > 10:
                for old_id in list(clean_scans)[:-10]:
                    clean_scans.pop(old_id, None)
            job.update(
                status="done",
                phase="Duplicate scan complete",
                percent=100,
                processed=job.total,
                scan_id=scan_id,
                result={"scan_id": scan_id, **result},
                done=True,
            )
        except InterruptedError:
            job.update(status="cancelled", phase="Scan cancelled", done=True)
        except Exception as error:
            uploader_core.logger.exception("Duplicate scan %s failed", job_id)
            job.update(status="failed", phase="Scan failed", error=str(error), done=True)

    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({"job_id": job_id, "status": job.status}), 202


@app.route("/api/clean/status/<job_id>")
def clean_job_status(job_id):
    job = clean_jobs.get(job_id)
    if not job:
        return jsonify({"error": "cleaner job not found"}), 404
    return jsonify(job.snapshot())


@app.route("/api/clean/cancel/<job_id>", methods=["POST"])
def clean_job_cancel(job_id):
    job = clean_jobs.get(job_id)
    if not job:
        return jsonify({"error": "cleaner job not found"}), 404
    if job.done:
        return jsonify({"error": "cleaner job already finished", "status": job.status}), 409
    job.cancel_event.set()
    job.update(phase="Cancellation requested")
    return jsonify({"ok": True, "job_id": job_id})


@app.route("/clean/delete", methods=["POST"])
@app.route("/api/clean/delete", methods=["POST"])
def clean_delete():
    """Start a cancellable, background Firestore-only duplicate deletion job."""
    payload = request.get_json(silent=True) or {}
    doc_ids = list(payload.get("doc_ids") or [])
    scan_id = payload.get("scan_id")

    if scan_id and payload.get("delete_all"):
        scan = clean_scans.get(scan_id)
        if not scan:
            return jsonify({"error": "scan not found or expired, run /api/clean/scan again"}), 404
        doc_ids = [t["docId"] for g in scan["groups"] for t in g["duplicates"]]
    elif scan_id and not doc_ids:
        scan = clean_scans.get(scan_id)
        if not scan:
            return jsonify({"error": "scan not found or expired, run /api/clean/scan again"}), 404
        doc_ids = [t["docId"] for g in scan["groups"] for t in g["duplicates"]]

    if not doc_ids:
        return jsonify({"error": "No duplicate documents to delete"}), 400

    job_id = str(uuid.uuid4())
    job = CleanerJob(job_id)
    job.total = len(doc_ids)
    _register_clean_job(job)

    def delete_duplicates():
        deleted, failed = [], []
        job.update(status="running", phase="Deleting Firestore documents")
        for index, doc_id in enumerate(doc_ids, 1):
            if job.cancel_event.is_set():
                break
            if uploader_core.delete_firestore_doc(doc_id):
                deleted.append(doc_id)
            else:
                failed.append(doc_id)
            job.update(
                processed=index,
                total=len(doc_ids),
                percent=index / len(doc_ids) * 100,
                phase=f"Deleting duplicate {index}/{len(doc_ids)}",
            )
        cancelled = job.cancel_event.is_set()
        job.update(
            status="cancelled" if cancelled else "done",
            phase="Deletion cancelled" if cancelled else "Deletion complete",
            percent=(len(deleted) + len(failed)) / len(doc_ids) * 100,
            result={
                "deleted": deleted,
                "failed": failed,
                "deleted_count": len(deleted),
                "failed_count": len(failed),
            },
            done=True,
        )

    threading.Thread(target=delete_duplicates, daemon=True).start()
    return jsonify({"job_id": job_id, "status": job.status}), 202


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"Listening on port {port}", flush=True)
    print(f"Local URL: http://127.0.0.1:{port}", flush=True)
    print(f"Network URL: http://0.0.0.0:{port}", flush=True)
    # threaded=True: status polling must stay responsive while background
    # upload tasks run (Render deploys rely on this).
    app.run(host="0.0.0.0", port=port, threaded=True)
