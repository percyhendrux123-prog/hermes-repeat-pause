"""Repeat comparison and state; no Hermes dependency at import time."""
import fcntl
import hashlib
import json
import logging
import os
import re
import stat
import sys
import time
import unicodedata
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

SILENT = "[SILENT]"
FAILURE = "[CRON_FAILURE]"
STATE_FILE = "streaks.json"
LOCK_FILE = ".lock"
MAX_JOBS = 2000
_MAX_BYTES = 2 * 1024 * 1024
_DAY = 86400
_JOB_ID = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:/-]{0,79}")
_HASH = re.compile(r"[a-f0-9]{64}")
_ROW_KEYS = {"fingerprint", "streak", "notified", "held", "last_seen"}
_NO_FOLLOW = os.O_NOFOLLOW | os.O_NONBLOCK
DEFAULT_NOTICE = (
    "This job has sent the same result {n} runs in a row. "
    "Further identical results will be held until something changes. "
    "To end it, pause the job: hermes cron pause {job_id}"
)

_CLOCK = r"(?:[01]?\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:[.,]\d+)?)?"
_OFFSET = r"(?:z|[+-]\d{2}(?::?\d{2})?)"
_VOLATILE = re.compile(
    rf"(?<!\w)(?:\d{{4}}-\d{{2}}-\d{{2}}(?:[t ]{_CLOCK}{_OFFSET}?)?)(?!\w)"
    rf"|(?<!\w){_CLOCK}(?:[ \t]*[ap]\.?m\.?)?(?!\w)"
    r"|(?<!\w)(?<!\d[.,])[-+−]?\d+(?:,\d{3})*(?:\.\d+)?(?!\w|[.,]\d)"
)


@dataclass(frozen=True)
class Settings:
    threshold: int = 3
    hold_repeats: bool = True
    ignore_numbers: bool = True
    notice: str = DEFAULT_NOTICE
    forget_days: int = 30


def fingerprint(response, ignore_numbers=True):
    text = response.lower()
    if ignore_numbers:
        text = _VOLATILE.sub(" ", text)
    return hashlib.sha256(" ".join(text.split()).encode("utf-8")).hexdigest()


def format_time(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def advance(previous, response, job_id, settings, now):
    """Return the next row and a requested rewrite for a normal (not silent/failed) reply."""
    previous = previous or {}
    digest = fingerprint(response, settings.ignore_numbers)
    same = previous.get("fingerprint") == digest
    streak = previous["streak"] + 1 if same else 1
    notified = same and previous["notified"]
    held = previous.get("held", 0)
    rewrite = None
    if streak == settings.threshold and not notified:
        notice = settings.notice.format(n=streak, job_id=job_id)
        if not notice.strip():
            raise ValueError("empty repeat notice")
        rewrite = response + "\n\n" + notice
        notified = True
    elif streak > settings.threshold and notified and settings.hold_repeats:
        rewrite = SILENT
        held += 1
    return {"fingerprint": digest, "streak": streak, "notified": notified,
            "held": held, "last_seen": now}, rewrite


def valid_job_id(value):
    return isinstance(value, str) and _JOB_ID.fullmatch(value) is not None


# Fallback mirrors the gateway silence rule in the staged run-receipts template.
_SILENT_VARIANTS = {SILENT, "SILENT", "NO_REPLY", "NO REPLY", "[静默]", "静默", "[沉默]", "沉默"}
_BRACKETED = tuple(marker for marker in _SILENT_VARIANTS if marker.startswith("["))


def _standalone_silent(text):
    text = text.strip()
    if not 0 < len(text) <= 64:
        return False
    start, end = 0, len(text)
    while start < end and text[start] not in "[]" and unicodedata.category(text[start]).startswith("P"):
        start += 1
    while start < end and text[end - 1] not in "[]" and unicodedata.category(text[end - 1]).startswith("P"):
        end -= 1
    return any(" ".join(form.upper().split()) in _SILENT_VARIANTS for form in (text, text[start:end]))


def _fallback_is_silent(response):
    text = response.strip()
    if not text:
        return False
    if text.upper().startswith(_BRACKETED):
        return True
    lines = text.splitlines()
    return any(_standalone_silent(part) for part in (text, lines[0], lines[-1]))


def is_silent(response):
    # In real cron the scheduler is already imported. Don't pull in the entire
    # scheduler from a standalone test or plugin host just to match a marker.
    if sys.modules.get("cron.scheduler") is not None:
        try:
            from cron.scheduler import _is_cron_silence_response
        except ImportError:
            pass
        else:
            return _is_cron_silence_response(response)
    try:
        from gateway.response_filters import is_autonomous_silence_response
    except ImportError:
        return _fallback_is_silent(response)
    return is_autonomous_silence_response(response)


def response_kind(response):
    if (not isinstance(response, str) or not response.strip()
            or response.strip() == "(No response generated)"
            or any(line.strip() == FAILURE for line in response.splitlines())):
        return "failure"
    return "silent" if is_silent(response) else "report"


def _object_without_duplicates(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("duplicate state key")
    return result


def _valid_timestamp(value):
    return type(value) in (int, float) and 0 <= value <= 253402300799  # UTC through year 9999


def _valid_row(row):
    if (not isinstance(row, dict) or row.keys() - {"confirmed"} != _ROW_KEYS
            or row.get("confirmed", True) is not True):
        return False
    streak, digest, notified, held = row["streak"], row["fingerprint"], row["notified"], row["held"]
    if (type(streak) is not int or streak < 0 or type(held) is not int or held < 0
            or type(notified) is not bool or not _valid_timestamp(row["last_seen"])):
        return False
    if streak == 0:
        return digest is None and not notified
    return isinstance(digest, str) and _HASH.fullmatch(digest) is not None


class Store:
    """One file per state directory. A stable lock covers the read/modify/replace."""

    def __init__(self, directory, settings, *, clock=time.time):
        self.directory = Path(directory)
        self.settings = settings
        self.clock = clock

    @contextmanager
    def _directory(self, *, create):
        try:
            fd = os.open(self.directory, os.O_RDONLY | _NO_FOLLOW)
        except FileNotFoundError:
            if not create:
                yield None
                return
            try:
                self.directory.mkdir(mode=0o700)
            except FileExistsError:
                pass  # a concurrent writer created it; still open no-follow
            fd = os.open(self.directory, os.O_RDONLY | _NO_FOLLOW)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o700):
                raise PermissionError("state directory must be owned and private (0700)")
            yield fd
        finally:
            os.close(fd)

    @staticmethod
    def _check_file(fd):
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != os.geteuid() or not info.st_mode & stat.S_IRUSR):
            raise OSError("not a readable, owned state file")

    @contextmanager
    def _locked(self, dir_fd):
        # Never unlink this inode: other writers may already have it open.
        fd = os.open(LOCK_FILE, os.O_RDWR | os.O_CREAT | _NO_FOLLOW, 0o600, dir_fd=dir_fd)
        try:
            self._check_file(fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)

    def _read(self, dir_fd):
        try:
            fd = os.open(STATE_FILE, os.O_RDONLY | _NO_FOLLOW, dir_fd=dir_fd)
        except FileNotFoundError:
            return {}
        try:
            self._check_file(fd)
            with os.fdopen(fd, "rb", closefd=False) as handle:
                data = handle.read(_MAX_BYTES + 1)
        finally:
            os.close(fd)
        if len(data) > _MAX_BYTES:
            raise ValueError("state file is too large")
        rows = json.loads(data.decode("utf-8"), object_pairs_hook=_object_without_duplicates)
        if (not isinstance(rows, dict) or len(rows) > MAX_JOBS
                or not all(valid_job_id(key) and _valid_row(row) for key, row in rows.items())):
            raise ValueError("invalid repeat state")
        return rows

    @staticmethod
    def _write(dir_fd, rows, *, cancelled=None):
        data = json.dumps(rows, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(data) > _MAX_BYTES:
            raise ValueError("repeat state too large to write")
        temp = f".streaks-{uuid.uuid4().hex}.tmp"
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NO_FOLLOW, 0o600, dir_fd=dir_fd)
        try:
            try:
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, "wb", closefd=False) as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(fd)
            finally:
                os.close(fd)
            if cancelled is not None and cancelled():
                os.unlink(temp, dir_fd=dir_fd)
                return False
            os.replace(temp, STATE_FILE, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except BaseException:
            try:
                os.unlink(temp, dir_fd=dir_fd)
            except FileNotFoundError:
                pass
            raise
        return True

    def _now(self):
        value = self.clock()
        if not _valid_timestamp(value):
            raise ValueError("invalid clock")
        return value

    def _fresh(self, rows, now):
        cutoff = now - self.settings.forget_days * _DAY
        return {key: row for key, row in rows.items() if row["last_seen"] >= cutoff}

    @staticmethod
    def _put(rows, job_id, row):
        rows[job_id] = row
        if len(rows) > MAX_JOBS:
            oldest = min((key for key in rows if key != job_id), key=lambda key: (rows[key]["last_seen"], key))
            del rows[oldest]

    def observe(self, job_id, response, *, cancelled=None, prepare=False, rebaseline=False):
        """With prepare=True, plan an update for an already-classified report."""
        try:
            if not valid_job_id(job_id):
                return None
            if not prepare:
                kind = response_kind(response)
                if kind == "failure":
                    self.break_streak(job_id, cancelled=cancelled)
                if kind != "report":
                    return None
            with self._directory(create=True) as dir_fd:
                with self._locked(dir_fd):
                    now = self._now()
                    rows = self._fresh(self._read(dir_fd), now)
                    previous = rows.get(job_id)
                    reference = previous
                    if prepare and previous is not None and (rebaseline or not previous.get("confirmed")):
                        # Keep the historical held count, but not a stale or unconfirmed streak.
                        reference = {"held": previous["held"]}
                    row, rewrite = advance(reference, response, job_id, self.settings, now)
                    if rewrite and rewrite != SILENT and is_silent(rewrite):
                        raise ValueError("notice would mark report silent")
                    if prepare:
                        return rewrite, previous, dict(row, confirmed=True)
                    self._put(rows, job_id, row)
                    if cancelled is not None and cancelled():
                        return None
                    if self._write(dir_fd, rows, cancelled=cancelled):
                        return rewrite
        except Exception as exc:
            # Exception text could contain a path, invalid template, or response data.
            log.warning("repeat-pause state update skipped (%s)", type(exc).__name__)
            return None

    def break_streak(self, job_id, *, cancelled=None):
        """Acknowledge only once there is no eligible streak left on disk."""
        with self._directory(create=False) as dir_fd:
            if dir_fd is None:
                return True
            with self._locked(dir_fd):
                now = self._now()
                rows = self._fresh(self._read(dir_fd), now)
                previous = rows.get(job_id)
                if previous is None:
                    return True
                rows[job_id] = dict(previous, fingerprint=None, streak=0, notified=False, last_seen=now)
                rows[job_id].pop("confirmed", None)
                return self._write(dir_fd, rows, cancelled=cancelled)

    def confirm(self, job_id, previous, row):
        """Record a clean turn only if the state it compared against is still current."""
        with self._directory(create=False) as dir_fd:
            if dir_fd is None:
                return
            with self._locked(dir_fd):
                rows = self._fresh(self._read(dir_fd), self._now())
                if rows.get(job_id) == previous:
                    self._put(rows, job_id, row)
                    return self._write(dir_fd, rows)

    def list_repeats(self):
        # Atomic replacement gives readers a complete snapshot without taking a lock.
        with self._directory(create=False) as dir_fd:
            if dir_fd is None:
                return []
            rows = self._fresh(self._read(dir_fd), self._now())
        return sorted((job, row) for job, row in rows.items() if row["streak"] >= 2)

    def reset(self, job_id):
        if not valid_job_id(job_id):
            return False
        with self._directory(create=False) as dir_fd:
            if dir_fd is None:
                return False
            with self._locked(dir_fd):
                rows = self._fresh(self._read(dir_fd), self._now())
                if job_id not in rows:
                    return False
                del rows[job_id]
                self._write(dir_fd, rows)
                return True
