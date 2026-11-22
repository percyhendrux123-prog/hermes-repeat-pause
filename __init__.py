"""Hermes entry point for repeat-pause; only cron results can be rewritten."""
import json
import logging
import os
import re
import threading
import time
from contextvars import copy_context
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

from . import core

log = logging.getLogger(__name__)
_clock = time.time
_CRON_SESSION = re.compile(r"cron_(.+)_[0-9]{8}_[0-9]{6}")
_STATE_WAIT = 0.040
_FILE_MUTATIONS = {"write_file", "patch"}
_TOOL_RESULT_LIMIT = 64 * 1024


@dataclass
class _Run:
    saw_transform: bool = False
    tool_error: bool = False
    prepared: tuple | None = None


def _tool_error(tool_name, status, result):
    if status in ("error", "blocked"):
        return True
    mutation = isinstance(tool_name, str) and tool_name in _FILE_MUTATIONS
    if status == "ok" and not mutation:
        return False
    # As in run-receipts, inspect the structured result when status is absent.
    # For file mutations also check the result itself: a missed failure can hide
    # the verifier footer Hermes appends after our transform has returned.
    if isinstance(result, str):
        if len(result) > _TOOL_RESULT_LIMIT:
            return True
        try:
            result = json.loads(result)
        except (ValueError, RecursionError):
            return True
    if not isinstance(result, dict):
        return True
    code = result.get("exit_code")
    return (bool(result.get("error")) or result.get("success") is False
            or (type(code) is int and code != 0))


def _hermes_home():
    try:
        from hermes_constants import get_hermes_home
    except ImportError:
        return Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes")
    return Path(get_hermes_home())


def _integer(value):
    if type(value) is int or isinstance(value, str):
        return int(value)
    raise ValueError("expected integer setting")


def _boolean(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        word = value.strip().lower()
        if word in {"1", "true", "yes", "on"}:
            return True
        if word in {"0", "false", "no", "off"}:
            return False
    raise ValueError("expected boolean setting")


def _store(ctx, profile_home=None):
    def get(key, default=None):
        override = os.environ.get("REPEAT_PAUSE_" + key.upper())
        return override if override is not None else ctx.get_config(key, default)

    settings = core.Settings(
        threshold=max(2, _integer(get("threshold", 3))),
        hold_repeats=_boolean(get("hold_repeats", True)),
        ignore_numbers=_boolean(get("ignore_numbers", True)),
        notice=get("notice", core.DEFAULT_NOTICE),
        forget_days=_integer(get("forget_days", 30)),
    )
    if settings.forget_days < 0 or not isinstance(settings.notice, str):
        raise ValueError("invalid repeat-pause settings")
    raw_dir = get("state_dir", "")
    if not isinstance(raw_dir, str):
        raise ValueError("expected state_dir string")
    directory = Path(raw_dir).expanduser() if raw_dir.strip() else Path("repeat-pause")
    if not directory.is_absolute():
        directory = (profile_home if profile_home is not None else _hermes_home()) / directory
    return core.Store(directory, settings, clock=lambda: _clock())


def _fail_open(callback, fallback=None):
    @wraps(callback)
    def wrapped(*args, **kwargs):
        try:
            return callback(*args, **kwargs)
        except Exception as exc:
            log.warning("repeat-pause %s skipped (%s)", callback.__name__, type(exc).__name__)
            return fallback
    return wrapped


def register(ctx):
    active = {}  # (profile/job, run identity) -> flags and fingerprint-only state
    fences = {}  # profile/job -> identity of the latest uncommitted state change
    lock = threading.Lock()
    storage_slot = threading.Lock()

    @_fail_open
    def update_state(operation, cancelled):
        if cancelled is None or not cancelled():
            return operation(cancelled)

    def state_call(operation, *, finish=False):
        # No backlog: a stalled filesystem costs at most one worker. Capture the
        # run context so profile-scoped paths are still resolved off the hook thread.
        done, cancelled = threading.Event(), threading.Event()
        context = copy_context()
        result = None

        def run():
            nonlocal result
            try:
                # End-hook bookkeeping can't produce a rewrite. Let it finish off-thread
                # even if the observer has already returned.
                result = context.run(update_state, operation, None if finish else cancelled.is_set)
            finally:
                storage_slot.release()
                done.set()

        if not storage_slot.acquire(blocking=False):
            return None
        try:
            threading.Thread(target=run, name="repeat-pause-state", daemon=True).start()
        except Exception:
            storage_slot.release()
            raise
        if done.wait(_STATE_WAIT):
            return result
        cancelled.set()
        return None

    def fence(scope):  # caller holds lock
        token = object()
        fences[scope] = token
        return token

    def persist(scope, token, operation):
        def commit(_):
            if operation():
                # An older worker must not clear a newer missed update.
                with lock:
                    if fences.get(scope) is token:
                        del fences[scope]
        state_call(commit, finish=True)

    def break_streak(scope):
        home, job = scope
        with lock:
            token = fence(scope)
        persist(scope, token, lambda: _store(ctx, home).break_streak(job))

    def identify(session_id, platform):
        if platform != "cron":
            return None
        getter = getattr(ctx, "current_cron_execution", None)
        execution = getter() if callable(getter) else None
        job = getattr(execution, "job_id", None)
        if not core.valid_job_id(job):
            match = _CRON_SESSION.fullmatch(session_id or "")
            job = match.group(1) if match else None
        if not core.valid_job_id(job):
            return None
        scope = (_hermes_home(), job)
        run = getattr(execution, "execution_id", None)
        return (scope, "execution", run) if run else (scope, "session", session_id)

    def on_session_start(session_id=None, platform=None, **_):
        key = identify(session_id, platform)
        if key is not None:
            with lock:
                if key not in active and len(active) >= core.MAX_JOBS:
                    active.pop(next(iter(active)))
                active.setdefault(key, _Run())

    def post_tool_call(tool_name=None, session_id=None, status=None, result=None, platform=None, **_):
        # This hook carries a session ID but no platform. Only an already
        # tracked cron run (stable execution ID when available) can be tainted.
        key = identify(session_id, "cron" if platform is None else platform)
        with lock:
            run = active.get(key)
        if run is not None and _tool_error(tool_name, status, result):
            with lock:
                if active.get(key) is run:
                    run.tool_error = True
                    fence(key[0])

    def transform_llm_output(response_text=None, session_id=None, platform=None, **_):
        key = identify(session_id, platform)
        if key is None:
            return None
        scope = key[0]
        with lock:
            run = active.get(key)
            if run is not None:
                if run.tool_error:
                    return None
                run.saw_transform = True
            token = fences.get(scope)

        # Classify before taking the storage slot: an already-silent run must
        # leave the streak alone even if a different run has stalled storage.
        try:
            kind = core.response_kind(response_text)
        except Exception:
            with lock:
                fence(scope)
            raise
        if kind == "silent":
            return None
        if run is None:
            with lock:
                fence(scope)
            return None
        if kind == "failure":
            break_streak(scope)
            return None

        def prepare(cancelled):
            home, job = scope
            store = _store(ctx, home)
            plan = store.observe(job, response_text, cancelled=cancelled, prepare=True,
                                 rebaseline=token is not None)
            if plan is not None:
                rewrite, previous, row = plan
                return rewrite, (store, previous, row, token)

        result = None
        try:
            result = state_call(prepare)
        finally:
            with lock:
                if (result is not None and active.get(key) is run and not run.tool_error
                        and fences.get(scope) is token):
                    run.prepared = result[1]
                else:
                    fence(scope)
                    result = None
        if result is not None:
            return result[0]

    def on_session_end(session_id=None, platform=None, completed=None, failed=False,
                       interrupted=False, turn_exit_reason=None, **_):
        key = identify(session_id, platform)
        if key is None:
            return
        scope = key[0]
        with lock:
            run = active.pop(key, None)
        saw_transform = run.saw_transform if run is not None else None
        reason = str(turn_exit_reason or "")
        max_iteration_handoff = saw_transform and reason.startswith("max_iterations_reached(")
        # The transform can be absent for empty or interrupted turns. The end
        # hook sees flags but runs too late to change the text the host returned.
        if (failed or interrupted or (run is not None and run.tool_error)
                or reason == "empty_response_exhausted" or saw_transform is False
                or (completed is False and not max_iteration_handoff)):
            break_streak(scope)
        elif run is not None and run.prepared:
            store, previous, row, prepared_token = run.prepared
            with lock:
                current_token = fences.get(scope)
                token = fence(scope)
            if current_token is prepared_token and (completed is True or max_iteration_handoff):
                persist(scope, token, lambda: store.confirm(scope[1], previous, row))

    def command(raw_args=""):
        parts = raw_args.split()
        if not parts:
            rows = _store(ctx).list_repeats()
            if not rows:
                return "No repeat streaks of 2 or more."
            lines = ["Job ID  Streak  Held  Last seen (UTC)"]
            lines.extend(f"{job}  {row['streak']}  {row['held']}  {core.format_time(row['last_seen'])}"
                         for job, row in rows)
            return "\n".join(lines)
        if len(parts) == 2 and parts[0] == "reset" and core.valid_job_id(parts[1]):
            job = parts[1]
            if _store(ctx).reset(job):
                return f"Repeat state cleared for {job}. Its schedule was not changed."
            return f"No repeat state for {job}."
        return "Usage: /repeats or /repeats reset <job_id>"

    for callback in (on_session_start, post_tool_call, transform_llm_output, on_session_end):
        ctx.register_hook(callback.__name__, _fail_open(callback))
    ctx.register_command("repeats", _fail_open(command, "Repeat state unavailable (see agent log)."),
                         description="List repeated cron results or clear a job’s streak",
                         args_hint="[reset <job_id>]")
