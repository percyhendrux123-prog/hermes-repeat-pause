import ast
import importlib.util
import json
import os
import re
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock

ROOT = Path(os.path.realpath(Path(__file__).parent.parent))
NAME = "repeat_pause_under_test"
PLUGIN_SPEC = importlib.util.spec_from_file_location(NAME, ROOT / "__init__.py",
                                                      submodule_search_locations=[str(ROOT)])


class FakeContext:
    def __init__(self, config=None):
        self.config = dict(config or {})
        self.execution = None
        self.hooks = {}
        self.commands = {}
        self.tools = {}

    def get_config(self, key, default=None):
        return self.config.get(key, default)

    def current_cron_execution(self):
        return self.execution

    def register_hook(self, name, handler, **kwargs):
        if name in self.hooks or kwargs.get("hold_stream"):
            raise AssertionError("duplicate hook or global stream hold")
        self.hooks[name] = handler

    def register_command(self, name, handler, **_):
        self.commands[name] = handler

    def register_tool(self, name, **kwargs):
        self.tools[name] = kwargs


class PluginTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((ROOT / "__init__.py").is_file(), "plugin entry point is not implemented")
        self.plugin = importlib.util.module_from_spec(PLUGIN_SPEC)
        modules = mock.patch.dict(sys.modules, {NAME: self.plugin, "hermes_constants": None,
                                                "cron.scheduler": None, "gateway.response_filters": None})
        modules.start()
        self.addCleanup(modules.stop)
        assert PLUGIN_SPEC.loader is not None
        PLUGIN_SPEC.loader.exec_module(self.plugin)
        tmp = tempfile.TemporaryDirectory(dir=os.path.realpath(tempfile.gettempdir()))
        self.addCleanup(tmp.cleanup)
        self.root = Path(os.path.realpath(tmp.name))
        env = mock.patch.dict(os.environ, {"HERMES_HOME": str(self.root)}, clear=True)
        env.start()
        self.addCleanup(env.stop)
        self.path = self.root / "repeat-pause" / "streaks.json"
        self.ctx = FakeContext()
        self.plugin._clock = lambda: 1_790_000_000
        self.plugin.register(self.ctx)
        self.sid_counter = 0

    def run_turn(self, response, *, platform="cron", sid=None, end_sid=None, reason=None,
                 failed=False, interrupted=False, completed=True, transform=True, end=True):
        self.sid_counter += 1
        sid = sid or f"cron_daily_watch_20261121_{self.sid_counter:06}"
        payload = dict(session_id=sid, model="test", platform=platform)
        hooks = self.ctx.hooks
        hooks["on_session_start"](**payload)
        rewrite = None
        if transform:
            rewrite = hooks["transform_llm_output"](**payload, response_text=response, turn_id=str(self.sid_counter))
        if end:
            payload["session_id"] = end_sid or sid
            hooks["on_session_end"](**payload, completed=completed, failed=failed, interrupted=interrupted,
                                    turn_exit_reason=reason, turn_id=str(self.sid_counter))
        return rewrite

    def rows(self):
        return json.loads(self.path.read_bytes())

    def test_loaded_hooks_issue_notice_then_suppress_only_later_repeats(self):
        self.assertIsNone(self.run_turn("A"))
        self.assertIsNone(self.run_turn("A"))
        self.assertIn("hermes cron pause daily_watch", self.run_turn("A"))
        self.assertEqual(self.run_turn("A"), "[SILENT]")
        self.assertIsNone(self.run_turn("B"))
        self.assertIsNone(self.run_turn("A"))
        self.assertEqual(self.rows()["daily_watch"]["streak"], 1)

    def test_tool_errors_never_rewrite_a_report_or_hide_a_late_verifier_footer(self):
        report = "Daily report: all checks passed."
        cases = (
            ("write_file", "ok", '{"success": false}', "File mutation verifier: write_file failed."),
            ("patch", None, '{"error": "edit did not apply"}', "File mutation verifier: patch failed."),
            ("terminal", "error", '{}', ""),
        )
        for tool, status, result, footer in cases:
            for prior_runs in (2, 3):  # exercise the notice and hold boundaries
                with self.subTest(tool=tool, prior_runs=prior_runs):
                    self.ctx.commands["repeats"]("reset daily_watch")
                    for _ in range(prior_runs):
                        self.run_turn(report)
                    sid = "cron_daily_watch_20261121_999999"
                    payload = dict(session_id=sid, platform="cron")
                    hooks = self.ctx.hooks
                    hooks["on_session_start"](**payload)
                    observer = hooks.get("post_tool_call")
                    if observer is not None:
                        observer(session_id=sid, tool_name=tool, status=status, result=result)
                        observer(session_id=sid, tool_name="patch", status="ok", result='{"success": true}')
                    rewrite = hooks["transform_llm_output"](**payload, response_text=report)
                    final = (report if rewrite is None else rewrite) + ("\n\n" + footer if footer else "")
                    self.assertFalse(self.plugin.core.is_silent(final), final)
                    self.assertIsNone(rewrite)
                    hooks["on_session_end"](**payload, completed=True)
                    self.assertEqual(self.rows()["daily_watch"]["streak"], 0)
                    self.assertIsNone(self.run_turn(report))
                    self.assertEqual(self.rows()["daily_watch"]["streak"], 1)

    def test_interactive_gateway_and_subagents_never_touch_state_even_with_cron_like_id(self):
        self.ctx.execution = SimpleNamespace(job_id="real", execution_id="id")
        for platform in ("cli", "telegram", "gateway", "subagent", ""):
            self.assertIsNone(self.run_turn("A", platform=platform))
        self.assertFalse(self.path.parent.exists())

    def test_execution_identity_wins_over_legacy_session_id(self):
        self.ctx.execution = SimpleNamespace(job_id="owned-job", execution_id="id-1")
        self.assertIsNone(self.run_turn("A", sid="cron_other_20261121_000001"))
        self.ctx.execution = SimpleNamespace(job_id="owned-job", execution_id="id-2")
        self.assertIsNone(self.run_turn("A", sid="opaque-host-session"))
        self.assertEqual(set(self.rows()), {"owned-job"})
        self.assertEqual(self.rows()["owned-job"]["streak"], 2)

    def test_unconfirmed_replies_cannot_build_a_streak_that_hides_a_failure(self):
        diagnostic = ("No reply: the turn stopped while a tool result was still pending. "
                      "Send `continue` to let the model summarize.")
        self.run_turn(diagnostic)
        self.run_turn(diagnostic, end=False)
        self.run_turn(diagnostic, completed=None)
        rewrite = self.run_turn(diagnostic, failed=True, completed=False, reason="pending_tool_result")
        self.assertEqual(diagnostic if rewrite is None else rewrite, diagnostic)
        self.assertEqual(self.rows()["daily_watch"]["streak"], 0)

        for _ in range(2):
            self.assertIsNone(self.run_turn(diagnostic))
        self.assertIn("3 runs in a row", self.run_turn(diagnostic))
        # A new registration can still hold based on persisted, confirmed success.
        self.ctx = FakeContext()
        self.plugin.register(self.ctx)
        self.assertEqual(self.run_turn(diagnostic.upper()), "[SILENT]")

    def test_missing_job_identity_does_nothing(self):
        self.assertIsNone(self.run_turn("A", sid="unparseable"))
        self.assertFalse(self.path.parent.exists())

    def test_empty_turn_without_transform_resets_the_streak(self):
        for _ in range(2):
            self.run_turn("A")
        self.assertIsNone(self.run_turn("", transform=False))
        self.assertEqual(self.rows()["daily_watch"]["streak"], 0)
        self.assertIsNone(self.run_turn("A"))
        self.assertEqual(self.rows()["daily_watch"]["streak"], 1)

    def test_slow_storage_does_not_hold_either_cron_callback(self):
        for _ in range(3):
            self.run_turn("A")
        before = self.path.read_bytes()
        original_fsync = os.fsync
        store = self.plugin.core.Store
        original_read = store._read
        sid = "cron_daily_watch_20261121_000100"
        self.ctx.hooks["on_session_start"](session_id=sid, platform="cron")
        for hook, kwargs in (("transform_llm_output", {"response_text": "A"}),
                             ("on_session_end", {"failed": True})):
            with self.subTest(hook=hook):
                entered, release, returned = (threading.Event() for _ in range(3))
                io_threads = []

                def stall():
                    io_threads.append(threading.current_thread())
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("test did not release storage")

                def stall_fsync(fd):
                    stall()
                    original_fsync(fd)

                def stall_read(instance, fd):
                    stall()
                    return original_read(instance, fd)

                result = []

                def callback():
                    result.append(self.ctx.hooks[hook](session_id=sid, platform="cron", **kwargs))
                    returned.set()

                target = ((store, "_read", stall_read) if hook == "transform_llm_output"
                          else (self.plugin.core.os, "fsync", stall_fsync))
                with mock.patch.object(*target):
                    caller = threading.Thread(target=callback, daemon=True)
                    caller.start()
                    try:
                        self.assertTrue(entered.wait(1), "the uncontended operation must reach storage")
                        self.assertTrue(returned.wait(0.2), f"{hook} waited for slow storage")
                        self.assertEqual(result, [None])
                    finally:
                        release.set()
                        caller.join(2)
                        for worker in io_threads:
                            worker.join(2)
                            self.assertFalse(worker.is_alive())
                if hook == "transform_llm_output":
                    self.assertEqual(self.path.read_bytes(), before)
                else:
                    self.assertEqual(self.rows()["daily_watch"]["streak"], 0)
        self.assertIsNone(self.run_turn("A"))
        self.assertEqual(self.rows()["daily_watch"]["streak"], 1)

    def test_missed_streak_break_cannot_hold_the_next_report(self):
        hooks = self.ctx.hooks
        store = self.plugin.core.Store
        read = store._read
        cases = (
            ("failed turn", {"completed": False, "failed": True}),
            ("changed successful turn", {"completed": True}),
            ("observed write failure", {"completed": True}),
        )
        for case, end_status in cases:
            with self.subTest(case=case):
                self.ctx.commands["repeats"]("reset daily_watch")
                for _ in range(2):
                    self.assertIsNone(self.run_turn("A"))
                self.assertIn("3 runs in a row", self.run_turn("A"))

                entered, release, transformed, ended = (threading.Event() for _ in range(4))
                io_threads, rewrites = [], []
                write_failed = case == "observed write failure"
                busy_job = "other" if write_failed else "daily_watch"
                payload = dict(session_id=f"cron_{busy_job}_20261121_999998", platform="cron")
                hooks["on_session_start"](**payload)

                def stall_read(instance, fd):
                    io_threads.append(threading.current_thread())
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("test did not release storage")
                    return read(instance, fd)

                def transform():
                    rewrites.append(hooks["transform_llm_output"](**payload, response_text="B"))
                    transformed.set()

                def end():
                    ending = payload
                    if write_failed:
                        ending = dict(session_id="cron_daily_watch_20261121_999999", platform="cron")
                        hooks["on_session_start"](**ending)
                        hooks["post_tool_call"](session_id=ending["session_id"], tool_name="write_file",
                                                status="ok", result='{"success": false}')
                        rewrites.append(hooks["transform_llm_output"](**ending, response_text="A"))
                    hooks["on_session_end"](**ending, **end_status)
                    ended.set()

                callers = [threading.Thread(target=callback, daemon=True) for callback in (transform, end)]
                with mock.patch.object(store, "_read", stall_read):
                    try:
                        callers[0].start()
                        self.assertTrue(entered.wait(1), "the uncontended read must reach its barrier")
                        self.assertTrue(transformed.wait(0.2), "transform waited for the read")
                        self.assertEqual(rewrites, [None])
                        callers[1].start()
                        self.assertTrue(ended.wait(0.2), "end callback waited for the read")
                        if write_failed:
                            self.assertEqual(rewrites, [None, None], "the verifier footer must still deliver")
                    finally:
                        release.set()
                        for thread in callers + io_threads:
                            if thread.ident is not None:
                                thread.join(2)
                                self.assertFalse(thread.is_alive())

                if write_failed:
                    hooks["on_session_end"](**payload, completed=True)
                self.assertIsNone(self.run_turn("A"), "stale state must not suppress the first clean report")
                row = self.rows()["daily_watch"]
                self.assertEqual((row["streak"], row["notified"]), (1, False))
                self.assertIsNone(self.run_turn("A"))
                self.assertIn("3 runs in a row", self.run_turn("A"))
                self.assertEqual(self.run_turn("A"), "[SILENT]")

    def test_each_explicit_end_status_resets_a_nonempty_transformed_turn(self):
        for status in ({"failed": True}, {"interrupted": True}, {"completed": False},
                       {"reason": "empty_response_exhausted"}):
            with self.subTest(status=status):
                self.ctx.commands["repeats"]("reset daily_watch")
                self.assertIsNone(self.run_turn("Report"))
                self.assertIsNone(self.run_turn("Report"))
                # The early transform sees the third match, not the status. It
                # may append a notice, but cannot replace the report with silence.
                rewritten = self.run_turn("Report", **status)
                self.assertIsInstance(rewritten, str)
                self.assertTrue(rewritten.startswith("Report\n\n"), rewritten)
                row = self.rows()["daily_watch"]
                self.assertEqual(row["streak"], 0)
                self.assertIsNone(row["fingerprint"])
                self.assertFalse(row["notified"])
                self.assertIsNone(self.run_turn("Report"))
                self.assertEqual(self.rows()["daily_watch"]["streak"], 1)

    def test_silent_transform_keeps_streak_when_execution_session_id_changes(self):
        for run_id in ("first", "second"):
            self.ctx.execution = SimpleNamespace(job_id="daily_watch", execution_id=run_id)
            self.run_turn("A", sid="opaque-run-start")
        self.ctx.execution = SimpleNamespace(job_id="daily_watch", execution_id="third")
        before = self.path.read_bytes()
        self.assertIsNone(self.run_turn("NO_REPLY", sid="opaque-run-start", end_sid="compressed-run"))
        self.assertEqual(self.path.read_bytes(), before)

    def test_deliverable_max_iteration_summary_is_not_reset_by_end_hook(self):
        for _ in range(2):
            self.run_turn("A", completed=False, reason="max_iterations_reached(12)")
        self.assertEqual(self.rows()["daily_watch"]["streak"], 2)

    def test_command_lists_streak_and_held_count_then_resets_only_selected_job(self):
        self.assertIn("No", self.ctx.commands["repeats"](""))
        self.assertFalse(self.path.parent.exists())
        for _ in range(4):
            self.run_turn("A")
        for i in range(1, 3):
            self.run_turn("B", sid=f"cron_other_20261121_{i:06}")
        command = self.ctx.commands["repeats"]
        out = command("")
        self.assertRegex(out, r"daily_watch\s+4\s+1\s+2026-09-21T14:13:20Z")
        self.assertRegex(out, r"other\s+2\s+0\s+2026-09-21T14:13:20Z")
        self.assertIn("Usage:", command("reset daily_watch extra"))
        self.assertIn("daily_watch", command("reset daily_watch"))
        self.assertEqual(set(self.rows()), {"other"})
        self.assertIsNone(self.run_turn("A"))
        self.assertEqual(self.rows()["daily_watch"]["streak"], 1)

    def test_env_settings_override_plugin_config(self):
        self.ctx.config.update(threshold=7, hold_repeats=True, notice="configured", ignore_numbers=False,
                               state_dir=str(self.root / "configured"), forget_days=30)
        override = self.root / "overridden"
        os.environ.update(REPEAT_PAUSE_THRESHOLD="1", REPEAT_PAUSE_HOLD_REPEATS="off",
                          REPEAT_PAUSE_NOTICE="{job_id} repeated {n}", REPEAT_PAUSE_IGNORE_NUMBERS="true",
                          REPEAT_PAUSE_STATE_DIR=str(override), REPEAT_PAUSE_FORGET_DAYS="1")
        self.assertIsNone(self.run_turn("A 12"))
        self.assertEqual(self.run_turn("A 32"), "A 32\n\ndaily_watch repeated 2")
        self.assertIsNone(self.run_turn("A 43"))
        self.assertTrue((override / "streaks.json").is_file())
        self.assertFalse(self.path.exists())
        self.assertFalse((self.root / "configured").exists())
        self.plugin._clock = lambda: 1_790_086_401
        self.assertIsNone(self.run_turn("A 12"))
        self.assertEqual(json.loads((override / "streaks.json").read_bytes())["daily_watch"]["streak"], 1)

    def test_canonical_hermes_home_resolves_active_profile_not_exported_env(self):
        homes = [self.root / x for x in ("profile-one", "profile-two")]
        for home in homes:
            home.mkdir()
        host = ModuleType("hermes_constants")
        host.get_hermes_home = lambda: homes[0]
        with mock.patch.dict(sys.modules, {"hermes_constants": host}):
            self.run_turn("A")
            self.run_turn("A")
            host.get_hermes_home = lambda: homes[1]
            self.run_turn("A")
        counts = [json.loads((home / "repeat-pause" / "streaks.json").read_bytes())["daily_watch"]["streak"]
                  for home in homes]
        self.assertEqual(counts, [2, 1])
        self.assertFalse(self.path.exists())

    def test_configuration_exception_fails_open_without_exposing_input(self):
        self.ctx.get_config = mock.Mock(side_effect=RuntimeError("private exception text"))
        with self.assertLogs(self.plugin.log.name, level="WARNING") as logs:
            self.assertIsNone(self.run_turn("private response"))
        self.assertFalse(self.path.exists())
        self.assertNotIn("private", " ".join(logs.output))

    def test_manifest_declarations_match_registrations(self):
        manifest = (ROOT / "plugin.yaml").read_text()
        hooks = re.search(r"(?m)^provides_hooks:\n((?:  - [a-z_]+\n)+)", manifest)
        self.assertIsNotNone(hooks)
        listed = re.findall(r"(?m)^  - (\w+)$", hooks.group(1))
        self.assertCountEqual(listed, self.ctx.hooks)
        self.assertEqual(set(self.ctx.hooks),
                         {"transform_llm_output", "on_session_start", "post_tool_call", "on_session_end"})
        self.assertEqual(set(self.ctx.commands), {"repeats"})
        self.assertEqual(self.ctx.tools, {})
        self.assertNotIn("provides_tools:", manifest)

    def test_readme_contains_required_sections_and_scope(self):
        text = (ROOT / "README.md").read_text(encoding="utf-8")
        for section in ("What it does", "What it stores", "Settings", "Install and test",
                        "Not covered", "License"):
            self.assertIn(f"\n## {section}\n", text)
        for section in ("Disclosures", "How it was made"):
            self.assertIn(f"\n## {section}\n", text)
        for disclosure in ("AI agents wrote", "no_agent", "monitor-gate", "[CRON_FAILURE]"):
            self.assertIn(disclosure, text)

    def test_no_network_or_subprocess_imports(self):
        banned = {"socket", "urllib", "requests", "http", "httpx", "subprocess", "asyncio", "websocket"}
        for name in ("__init__.py", "core.py"):
            for node in ast.walk(ast.parse((ROOT / name).read_text())):
                modules = ([ref.name for ref in node.names] if isinstance(node, ast.Import) else
                           [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
                self.assertTrue(banned.isdisjoint(module.split(".")[0] for module in modules), name)
