import fcntl
import json
import os
import stat
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

import core


class StateTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(dir=os.path.realpath(tempfile.gettempdir()))
        self.addCleanup(tmp.cleanup)
        self.root = Path(os.path.realpath(tmp.name))
        self.directory = self.root / "repeat-pause"
        self.path = self.directory / "streaks.json"
        self.now = 1_790_000_000
        self.store = core.Store(self.directory, core.Settings(), clock=lambda: self.now)

    def seed(self, entries):
        self.directory.mkdir(mode=0o700, exist_ok=True)
        self.path.write_text(json.dumps(entries), encoding="utf-8")
        self.path.chmod(0o600)

    def test_persists_across_instances_independently_without_response_text(self):
        alpha = "first synthetic response: never persist me"
        beta = "second synthetic response: also private"
        self.assertIsNone(self.store.observe("one", alpha))
        self.assertIsNone(self.store.observe("two", beta))
        another = core.Store(self.directory, core.Settings(), clock=lambda: self.now + 1)
        self.assertIsNone(another.observe("one", alpha))
        self.assertTrue(another.observe("one", alpha).startswith(alpha + "\n\n"))
        self.assertIsNone(another.observe("two", beta))
        raw = self.path.read_text(encoding="utf-8")
        self.assertNotIn(alpha, raw)
        self.assertNotIn(beta, raw)
        rows = json.loads(raw)
        self.assertEqual(rows["one"]["fingerprint"], core.fingerprint(alpha))
        self.assertEqual(rows["two"]["streak"], 2)
        self.assertEqual(set(rows["one"]), {"fingerprint", "streak", "notified", "held", "last_seen"})
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)

    def test_empty_failure_reset_and_silent_does_not_touch_state(self):
        for response in ("[CRON_FAILURE]\nprivate detail", "prefix\n[CRON_FAILURE]  \nprivate detail", "", " \t\n"):
            with self.subTest(response=response):
                for _ in range(3):
                    self.store.observe("watch", "steady")
                before = self.path.read_bytes()
                for silent in ("[SILENT]", "SILENT!", "[SILENT] no change", "report\nno reply", "静默"):
                    self.assertIsNone(self.store.observe("watch", silent))
                    self.assertEqual(self.path.read_bytes(), before)
                self.assertIsNone(self.store.observe("watch", response))
                self.assertEqual(json.loads(self.path.read_bytes())["watch"]["streak"], 0)
                self.assertIsNone(self.store.observe("watch", "steady"))
                self.assertEqual(json.loads(self.path.read_bytes())["watch"]["streak"], 1)

    def test_inline_tokens_are_not_control_lines(self):
        text = "Report quotes [SILENT] and [CRON_FAILURE] in a sentence."
        for _ in range(3):
            rewrite = self.store.observe("watch", text)
        self.assertTrue(rewrite.startswith(text + "\n\n"))
        self.assertEqual(self.store.observe("watch", text), "[SILENT]")

    def test_bad_state_fails_open_without_replacing_it(self):
        self.directory.mkdir(mode=0o700)
        for bad in (b"{bad", b'[]', b'{"watch": {"streak": 3}}', b'{"watch": {}, "watch": {}}'):
            with self.subTest(bad=bad):
                self.path.write_bytes(bad)
                inode = self.path.stat().st_ino
                with self.assertLogs(core.log.name, level="WARNING"):
                    self.assertIsNone(self.store.observe("watch", "A"))
                self.assertEqual(self.path.read_bytes(), bad)
                self.assertEqual(self.path.stat().st_ino, inode)

    def test_unreadable_state_stays_unreadable_and_untouched(self):
        self.seed({})
        inode = self.path.stat().st_ino
        self.path.chmod(0)
        with self.assertLogs(core.log.name, level="WARNING"):
            self.assertIsNone(self.store.observe("watch", "A"))
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0)
        self.assertEqual(self.path.stat().st_ino, inode)
        self.path.chmod(0o600)
        self.assertEqual(self.path.read_bytes(), b"{}")

    def test_symlinked_state_file_is_refused_without_touching_target(self):
        self.directory.mkdir(mode=0o700)
        target = self.root / "outside.json"
        target.write_text("{}")
        self.path.symlink_to(target)
        with self.assertLogs(core.log.name, level="WARNING"):
            self.assertIsNone(self.store.observe("watch", "A"))
        self.assertTrue(self.path.is_symlink())
        self.assertEqual(os.path.realpath(self.path), os.path.realpath(target))
        self.assertEqual(target.read_bytes(), b"{}")

    def test_symlinked_state_directory_is_refused(self):
        target = self.root / "outside"
        target.mkdir()
        self.directory.symlink_to(target, target_is_directory=True)
        with self.assertLogs(core.log.name, level="WARNING"):
            self.assertIsNone(self.store.observe("watch", "A"))
        self.assertEqual(os.path.realpath(self.directory), os.path.realpath(target))
        self.assertEqual(list(target.iterdir()), [])

    def test_existing_shared_directory_is_not_chmodded_or_written(self):
        self.directory.mkdir()
        self.directory.chmod(0o755)
        with self.assertLogs(core.log.name, level="WARNING"):
            self.assertIsNone(self.store.observe("watch", "A"))
        self.assertEqual(stat.S_IMODE(self.directory.stat().st_mode), 0o755)
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_directory_swap_does_not_redirect_atomic_write(self):
        self.store.observe("watch", "A")
        old, elsewhere = self.root / "moved", self.root / "elsewhere"
        elsewhere.mkdir()
        original_replace = os.replace

        def swap_then_replace(*args, **kwargs):
            self.directory.rename(old)
            self.directory.symlink_to(elsewhere, target_is_directory=True)
            return original_replace(*args, **kwargs)

        with mock.patch.object(core.os, "replace", side_effect=swap_then_replace):
            self.store.observe("watch", "A")
        self.assertEqual(list(elsewhere.iterdir()), [])
        self.assertEqual(json.loads((old / self.path.name).read_bytes())["watch"]["streak"], 2)

    def test_upstream_matcher_wins_and_its_error_preserves_state(self):
        # The stub supplies the host boundary only; repeat decisions and disk I/O are real.
        for _ in range(2):
            self.store.observe("watch", "A")
        before = self.path.read_bytes()
        scheduler = ModuleType("cron.scheduler")
        scheduler._is_cron_silence_response = lambda text: text == "A"
        with mock.patch.dict(sys.modules, {"cron.scheduler": scheduler}):
            self.assertIsNone(self.store.observe("watch", "A"))
            self.assertEqual(self.path.read_bytes(), before)
            scheduler._is_cron_silence_response = mock.Mock(side_effect=RuntimeError("private"))
            with self.assertLogs(core.log.name, level="WARNING") as logs:
                self.assertIsNone(self.store.observe("watch", "A"))
            self.assertNotIn("private", " ".join(logs.output))
            self.assertEqual(self.path.read_bytes(), before)

    def test_custom_notice_must_not_turn_threshold_report_silent(self):
        store = core.Store(self.directory, core.Settings(threshold=2, notice="[SILENT]"),
                           clock=lambda: self.now)
        self.assertIsNone(store.observe("watch", "A"))
        before = self.path.read_bytes()
        with self.assertLogs(core.log.name, level="WARNING"):
            self.assertIsNone(store.observe("watch", "A"))
        self.assertEqual(self.path.read_bytes(), before)

    def test_real_contended_lock_fails_open_under_50_ms(self):
        for _ in range(3):
            self.store.observe("watch", "A")
        before = self.path.read_bytes()
        with (self.directory / core.LOCK_FILE).open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertLogs(core.log.name, level="WARNING"):
                start = time.monotonic()
                self.assertIsNone(self.store.observe("watch", "A"))
                elapsed = time.monotonic() - start
            self.assertLess(elapsed, 0.050)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.store.observe("watch", "A"), "[SILENT]")

    def test_failed_atomic_replace_keeps_state_and_leaves_no_temp(self):
        for _ in range(3):
            self.store.observe("watch", "A")
        before = self.path.read_bytes()
        with mock.patch.object(core.os, "replace", side_effect=OSError("write failed")):
            with self.assertLogs(core.log.name, level="WARNING"):
                self.assertIsNone(self.store.observe("watch", "A"))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual({p.name for p in self.directory.iterdir()}, {core.LOCK_FILE, self.path.name})
        self.assertEqual(self.store.observe("watch", "A"), "[SILENT]")

    def test_row_retention_expires_a_streak_before_comparing(self):
        self.store.observe("old", "A")
        self.store.observe("old", "A")
        self.now += 30 * 86400 + 1
        self.store.observe("other", "B")
        self.assertNotIn("old", json.loads(self.path.read_bytes()))
        self.assertIsNone(self.store.observe("old", "A"))
        self.assertEqual(json.loads(self.path.read_bytes())["old"]["streak"], 1)

    def test_job_cap_keeps_current_job_and_evicts_oldest(self):
        self.seed({f"job-{i:04}": {"fingerprint": core.fingerprint("A"), "streak": 2,
                                  "notified": False, "held": 0, "last_seen": self.now - (2000-i)}
                   for i in range(2000)})
        self.store.observe("another-job", "A")
        rows = json.loads(self.path.read_bytes())
        self.assertEqual(len(rows), 2000)
        self.assertIn("another-job", rows)
        self.assertNotIn("job-0000", rows)
        self.assertIn("job-0001", rows)
