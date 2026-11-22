import unittest
from datetime import datetime, timezone


try:
    import core
except ModuleNotFoundError:
    core = None


class RepeatDecisionTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(core, "repeat-pause core is not implemented")
        self.now = 1_790_000_000
        self.settings = core.Settings()

    def next(self, previous, response, job="watch"):
        return core.advance(previous, response, job, self.settings, self.now)

    def test_threshold_notice_once_then_hold_and_reset_on_change(self):
        entry, rewrite = self.next(None, "A")
        self.assertIsNone(rewrite)
        entry, rewrite = self.next(entry, "A")
        self.assertIsNone(rewrite)
        entry, rewrite = self.next(entry, "A")
        self.assertEqual(rewrite, "A\n\nThis job has sent the same result 3 runs in a row. "
                         "Further identical results will be held until something changes. "
                         "To end it, pause the job: hermes cron pause watch")
        entry, rewrite = self.next(entry, "A")
        self.assertEqual(rewrite, "[SILENT]")
        self.assertEqual(entry["held"], 1)
        entry, rewrite = self.next(entry, "B")
        self.assertIsNone(rewrite)
        self.assertFalse(entry["notified"])
        entry, rewrite = self.next(entry, "A")
        self.assertIsNone(rewrite)
        self.assertEqual(entry["streak"], 1)
        self.assertEqual(entry["held"], 1)
        self.assertEqual(entry["last_seen"], self.now)

    def test_hold_disabled_notifies_once_but_fourth_is_unchanged(self):
        self.settings = core.Settings(hold_repeats=False)
        entry, rewrite = None, None
        for _ in range(3):
            entry, rewrite = self.next(entry, "A")
        self.assertIn("3 runs in a row", rewrite)
        for _ in range(3):
            entry, rewrite = self.next(entry, "A")
            self.assertIsNone(rewrite)
            self.assertEqual(entry["held"], 0)

    def test_normalization_numbers_iso_dates_and_times_and_case(self):
        pairs = (
            ("  12 new items  at 09:00", "14 NEW items\tat 10:00  "),
            ("on 2026-11-21 at 2026-11-21T09:00:05.123-06:00", "ON 2026-11-22 at 2026-11-22T10:13:59Z"),
            ("Current check 9:00 AM: +1,224.30 units", "current check 12:30:59 pm: -312.21 units"),
        )
        for first, second in pairs:
            with self.subTest(first=first):
                self.assertEqual(core.fingerprint(first), core.fingerprint(second))
                self.assertNotEqual(core.fingerprint(first, False), core.fingerprint(second, False))
        self.assertEqual(core.fingerprint("  New\tITEMS  ", False), core.fingerprint("new items", False))
        self.assertNotEqual(core.fingerprint("rack12"), core.fingerprint("rack14"))
        self.assertNotEqual(core.fingerprint("version v1.2"), core.fingerprint("version v1.3"))

    def test_threshold_and_notice_placeholders_keep_original_body(self):
        self.settings = core.Settings(threshold=2, notice="{job_id}: {n} repeats")
        entry, _ = self.next(None, "  Result \n", "daily_watch")
        _, rewrite = self.next(entry, "  RESULT\t", "daily_watch")
        self.assertEqual(rewrite, "  RESULT\t\n\ndaily_watch: 2 repeats")

    def test_clock_format_for_command(self):
        expected = datetime.fromtimestamp(self.now, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.assertEqual(core.format_time(self.now), expected)
