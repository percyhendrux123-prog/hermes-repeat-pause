# repeat-pause for Hermes Agent

## What it does

When an agent-backed cron job produces the same result three runs in a row, repeat-pause appends one notice to the third result. Later matches request Hermes's `[SILENT]` delivery behavior. When the result changes, it goes through normally and a new streak starts. **The job keeps running.** The plugin never pauses, edits, or removes it. The default notice shows the `hermes cron pause <job_id>` command for an operator who wants to stop the schedule.

Comparison lowercases text and collapses whitespace. By default it also ignores ISO calendar dates and timestamps, clock times, and standalone numbers. That groups reports such as `12 new items at 09:00` and `14 new items at 10:00`. Set `ignore_numbers: false` if the numbers or timestamps are the point of the report.

- `/repeats` shows job IDs with streaks of at least two, the streak length, the cumulative held count, and the last-seen time (UTC).
- `/repeats reset <job_id>` clears that job's repeat state without changing its schedule. No model-facing tools are registered. Use the command on sessions whose users should be able to view job IDs and clear counters; the plugin adds no authorization beyond Hermes's command routing.

It implements item 5 of the Hermes proactivity roadmap ([NousResearch/hermes-agent#158538](https://github.com/NousResearch/hermes-agent/issues/158538)): same output N runs in a row, one message, then stop repeating.

## Disclosures

- **Changes delivery:** a `transform_llm_output` hook appends the notice once per streak and rewrites later identical cron replies to `[SILENT]`, which Hermes does not deliver. Interactive and gateway chat replies are never changed.
- **Writes:** `<HERMES_HOME>/repeat-pause/streaks.json` and its `.lock` (or `state_dir`), fingerprints and counts only, as described below.
- **Background work:** one in-process storage thread per registration, no backlog. No other processes.
- **None of:** network calls, subprocesses, model calls, credentials, telemetry, self-update, changes to cron jobs.

## How it was made

I designed it and set the behavior. AI agents wrote the code and tests to my spec, under my review, with independent code review rounds and a live smoke test against Hermes's real cron runtime (`cron.scheduler.run_job`, mock model, throwaway Hermes home) before release.

## What it stores

`<state_dir>/streaks.json` records the job ID, the SHA-256 fingerprint of the normalized reply, streak length, whether a notice was requested, the held count, the last-seen timestamp, and whether the end hook confirmed the run. It stores no reply text, prompts, or tool data. A fingerprint is a comparison key, not a guarantee of anonymity for easy-to-guess text. The plugin does not control what Hermes itself stores in transcripts or run output.

The state directory is private (`0700`), state writes are atomic with a unique temporary file (`0600`), and a separate `.lock` file serializes writers without waiting. Use a dedicated directory, not a symlink or shared directory. The plugin refuses symlinked state files, corrupt state, and lock contention and leaves the response unchanged. Expired rows are ignored and pruned on state updates; at most 2,000 jobs are kept, evicting the oldest when needed. Held counts track this plugin's rewrite decisions, not delivery acknowledgments.

Cron callbacks run storage work on a separate thread with no backlog. If that worker is busy or the result isn't ready in 40 ms, the transform leaves the reply alone. An admitted state update can still finish in the background after the callback returns. If an observation or reset can't be saved, the affected job and profile stop using the old streak for notices and holds. The next successful report starts a fresh streak unless a reset has already cleared the old one. This safeguard is in memory; a restart or a different worker process cannot see an update that never reached disk.

Ordinary replies advance the streak only after the end hook confirms a successful turn. Only a match to that confirmed reply body can be held; a held match does not change the reference. Older, unconfirmed records do not establish a streak.

## Settings

All settings are optional under `plugins.entries.repeat-pause.settings` in `config.yaml`. The corresponding `REPEAT_PAUSE_<KEY>` environment variable takes precedence (for example, `REPEAT_PAUSE_THRESHOLD`).

| Setting | Default | Effect |
| --- | --- | --- |
| `threshold` | `3` | Matching runs before the notice; values below 2 are treated as 2. |
| `hold_repeats` | `true` | If `false`, keep delivering after the notice. Pair it with a custom `notice` that doesn't say later replies will be held. |
| `ignore_numbers` | `true` | Ignore standalone numbers and dates/times when comparing. If `false`, only case and whitespace are normalized. |
| `notice` | See below | Text appended once per streak. `{n}` is the streak length; `{job_id}` is the job ID. |
| `state_dir` | `<HERMES_HOME>/repeat-pause` | State location for the active profile. Relative paths are relative to its Hermes home; the parent must already exist. Give each profile a separate path if overriding the default. |
| `forget_days` | `30` | Ignore rows last seen more than this many days ago. Nonnegative integer. |

Default notice:

> This job has sent the same result {n} runs in a row. Further identical results will be held until something changes. To end it, pause the job: hermes cron pause {job_id}

If you change the comparison rules or threshold, use `/repeats reset <job_id>` to apply the new settings from the start of that job's next streak.

## Install and test

The manifest declares Hermes Agent `>=0.22.5` on macOS and Linux. On a Hermes host, copy `plugin.yaml`, `__init__.py`, and `core.py` to `<HERMES_HOME>/plugins/repeat-pause/`, then enable it in the intended profile with `hermes plugins enable repeat-pause`. For rollback, disable it with `hermes plugins disable repeat-pause`; there is no job change to undo. No third-party Python dependencies are needed.

From a source checkout:

```sh
python3 -m unittest discover -s tests
```

The tests exercise the registered callbacks with a fixture Hermes context and real files and locks; no Hermes installation or network is needed.

## Not covered

- Script-mode `no_agent` jobs and pre-agent skips (including monitor-gate unchanged ticks) have no agent turn for the plugin to see.
- Interactive sessions, gateway chat, subagents, and runs with no derivable job ID are ignored.
- Already-silent responses neither reset nor advance the streak. The plugin uses Hermes's cron silence matcher when available and a local fallback otherwise. An empty response or a standalone `[CRON_FAILURE]` line invalidates the streak without replacing the response; the saved streak is reset when storage is available. Runtime failures can reset it when the session end hook reports them; failures outside that hook are not observable.
- An observed tool error, including a failed `write_file` or `patch`, leaves the response unchanged so Hermes's later failed-write advisory stays visible. It invalidates the streak immediately and resets the saved streak when storage is available. If an agent run fails without an observed tool error (for example, an interruption or an iteration limit) and its reply still matches the previous successful report, the early transform may hold it before it learns the outcome. An outcome field available at transform time would close that gap.
- The end hook confirms turn completion before delivery, not receipt by the recipient. It cannot see scheduler-only advisories, such as a provider-switch notice the scheduler omits on silent runs.
- This changes the final response, not streaming or other side effects of a run. Other output-transform plugins may win (Hermes uses the first nonempty rewrite). A notice being requested is not proof it reached a recipient, and there is no “resumed” notice.

## License

[MIT](LICENSE).
