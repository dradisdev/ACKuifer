# ACKuifer — Claude Code Instructions

## Remaining Work
`FINISH_PLAN.md` is the source of truth for remaining work before launch: prioritized
items, "done when" tests, decisions, and the launch schedule. Read it before starting
work, and update it when an item is done or a decision changes. It is private
(gitignored, like `BUILD_NOTES.txt`) because it holds operational and security details;
never `git add -f` it.

## Git Workflow
Work flows dev → staging → main.
- Feature and fix work commits to the `staging` branch (or a short-lived branch merged into `staging`). Never commit feature work directly to `main`.
- `staging` auto-deploys to the Railway **staging** environment (separate preview URL, separate database). Validate there before promoting.
- Promote to production by merging `staging` → `main`. `main` auto-deploys to the production environment.
- The ONLY direct-to-main change permitted is an urgent production hotfix, and even then prefer routing through staging if time allows.
- Migrations and data-mutating scripts are rehearsed in staging before they touch production.
- The `scripts/` directory is gitignored BY DESIGN: the repo is public, and ad-hoc
  diagnostics/local tooling stay private by default. The ops scripts already tracked
  there (backfill_pattern_a.py, etc.) remain tracked and commit normally. Publishing
  a NEW script is a deliberate act: `git add -f scripts/<name>.py`. If `git add`
  warns that `scripts` is ignored, that's this policy working — not an error.

## Commit Messages
Use conventional commits: feat:, fix:, chore:, docs:

## Stack
Python 3.11+, FastAPI, PostgreSQL, Playwright, Mapbox GL JS, Railway deployment.
Local dev is macOS — use python3, not python.

## Key Rules
- Never commit .env or any file containing secrets
- Never hardcode constants — use named constants from app/config.py
- House numbers are never displayed publicly — street name only
- Run existing tests before committing if tests exist
- Data-mutating scripts must require `--expect-env` and verify DB identity
  (inet_server_addr + current_database + row count) before any write

## Pausing
If work stalls (blocked, waiting on something external, failing repeatedly) or the
operator says they're stopping for the day, offer to run `/pause`. It records a
"Resume here" checklist in `FINISH_PLAN.md`, pushes the feature branch as a backup,
and logs the pause. `/next` resumes from that checklist.

## Session Log
Two dated lines per completed or paused item, appended by `/next` or `/pause` (newest at the bottom).
This file is public: no hosts, IPs, credentials, or house numbers.
- 2026-11-10 — D-1: paused — crawler listing fix committed and tested; live census blocked by the portal's bot check
  Resume: see "Resume here" under D-1 in FINISH_PLAN.md
- 2026-11-11 — D-1: crawler now lists every Laserfiche folder in full (scroll fix, redraw wait, bot-check handling, empty folders read as 0); full portal census matched on every folder
  Verified on staging; awaiting promotion to main (staging crawl still running)
- 2026-11-11 — D-2: paused — coverage guard committed and tested; merge to staging waits for the running staging crawl
  Resume: see "Resume here" under D-2 in FINISH_PLAN.md
- 2026-11-11 — D-2: paused — coverage guard merged to staging; D-1 staging crawl checked clean; capped-run fire drill not yet run
  Resume: see "Resume here" under D-2 in FINISH_PLAN.md
- 2026-11-12 — D-2: coverage guard flags short folder listings as degraded and the dead man's switch alerts on them; capped-run fire drill produced a degraded run and an alert email
  Verified on staging; awaiting promotion to main (with D-7 and D-3b)
- 2026-11-13 — D-9: a failed digest no longer marks its results as notified (retried next run, failure noted on the run); a failed dead man's alert no longer starts its cooldown (N-12)
  Verified on staging; awaiting promotion to main (with D-1, D-2, D-7 and D-3b)
- 2026-11-14 — D-7: new subscribers only get results discovered after they subscribed (per neighborhood; email and SMS), so a first digest no longer floods them with old records
  Verified on staging; awaiting promotion to main (with D-1, D-2, D-9 and D-3b)
- 2026-11-15 — N-11: a deploy no longer starts a full Laserfiche crawl; the crawler runs only at its scheduled time or when a catch-up or forced run is asked for
  Verified on staging; awaiting promotion to main (with D-1, D-2, D-7, D-9 and D-3b)
- 2026-11-15 — D-3b: paused — label code and migration on staging; labelled-result check waits on the D-3 rehearsal and the production fingerprint check
  Resume: see "Resume here" under D-3b in FINISH_PLAN.md
- 2026-11-15 — D-3b: paused — staging catch-up rehearsal running (two docs reset to re-find); label check waits for the crawl to finish
  Resume: see "Resume here" under D-3b in FINISH_PLAN.md
- 2026-11-16 — D-3b: digest emails and SMS label a report as "older report, newly added" when it was sampled more than 90 days before ACKuifer found it; the map shows no label, and the catch-up switch only starts a crawl
  Verified on staging; awaiting promotion to main (with D-1, D-2, D-7, D-9 and N-11)
- 2026-11-16 — D-3 (street names): each street shows one standard spelling on the map, in the results list, digests and SMS (abbreviations spelled out, known corrections such as Fifth Way and Millbrook Road), and stray house-number fractions or extra address text are never shown
  Verified on staging; D-3 itself waits for the Dec 2 promotion and catch-up
- 2026-11-17 — D-8 (street names): the operator's decisions on the remaining street spellings are applied on the map, in the results list, digests and SMS (missing suffixes added, apostrophes dropped, Long Pond Drive, Shawkemo Hills Lane, Brier Patch Road, free text trimmed); Miacomet, West Miacomet and Somerset pairs stay separate, and place-name searches are unchanged
  Verified on staging; goes out with the Dec 2 promotion; D-8 seeded-row check still open
- 2026-11-18 — D-5: paused — MassDEP checked against the portal (all documents ingested, no parse errors, daily schedule kept on purpose); the scraper now reads every page of the list, waiting for the first scheduled staging run
  Resume: see "Resume here" under D-5 in FINISH_PLAN.md
- 2026-11-18 — D-6: production has no parser-garbled PFAS6 totals left (no backfill needed); the three discrepancies found since June match their lab reports; the Partition A scripts check the current database identity and run dry in a read-only session
  Verified on production (read-only) after a staging rehearsal; script changes go to staging with this merge
