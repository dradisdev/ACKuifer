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
