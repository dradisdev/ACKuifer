---
description: Pick the next open FINISH_PLAN.md item, plan it, build it on a branch, and verify it on staging
argument-hint: "[optional item ID, e.g. D-2]"
---

Work the next item from the launch plan. Stop at each **gate** and wait for the operator's reply. Never skip a gate.

## 1. Read
- Read `FINISH_PLAN.md` (private and gitignored, never `git add` it) and the `## Session Log` section of `CLAUDE.md`.
- Run `git status` and `git branch --show-current`. If the tree isn't clean, stop and say what's uncommitted. (Being on a paused item's feature branch is fine.)

## 2. Pick one item
- If an item ID was given (`$ARGUMENTS`), use it.
- Otherwise, an open item with a "Resume here" checklist comes first.
- Otherwise pick the highest-priority **open** item. An item is done when its heading has `✅ DONE`.
  - Follow "Launch sequence" order first. Then go through MUST-HAVE in order (Data integrity → API keys and secrets → Analytics → Working site and feedback). NICE-TO-HAVE comes only after every MUST-HAVE is done.
  - Skip items whose prerequisites aren't done. Skip anything that is a production action, such as D-3's catch-up run or a promotion to `main`. Name it as the next manual step instead.
- Tell the operator which item it is and why, **in one sentence**.
- **Resuming a paused item:** if the item has a **"Resume here"** checklist (written by `/pause`), resume it before anything else, unless the operator named a different item.
  - Say so in the same sentence, with the date of the "Where it stands" block.
  - Skip step 3 (the plan was approved before the pause) and the branch creation in step 4: use the branch the checklist names.
  - Present the checklist as it stands and ask "start at step 1?" (this is the gate). Then follow it in order, stopping at the first step whose result isn't good.
  - When the checklist is finished, continue `/next` from the step it points to.

## 3. Plan → gate
- Propose a short plan: at most 6 bullets, the files you'll touch, and how the item's **Done when** test will be shown.
- Call out anything that writes to a database or needs a migration. Those must use `--expect-env` and the DB identity guard, and must run on staging first.
- **Gate:** wait for approval. Don't edit any files before it.

## 4. Build
- Branch off up-to-date `staging`: `git switch staging && git pull --ff-only && git switch -c <type>/<item-id>-<short-slug>` (e.g. `fix/d2-coverage-guard`).
- Implement the plan. Follow CLAUDE.md rules: constants in `app/config.py`, never display house numbers, no secrets.
- Add or extend an offline test in `tests/` where the item allows it.
- Run every test. The tests are standalone scripts: `for f in tests/test_*.py; do python3 "$f" || echo "FAILED: $f"; done`. Report pass/fail exactly. Don't call it done if anything fails.
- Commit to the branch with a conventional commit message (feat:/fix:/chore:/docs:).

## 5. Deploy to staging → gate
- Ask before merging, because pushing `staging` auto-deploys to the Railway staging environment.
- On approval: `git switch staging && git merge --no-ff <branch> && git push origin staging`.
- Give **plain, numbered steps** to verify on staging, each with what "good" looks like. Base them on the item's **Done when** test. Point to the staging URL in `~/.ackuifer/staging.env` instead of printing it. Include the exact commands to run, if any.
- **Gate:** wait for the operator to confirm that staging looks good. If they report a problem, fix it on the branch and repeat steps 4–5.

## 6. Close out (only after the operator confirms)
- In `FINISH_PLAN.md`, add ` ✅ DONE YYYY-MM-DD` to the item's heading. Delete the item's "Resume here" checklist, if any (keep its evidence lines). Put a short evidence line under its **Done when** with what was checked, on staging, and the commit hash. If only the staging part is done and a production step remains, say so in the evidence line instead of marking it done.
- In `CLAUDE.md`, append to `## Session Log` (newest at the bottom), exactly two lines:
  ```
  - YYYY-MM-DD — <item ID>: <what changed, one line>
    Verified on staging; <next step, e.g. "awaiting promotion to main">
  ```
  `CLAUDE.md` is public. Keep the entry free of hosts, IPs, credentials, doc-level PFAS results tied to a parcel, and house numbers.
- Commit the `CLAUDE.md` change to `staging` (`docs: session log for <item ID>`). Ask before pushing it. Never commit `FINISH_PLAN.md`.
- Don't promote to `main`. Say that promotion is the operator's call.
