---
description: Stop mid-item; record a "Resume here" checklist, back up the branch, and log the pause
argument-hint: "[optional item ID, e.g. D-1]"
---

Pause the current launch-plan item so it can be picked up cleanly next time.

## 1. Find the item and its state
- The item is `$ARGUMENTS` if given. Otherwise it's the item worked on in this conversation, or the one named by the current branch (e.g. `fix/d1-...` → D-1). If it's still unclear, ask.
- Run `git status`, `git branch --show-current`, `git log --oneline staging..HEAD` and `git status -sb` (to see if it's ahead of its remote).
- Check for anything still running (background jobs, probes, scrapes). Stop it or note it. Don't leave something running unrecorded.
- If there are uncommitted changes to tracked files, list them and ask: commit them to the feature branch as work in progress (`chore: wip <item ID> ...`), or leave them. Never commit `FINISH_PLAN.md`.

## 2. Record "Resume here" in `FINISH_PLAN.md`
Under the item (after its **Done when**), add or replace a dated block. Remove any older "Where X stands" or "Resume here" block for the same item, so there's only one.
- **Where <item ID> stands (YYYY-MM-DD):**
  - What's committed: the branch, each commit hash with a few words, whether it's pushed, and whether it's merged to `staging`.
  - What's only local: private files such as gitignored `scripts/` and `FINISH_PLAN.md` itself.
  - What was running, blocked, or failing, and why, in plain words.
  - The plan for next time, plus any decisions made in this session.
- **Resume here (run in order; don't start a step until the previous one has finished):** numbered steps, each with:
  - the exact command, and
  - what a good result looks like, and what to do if it isn't good.

  Step 1 is always switching to the branch and checking the top commit. Step 2 is always running the tests. The last step says where to pick up `/next` (e.g. "continue at step 5, ask before merging to `staging`").

`FINISH_PLAN.md` is private and gitignored. Never `git add` it.

## 3. Back up the branch
- If the current branch is a feature branch (not `staging` or `main`) with unpushed commits, push it: `git push -u origin <branch>`. Feature branches don't deploy.
- Never push `staging` or `main` from here without asking first, because both auto-deploy.
- Update the "Where it stands" block to say the branch is pushed.

## 4. Session log in `CLAUDE.md`
- Append exactly two lines to `## Session Log` (newest at the bottom):
  ```
  - YYYY-MM-DD — <item ID>: paused — <state in one line, e.g. "fix committed; live check blocked">
    Resume: see "Resume here" under <item ID> in FINISH_PLAN.md
  ```
- `CLAUDE.md` is public. Keep out hosts, IPs, credentials, URLs of private environments, doc-level PFAS results tied to a parcel, and house numbers.
- Commit it to the **current feature branch** (`docs: session log, <item ID> paused`) and push the branch. It reaches `staging` when the branch merges. If you're on `staging` or `main`, commit but ask before pushing.

## 5. Tell the operator how to resume, in exactly two lines
```
<item ID> paused: <state in a few words>; branch <branch> pushed.
To resume: run /next — it will start from "Resume here" step 1 (`<step 1 command>`).
```
