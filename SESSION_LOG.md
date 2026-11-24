# ACKuifer — Session Log

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
- 2026-11-19 — D-5: MassDEP runs once a day on schedule (not on every deploy) and reads every page of the portal list; its first scheduled staging run found all 51 documents with no errors
  Verified on staging; goes out with the Dec 2 promotion
- 2026-11-19 — D-8: the 16 prototype-seeded results match a fresh parse of their reports on staging and production (PFAS6, compounds, status, street, sample date), so no backfill is needed; the check reads the portal once and the database read-only
  Verified on staging and production (read-only); street-name display fixes go out with the Dec 2 promotion
- 2026-11-19 — S-5: Railway variables inventoried for every service in both environments; links in digest emails now verify on the site, staging links point at staging, staging services follow database password changes, and unused variables (including SMS) are removed
  Verified on production and staging; production laserfiche-cron cleanup waits for the Dec 4 promotion step
- 2026-11-22 — S-1: on Railway, the site refuses to start without a real admin password and signing key, and the scraper crons without a real signing key (no more "changeme" default); every service logs one line saying whether the check is enforcing
  Verified on staging (including a deliberate placeholder deploy that was refused); goes out with the Dec 4 promotion
- 2026-11-23 — S-3: database scripts and migrations now share one identity check (server and database name, with a wide row-count sanity range), each passes on its own environment and refuses the other, and finished one-off scripts are retired so they can't run by accident
  Verified on staging and production (read-only and dry runs); goes out with the Dec 4 promotion
- 2026-11-24 — S-4: staging database password rotated after earlier exposure; the old password is refused and every staging service runs on the new one
  Verified on staging (site, both scraper crons, and a scheduled MassDEP run); no production change
