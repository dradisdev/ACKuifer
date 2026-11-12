"""Offline test of the Laserfiche folder-listing scroll (D-1). No network, no DB.

Loads a fake portal page into local Chromium: an overflow:hidden wrapper that
looks scrollable (the DIV.v-outer decoy that caused the July 2026 cutoff) and a
virtual scroller that renders only the rows on screen. Proves every row is
collected, with and without the .p-virtualscroller class, and that the portal's
"N Entries" figure parses. Run: python3 tests/test_laserfiche_scroll_offline.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DATABASE_URL", "postgresql://offline:offline@localhost:1/offline")

from playwright.sync_api import sync_playwright  # noqa: E402

from app.scrapers import laserfiche as lf  # noqa: E402

ROW_H = 40


def fake_portal(n_rows: int, scroller_class: str) -> str:
    """A page shaped like the portal: decoy wrapper first, virtual list inside."""
    return f"""<!doctype html><html><body style="margin:0;height:720px;overflow:hidden">
<div class="v-outer" style="height:543px;overflow:hidden">
  <div style="height:637px">
    <div id="list" class="{scroller_class}" style="height:543px;overflow-y:auto">
      <div id="spacer" style="position:relative;height:{n_rows * ROW_H}px"></div>
    </div>
  </div>
</div>
<span>{n_rows:,} Entries</span>
<script>(() => {{
  const N = {n_rows}, H = {ROW_H};
  const list = document.getElementById('list'), spacer = document.getElementById('spacer');
  function render() {{
    const first = Math.floor(list.scrollTop / H);
    const last = Math.min(N, first + Math.ceil(list.clientHeight / H) + 1);
    let html = '';
    for (let i = first; i < last; i++) {{
      const top = i * H;
      const href = (i % 7 === 0 ? 'DocView.aspx?id=' : 'Browse.aspx?id=') + (500000 + i) + '&repo=x';
      html += `<div style="position:absolute;top:${{top}}px;height:${{H}}px"><a href="${{href}}">21 ${{i}}</a></div>`;
    }}
    spacer.innerHTML = html;
  }}
  list.addEventListener('scroll', render);
  render();
}})();</script></body></html>"""


def collect(page, n_rows, scroller_class):
    page.set_content(fake_portal(n_rows, scroller_class))
    return lf._extract_all_links_with_scroll(page)


def main() -> int:
    failures = []

    def check(name, cond, detail=""):
        print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail else ""))
        if not cond:
            failures.append(name)

    # Entry-count parsing (no browser needed).
    check("parse '103 Entries'", lf._parse_entry_count("Map 21\n103 Entries\n") == 103)
    check("parse '1 Entry'", lf._parse_entry_count("1 Entry") == 1)
    check("parse '6,519 Entries'", lf._parse_entry_count("6,519 Entries") == 6519)
    check("parse missing -> None", lf._parse_entry_count("no count here") is None)
    # Empty folder, as the portal showed it for Map 21 parcel "21 3" Reports on 2026-11-11.
    check("parse empty folder -> 0", lf._parse_entry_count(
        "Reports Table finished loading Name Page count Template name No results found Reports") == 0)

    # Speed up the scroll loop for the test; behaviour is unchanged.
    lf.settings.laserfiche_scroll_settle_ms = 20

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 720})

        # Map 66 size, the largest folder in the 2026-11-09 census.
        items = collect(page, 477, "p-virtualscroller p-virtualscroller-inline")
        ids = {i["id"] for i in items}
        check("477-row virtual scroller fully collected", len(ids) == 477, f"got {len(ids)}")
        docs = [i for i in items if i["type"] == "document"]
        check("documents typed correctly", len(docs) == len(range(0, 477, 7)), f"got {len(docs)}")
        check("last row present", any(i["name"] == "21 476" for i in items))
        check("read_entry_count matches", lf._read_entry_count(page) == 477)

        # No settle wait at all: the crawler must still wait for the list to
        # redraw after each scroll, or it reads stale rows and skips a screen.
        lf.settings.laserfiche_scroll_settle_ms = 0
        counts = [len({i["id"] for i in collect(page, 477, "p-virtualscroller")}) for _ in range(10)]
        lf.settings.laserfiche_scroll_settle_ms = 20
        check("477 rows collected with no settle wait (10 runs)", counts == [477] * 10, f"got {counts}")

        # Fallback path: scroller without the PrimeNG class.
        items = collect(page, 103, "")
        check("fallback (no class) collects all 103", len(items) == 103, f"got {len(items)}")

        # Short folder that doesn't scroll at all.
        items = collect(page, 5, "p-virtualscroller")
        check("non-scrolling 5-row folder", len(items) == 5, f"got {len(items)}")

        # D-2 coverage guard: the portal says 50 entries but only 30 rows exist.
        stats = {"short_folders": [], "failed_folders": []}
        page.set_content(fake_portal(30, "p-virtualscroller").replace("30 Entries", "50 Entries"))
        lf._list_folder(page, "Map 66", stats)
        check("short folder recorded", stats["short_folders"] == [{"name": "Map 66", "collected": 30, "expected": 50}],
              f"got {stats['short_folders']}")
        status, message = lf._final_status(stats, None)
        check("short folder makes the run degraded", status == "degraded" and "Map 66 (30/50)" in message, message)

        page.set_content(fake_portal(30, "p-virtualscroller"))
        clean = {"short_folders": [], "failed_folders": []}
        lf._list_folder(page, "Map 21", clean)
        check("full folder not recorded", clean["short_folders"] == [])
        check("clean full run is success", lf._final_status(clean, None) == ("success", None))
        check("clean one-map run is partial", lf._final_status(clean, "21") == ("partial", None))
        failed = {"short_folders": [], "failed_folders": [{"id": "9", "name": "Map 79", "error": "x"}]}
        check("failed folder makes the run degraded", lf._final_status(failed, "21")[0] == "degraded")

        # Navigation failure is retried, then recorded rather than silently dropped.
        lf.settings.laserfiche_nav_retries = 1
        calls = {"n": 0}
        real_goto = page.goto

        def failing_goto(*a, **k):
            calls["n"] += 1
            raise TimeoutError("simulated timeout")

        page.goto = failing_goto
        failed = []
        ok = lf._navigate_and_wait(page, "999", "Map 79", failed)
        page.goto = real_goto
        check("failed nav returns False", ok is False)
        check("failed nav retried once", calls["n"] == 2, f"calls={calls['n']}")
        check("failed nav recorded", failed and failed[0]["id"] == "999" and failed[0]["name"] == "Map 79")

        # Bot check: never treated as an empty folder; backs off, records, then stops the run.
        lf.settings.laserfiche_folder_delay_ms = 0
        lf.settings.laserfiche_block_backoff_seconds = 0
        lf.settings.laserfiche_max_consecutive_blocks = 2
        lf.reset_block_state()

        class _Resp:
            def __init__(self, status):
                self.status = status

        def challenge_goto(*a, **k):
            page.set_content("<html><body>Performing security verification</body></html>")
            return _Resp(429)

        def challenge_200_goto(*a, **k):
            page.set_content("<html><body>Performing security verification "
                             "<iframe src='about:blank' data-x='challenges.cloudflare.com'></iframe></body></html>")
            return _Resp(200)

        def good_goto(*a, **k):
            page.set_content(fake_portal(3, "p-virtualscroller"))
            return _Resp(200)

        failed = []
        page.goto = challenge_goto
        ok = lf._navigate_and_wait(page, "1", "Map 21", failed)
        check("blocked folder returns False", ok is False)
        check("blocked folder recorded as bot check", failed and "bot check" in failed[0]["error"])

        page.goto = good_goto
        check("good folder after a block loads", lf._navigate_and_wait(page, "2", "Map 22", failed) is True)
        check("success resets block counter", lf._block_state["consecutive"] == 0)

        page.goto = challenge_200_goto
        lf._navigate_and_wait(page, "3", "Map 23", failed)
        check("challenge page with HTTP 200 detected", "bot check" in failed[-1]["error"])
        try:
            lf._navigate_and_wait(page, "4", "Map 24", failed)
            raised = False
        except lf.PortalBlockedError:
            raised = True
        check("2 blocked folders in a row stop the run", raised)
        page.goto = real_goto

        browser.close()

    print(f"\n{len(failures)} failure(s)" if failures else "\nALL PASS")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
