"""Offline test of MassDEP document-list paging (D-5). No network, no DB.

Loads a fake EEA portal page into local Chromium, shaped like the real one
(2026-11-18): an ngx-bootstrap paginator (li.pagination-next, "disabled" on the
last page, <a href=""> links) over a list of get-file document links, plus an
unpaged section. Proves every page is read, and that a paginator that breaks
or never ends is reported (the run is then "degraded").
Run: python3 tests/test_massdep_paging_offline.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DATABASE_URL", "postgresql://offline:offline@localhost:1/offline")

from playwright.sync_api import sync_playwright  # noqa: E402

from app.config import settings  # noqa: E402
from app.scrapers import massdep as md  # noqa: E402

BASE = "https://eeaonline.eea.state.ma.us/DEP/WscNBApi/documents/get-file/"
PER_PAGE = 50
UNPAGED = 11  # the real page has 51 links for 40 paged items


def fake_portal(n_items: int, mode: str = "ok") -> str:
    """mode: 'ok' | 'stuck' (Next does nothing) | 'endless' (Next never disables)
    | 'slow' (page redraws after 1.5 s)."""
    return f"""<!doctype html><html><body>
<div id="unpaged">{''.join(f'<a href="{BASE}edep{i}?fileSource=EDEP">BWSC102 Form {i}</a>' for i in range(UNPAGED))}</div>
<table><tbody id="rows"></tbody></table>
<pagination><ul class="pagination">
  <li id="prev" class="pagination-prev page-item"><a href="" class="page-link">Previous</a></li>
  <li id="next" class="pagination-next page-item"><a href="" class="page-link">Next</a></li>
</ul></pagination>
<script>(() => {{
  const N = {n_items}, P = {PER_PAGE}, MODE = '{mode}';
  let pg = 0;
  const pages = Math.max(1, Math.ceil(N / P));
  function render() {{
    let html = '';
    const lo = MODE === 'endless' ? 0 : pg * P;
    const hi = MODE === 'endless' ? P : Math.min(N, lo + P);
    for (let i = lo; i < hi; i++) {{
      const id = MODE === 'endless' ? 'p' + pg + '_' + i : 'bear' + i;
      html += '<tr><td><a href="{BASE}' + id + '?fileSource=BEAR">Doc ' + id + '.pdf</a></td></tr>';
    }}
    document.getElementById('rows').innerHTML = html;
    const last = MODE !== 'endless' && pg >= pages - 1;
    document.getElementById('next').className = 'pagination-next page-item' + (last ? ' disabled' : '');
    document.getElementById('prev').className = 'pagination-prev page-item' + (pg === 0 ? ' disabled' : '');
  }}
  document.querySelector('#next a').addEventListener('click', e => {{
    e.preventDefault();
    if (document.getElementById('next').classList.contains('disabled') || MODE === 'stuck') return;
    pg++;
    if (MODE === 'slow') setTimeout(render, 1500); else render();
  }});
  render();
}})();</script></body></html>"""


failures = 0


def check(name, cond, detail=""):
    global failures
    print(("PASS " if cond else "FAIL ") + name + (f"  ({detail})" if detail else ""))
    if not cond:
        failures += 1


def run(page, n, mode="ok"):
    page.set_content(fake_portal(n, mode))
    return md._collect_all_pages(page)


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    # 1. Today's shape: 40 items, one page, Next disabled
    docs, problem = run(page, 40)
    check("40 items: one page read, no problem", problem is None and len(docs) == 40 + UNPAGED,
          f"{len(docs)} docs, problem={problem}")

    # 2. Exactly a full page: Next disabled, nothing more
    docs, problem = run(page, 50)
    check("50 items: one full page, no problem", problem is None and len(docs) == 50 + UNPAGED,
          f"{len(docs)} docs, problem={problem}")

    # 3. 51 items: the old scraper would miss item 51; now page 2 is read
    docs, problem = run(page, 51)
    urls = {d["url"] for d in docs}
    check("51 items: page 2 read, last item found",
          problem is None and len(docs) == 51 + UNPAGED and f"{BASE}bear50?fileSource=BEAR" in urls,
          f"{len(docs)} docs, problem={problem}")

    # 4. 120 items over 3 pages, no duplicates of the unpaged section
    docs, problem = run(page, 120)
    check("120 items: 3 pages, every item once",
          problem is None and len(docs) == 120 + UNPAGED and len({d['url'] for d in docs}) == len(docs),
          f"{len(docs)} docs, problem={problem}")

    # 5. Slow redraw (1.5 s) still read
    docs, problem = run(page, 75, "slow")
    check("slow redraw: page 2 still read", problem is None and len(docs) == 75 + UNPAGED,
          f"{len(docs)} docs, problem={problem}")

    # 6. Next enabled but does nothing -> problem reported, page 1 kept
    saved = settings.massdep_page_change_timeout_ms
    settings.massdep_page_change_timeout_ms = 2000
    docs, problem = run(page, 120, "stuck")
    check("stuck Next: problem reported, page 1 kept",
          problem is not None and "did not load" in problem and len(docs) == 50 + UNPAGED,
          f"{len(docs)} docs, problem={problem}")
    settings.massdep_page_change_timeout_ms = saved

    # 7. Next never disables -> stops at the page limit with a problem
    saved_max = settings.massdep_max_pages
    settings.massdep_max_pages = 4
    docs, problem = run(page, 0, "endless")
    check("endless Next: stops at page limit, problem reported",
          problem is not None and "limit" in problem and len(docs) == 4 * PER_PAGE + UNPAGED,
          f"{len(docs)} docs, problem={problem}")
    settings.massdep_max_pages = saved_max

    # 8. No paginator at all (layout change) -> no paging problem, links still read
    page.set_content(f'<a href="{BASE}x1?fileSource=BEAR">x1.pdf</a>')
    docs, problem = md._collect_all_pages(page)
    check("no paginator: links read, no problem", problem is None and len(docs) == 1,
          f"{len(docs)} docs, problem={problem}")

    browser.close()

# 9. A degraded MassDEP run still sends digests (same rule as Laserfiche)
check("degraded is a notifiable run status", "degraded" in md.NOTIFIABLE_RUN_STATUSES)

print(f"\n{'ALL PASS' if not failures else f'{failures} FAILED'}")
sys.exit(1 if failures else 0)
