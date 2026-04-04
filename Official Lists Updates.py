"""
Scrape two official runtime Letterboxd lists in list order, trim CSV backups to
250 films, and compare to the CSVs in Outputs.

The CSV is treated as the *current* list; the website scrape may lag (e.g. last
week’s order). *In* = films in the CSV but not on the scraped page; *Out* = films
on the scraped page but not in the CSV. HTML is written to the stats .txt files.
"""
from __future__ import annotations

import csv
import html
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from time import sleep
from typing import List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# (list url, csv filename, stats output filename)
LISTS: Sequence[Tuple[str, str, str]] = (
    (
        "https://letterboxd.com/bigbadraj/list/top-250-films-of-more-than-150-minutes/",
        "150_Minutes_or_More_top_movies.csv",
        "150_Minutes_or_More_top_movies_stats.txt",
    ),
    (
        "https://letterboxd.com/bigbadraj/list/top-250-narrative-films-of-less-than-100/",
        "100_Minutes_or_Less_top_movies.csv",
        "100_Minutes_or_Less_top_movies_stats.txt",
    ),
)

MAX_FILMS = 250
FETCH_WORKERS = 8


def ordinal_day(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def changes_from_date_phrase(as_of: date) -> str:
    """e.g. April 4th, 2026's Update (no leading 'Changes from')."""
    month = as_of.strftime("%B")
    return f"{month} {ordinal_day(as_of.day)}, {as_of.year}'s Update"


def get_paths() -> dict:
    import platform

    system = platform.system()
    if system == "Windows":
        base_dir = r"C:\Users\bigba\aa Personal Projects\Letterboxd-List-Scraping"
    elif system == "Darwin":
        base_dir = "/Users/calebcollins/Documents/Letterboxd List Scraping"
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return {"base_dir": base_dir, "output_dir": os.path.join(base_dir, "Outputs")}


PATHS = get_paths()
OUTPUT_DIR = PATHS["output_dir"]


def read_csv_file_text(path: str) -> str:
    """Decode CSV bytes; UTF-8 first, then Windows-1252 (Excel / NBSP), then Latin-1."""
    with open(path, "rb") as f:
        raw = f.read()
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def create_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
        }
    )
    return session


def normalize_film_path(href: Optional[str]) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("http"):
        href = urlparse(href).path or href
    if not href.startswith("/"):
        href = "/" + href
    href = href.split("?")[0].rstrip("/") + "/"
    return href


def collect_ordered_paths(session: requests.Session, base_url: str, max_films: int) -> List[str]:
    """Film paths in list order (/film/slug/), same DOM logic as Letterboxd List Scraping CSV.py."""
    base = base_url.rstrip("/") + "/"
    paths: List[str] = []
    page = 1
    while len(paths) < max_films:
        url = f"{base}page/{page}/"
        response = session.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        film_list = soup.find("ul", class_="poster-list")
        if not film_list:
            break
        for li in film_list.find_all("li", class_="posteritem"):
            if len(paths) >= max_films:
                break
            film_url = None
            inner_div = li.find("div", class_="react-component")
            if inner_div:
                film_url = inner_div.get("data-target-link") or inner_div.get("data-item-link")
            if not film_url:
                anchor = li.find("a", href=True)
                if anchor:
                    film_url = anchor["href"]
            if not film_url:
                film_link = li.find("a", href=lambda x: x and "/film/" in x)
                if film_link:
                    film_url = film_link["href"]
            if film_url:
                paths.append(normalize_film_path(film_url))
        if not soup.find("a", class_="next"):
            break
        page += 1
    return paths[:max_films]


def fetch_og_title_year(
    session: requests.Session, film_path: str, polite_sleep: bool = True
) -> Tuple[str, str]:
    """Return (display_title, year) from film page og:title."""
    try:
        r = session.get(f"https://letterboxd.com{film_path}", timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        og = soup.find("meta", property="og:title")
        if not og or not og.get("content"):
            return film_path, ""
        title_text = og["content"]
        year = ""
        if "(" in title_text and ")" in title_text:
            year = title_text[title_text.rindex("(") + 1 : title_text.rindex(")")]
            title = title_text[: title_text.rindex("(")].strip()
        else:
            title = title_text.strip()
        if polite_sleep:
            sleep(0.05)
        return title, year
    except Exception:
        return film_path, ""


def fetch_metadata_parallel(paths: Sequence[str]) -> List[Tuple[str, str]]:
    """Same order as paths; one session per worker (thread-safe)."""
    if not paths:
        return []
    results: List[Optional[Tuple[str, str]]] = [None] * len(paths)

    def job(idx: int, path: str) -> Tuple[int, Tuple[str, str]]:
        sess = create_session()
        return idx, fetch_og_title_year(sess, path, polite_sleep=False)

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        futures = [pool.submit(job, i, p) for i, p in enumerate(paths)]
        for fut in as_completed(futures):
            idx, pair = fut.result()
            results[idx] = pair
    return [(r if r is not None else ("", "")) for r in results]


def trim_csv_to_max_films(csv_path: str, max_data_rows: int) -> None:
    """Keep header row + first max_data_rows data rows."""
    text = read_csv_file_text(csv_path)
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return
    limit = 1 + max_data_rows
    if len(rows) <= limit:
        return
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows[:limit])


def load_csv_rows(csv_path: str) -> Tuple[List[dict], List[str]]:
    text = read_csv_file_text(csv_path)
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    return list(reader), list(fieldnames)


def link_from_row(row: dict, fieldnames: Sequence[str]) -> str:
    """Prefer `Link` header; otherwise use column D (fourth column, index 3)."""
    raw = (row.get("Link") or "").strip()
    if raw:
        return raw
    if len(fieldnames) > 3:
        key = fieldnames[3]
        return (row.get(key) or "").strip()
    return ""


def read_snapshot(rows: List[dict], fieldnames: Sequence[str]) -> Tuple[List[str], List[str]]:
    """Ordered film paths and display titles from CSV (Link or column D)."""
    paths: List[str] = []
    titles: List[str] = []
    for row in rows:
        link = link_from_row(row, fieldnames)
        p = normalize_film_path(link)
        paths.append(p)
        titles.append((row.get("Title") or "").strip() or p)
    return paths, titles


def diff_csv_current_vs_stale_scrape(
    csv_paths: Sequence[str],
    csv_titles: Sequence[str],
    scrape_paths: Sequence[str],
) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
    """CSV = up-to-date list; scrape = older site state (order may lag).

    *In* — in the CSV but not on the scraped list: csv_paths − scrape_paths.
    Ranks are positions in the CSV (at #). Titles come from the CSV.

    *Out* — on the scraped list but not in the CSV: scrape_paths − csv_paths.
    Ranks are positions on the scraped list (was #). Titles fetched from Letterboxd.
    """
    scrape_set = set(scrape_paths)
    csv_set = set(csv_paths)

    additions: List[Tuple[str, int]] = []
    for i, p in enumerate(csv_paths, start=1):
        if p and p not in scrape_set:
            additions.append((csv_titles[i - 1], i))

    pending: List[Tuple[int, str]] = [
        (i, p) for i, p in enumerate(scrape_paths, start=1) if p and p not in csv_set
    ]
    removals: List[Tuple[str, int]] = []
    if pending:
        meta = fetch_metadata_parallel([p for _, p in pending])
        for (rank, _), (title, _) in zip(pending, meta):
            removals.append((title, rank))

    return additions, removals


def format_change_lines(items: Sequence[Tuple[str, int]], kind: str) -> str:
    """kind: 'in' uses one <i> per title; 'out' matches Letterboxd-style single <i> block."""
    if not items:
        return ""
    if kind == "out":
        inner = "<br>".join(
            f"{html.escape(title)} (was&nbsp;#{rank})" for title, rank in items
        )
        return f"<i>{inner}</i>"
    parts = []
    for title, rank in items:
        safe = html.escape(title)
        parts.append(f"<i>{safe} (at&nbsp;#{rank})</i>")
    return "<br>".join(parts)


def blockquote_html(
    as_of: date,
    additions: List[Tuple[str, int]],
    removals: List[Tuple[str, int]],
) -> str:
    in_block = format_change_lines(additions, "in")
    out_block = format_change_lines(removals, "out")
    if not in_block and not out_block:
        body = "<p><i>No changes since the last update.</i></p>"
    else:
        in_html = in_block or "<i>—</i>"
        out_html = out_block or "<i>—</i>"
        body = (
            f"<p><b>In:</b><br>{in_html}</p>"
            f"<p><b>Out:</b><br>{out_html}</p>"
        )
    header = html.escape(f"Changes from {changes_from_date_phrase(as_of)}:")
    return (
        f"<blockquote>"
        f"<p><b><i>{header}</i></b></p>"
        f"{body}"
        f"</blockquote>"
    )


def run() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    session = create_session()
    chunks: List[str] = []
    as_of = date.today()

    for base_url, csv_name, stats_name in LISTS:
        csv_path = os.path.join(OUTPUT_DIR, csv_name)
        if not os.path.isfile(csv_path):
            raise FileNotFoundError(f"Missing CSV: {csv_path}")

        trim_csv_to_max_films(csv_path, MAX_FILMS)
        old_rows, fieldnames = load_csv_rows(csv_path)

        scrape_paths = collect_ordered_paths(session, base_url, MAX_FILMS)
        csv_paths, csv_titles = read_snapshot(old_rows, fieldnames)
        additions, removals = diff_csv_current_vs_stale_scrape(
            csv_paths, csv_titles, scrape_paths
        )

        block = blockquote_html(as_of, additions, removals)
        chunks.append(block)

        stats_path = os.path.join(OUTPUT_DIR, stats_name)
        with open(stats_path, "w", encoding="utf-8", newline="") as out:
            out.write(block + "\n")

    return "\n\n".join(chunks)


if __name__ == "__main__":
    run()
    print("Stats outputs created.")