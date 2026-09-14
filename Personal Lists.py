"""Compare personal Letterboxd lists to the master list and export corrections."""
from __future__ import annotations

import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


MASTER_LIST_URL = "https://letterboxd.com/bigbadraj/list/every-movie-ive-seen-ranked/"
LISTS_TO_COMPARE = (
    "https://letterboxd.com/bigbadraj/list/sleepaway-camp-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/2026-releases-ranked/",
    "https://letterboxd.com/bigbadraj/list/2025-releases-ranked/",
    "https://letterboxd.com/bigbadraj/list/friday-the-13th-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/halloween-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/v-h-s-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/mission-impossible-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/scream-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/2024-releases-ranked/",
    "https://letterboxd.com/bigbadraj/list/saw-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/nightmare-on-elm-street-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/hannibal-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/marvel-movies-ranked/",
    "https://letterboxd.com/bigbadraj/list/superhero-movies-ranked-1/",
    "https://letterboxd.com/bigbadraj/list/dc-movies-ranked-1/"
)
FETCH_WORKERS = 8


def create_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
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
    if href.startswith("http://") or href.startswith("https://"):
        href = urlparse(href).path
    if not href.startswith("/"):
        href = "/" + href
    return href.split("?")[0].rstrip("/") + "/"


def film_path_from_item(item) -> str:
    inner_div = item.find("div", class_="react-component")
    film_url = None
    if inner_div:
        film_url = inner_div.get("data-target-link") or inner_div.get("data-item-link")
    if not film_url:
        anchor = item.find("a", href=True)
        if anchor:
            film_url = anchor["href"]
    if not film_url:
        film_link = item.find("a", href=lambda value: value and "/film/" in value)
        if film_link:
            film_url = film_link["href"]
    return normalize_film_path(film_url)


def collect_ordered_paths(session: requests.Session, list_url: str) -> List[str]:
    """Collect every film path in the order shown by Letterboxd."""
    base_url = list_url.rstrip("/") + "/"
    film_paths: List[str] = []
    page = 1
    seen_pages = set()

    while page not in seen_pages:
        seen_pages.add(page)
        response = session.get(f"{base_url}page/{page}/", timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        film_list = soup.find("ul", class_="poster-list")
        if not film_list:
            break

        page_paths = [
            path for item in film_list.find_all("li", class_="posteritem")
            if (path := film_path_from_item(item))
        ]
        if not page_paths:
            break
        film_paths.extend(page_paths)
        if not soup.find("a", class_="next"):
            break
        page += 1

    return film_paths


def correct_release_order(master_paths: Sequence[str], release_paths: Sequence[str]) -> List[str]:
    """Sort master-listed films while anchoring unknown films after their current predecessor."""
    master_rank: Dict[str, int] = {path: rank for rank, path in enumerate(master_paths)}
    known_paths = [path for path in release_paths if path in master_rank]
    sorted_known = sorted(known_paths, key=master_rank.__getitem__)

    unknown_after: Dict[Optional[str], List[str]] = {}
    previous_known: Optional[str] = None
    for path in release_paths:
        if path in master_rank:
            previous_known = path
        else:
            unknown_after.setdefault(previous_known, []).append(path)

    corrected: List[str] = []
    corrected.extend(unknown_after.get(None, []))
    for path in sorted_known:
        corrected.append(path)
        corrected.extend(unknown_after.get(path, []))
    return corrected


def fetch_title_year_and_tmdb_id(
    session: requests.Session, film_path: str
) -> Tuple[str, str, str]:
    try:
        response = session.get(f"https://letterboxd.com{film_path}", timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        og_title = soup.find("meta", property="og:title")
        tmdb_element = soup.find(attrs={"data-tmdb-id": True})
        tmdb_id = tmdb_element.get("data-tmdb-id", "") if tmdb_element else ""
        title_text = (og_title.get("content") if og_title else "") or film_path
        year = ""
        title = title_text.strip()
        if "(" in title_text and ")" in title_text:
            close_paren = title_text.rfind(")")
            open_paren = title_text.rfind("(", 0, close_paren)
            if open_paren >= 0:
                year = title_text[open_paren + 1 : close_paren].strip()
                title = title_text[:open_paren].strip()
        sleep(0.05)
        return title, year, tmdb_id
    except requests.RequestException as error:
        print(f"Could not fetch metadata for {film_path}: {error}")
        return film_path, "", ""


def fetch_metadata(paths: Sequence[str]) -> List[Tuple[str, str, str]]:
    results: List[Optional[Tuple[str, str, str]]] = [None] * len(paths)

    def fetch(index: int, path: str) -> Tuple[int, Tuple[str, str, str]]:
        return index, fetch_title_year_and_tmdb_id(create_session(), path)

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
        futures = [executor.submit(fetch, index, path) for index, path in enumerate(paths)]
        for future in as_completed(futures):
            index, metadata = future.result()
            results[index] = metadata
    return [metadata or (path, "", "") for path, metadata in zip(paths, results)]


def get_output_dir() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "Outputs")


def output_filename(list_url: str) -> str:
    list_slug = urlparse(list_url).path.rstrip("/").split("/")[-1]
    return f"Personal_{list_slug.replace('-', '_')}.csv"


def write_corrected_csv(
    paths: Sequence[str], output_path: str, metadata: Dict[str, Tuple[str, str, str]]
) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(["Title", "Year", "tmdbID", "Link"])
        for path in paths:
            title, year, tmdb_id = metadata.get(path, (path, "", ""))
            writer.writerow([title, year, tmdb_id, f"https://letterboxd.com{path}"])


def print_discrepancies(
    current_paths: Sequence[str],
    corrected_paths: Sequence[str],
    metadata: Dict[str, Tuple[str, str, str]],
) -> None:
    current_positions = {path: index for index, path in enumerate(current_paths, start=1)}
    corrected_positions = {path: index for index, path in enumerate(corrected_paths, start=1)}
    moved_paths = [
        path
        for path in current_paths
        if current_positions[path] != corrected_positions.get(path)
    ]

    print("\nFilms that need to move:")
    for path in moved_paths:
        title = metadata.get(path, (path, "", ""))[0]
        print(
            f"  {title}: position #{current_positions[path]} -> "
            f"position #{corrected_positions[path]}"
        )


def compare_list(
    session: requests.Session, master_paths: Sequence[str], list_url: str
) -> None:
    list_name = urlparse(list_url).path.rstrip("/").split("/")[-1]
    print(f"\nReading the {list_name} list...")
    current_paths = collect_ordered_paths(session, list_url)
    print(f"Found {len(current_paths)} films in the {list_name} list.")

    corrected_paths = correct_release_order(master_paths, current_paths)
    if corrected_paths == current_paths:
        print(f"✅ The {list_name} list is correctly ordered relative to the master list.")
        return

    changed_positions = sum(
        current != corrected
        for current, corrected in zip(current_paths, corrected_paths)
    )
    metadata = dict(zip(current_paths, fetch_metadata(current_paths)))
    print(f"\n❌ Discrepancies in {list_name}:")
    print_discrepancies(current_paths, corrected_paths, metadata)
    output_path = os.path.join(get_output_dir(), output_filename(list_url))
    write_corrected_csv(corrected_paths, output_path, metadata)
    print(
        f"The {list_name} list has ordering discrepancies at {changed_positions} "
        f"position(s). Corrected CSV written to {output_path}."
    )


def main() -> None:
    session = create_session()
    print("Reading the master list...")
    master_paths = collect_ordered_paths(session, MASTER_LIST_URL)
    print(f"Found {len(master_paths)} films in the master list.")

    for list_url in LISTS_TO_COMPARE:
        try:
            compare_list(session, master_paths, list_url)
        except requests.RequestException as error:
            print(f"Could not process {list_url}: {error}")
        except Exception as error:
            print(f"Unexpected error processing {list_url}: {error}")


if __name__ == "__main__":
    main()