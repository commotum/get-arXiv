from __future__ import annotations

import argparse
import csv
import random
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode

import requests
from tqdm import tqdm

API_URL = "https://export.arxiv.org/api/query?"
PAGE_SIZE = 50
CSV_PATH = Path(__file__).resolve().parent / "authors.csv"
BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
API_ACCEPT = "application/atom+xml,application/xml;q=0.9,*/*;q=0.8"
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) "
    "Gecko/20100101 Firefox/122.0",
]
RETRY_DELAYS = [5, 15, 30]
API_SLEEP_MIN = 4.0
API_SLEEP_MAX = 8.0
HTML_SLEEP_MIN = 1.1
HTML_SLEEP_MAX = 3.3

NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
}


class DownloadError(RuntimeError):
    pass


class SlowDownloadError(DownloadError):
    pass


@dataclass(frozen=True)
class DownloadResult:
    pages: int
    total_results: int
    papers: int
    html_dir: Path
    api_dir: Path


@dataclass(frozen=True)
class Entry:
    arxiv_id: str
    authors: list[str]


def build_api_url(
    last_name: str,
    first_name: str,
    start: int,
    *,
    max_results: int = PAGE_SIZE,
) -> str:
    query = f'au:"{first_name} {last_name}"'
    params = {
        "search_query": query,
        "start": str(start),
        "max_results": str(max_results),
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    return f"{API_URL}{urlencode(params)}"


def build_headers(referer: str | None = None, accept: str | None = None) -> dict[str, str]:
    headers = dict(BASE_HEADERS)
    headers["User-Agent"] = random.choice(USER_AGENTS)
    if accept:
        headers["Accept"] = accept
    if referer:
        headers["Referer"] = referer
    return headers


def fetch_text(
    session: requests.Session,
    url: str,
    *,
    referer: str | None = None,
    retry_delays: list[int] | None = None,
    accept: str | None = None,
) -> str:
    last_error: Exception | None = None
    delays = retry_delays if retry_delays is not None else RETRY_DELAYS
    max_attempts = len(delays) + 1
    for attempt in range(1, max_attempts + 1):
        try:
            headers = build_headers(referer, accept)
            response = session.get(url, headers=headers, timeout=30)
            if 500 <= response.status_code <= 599:
                raise requests.HTTPError(
                    f"{response.status_code} Server Error: {response.reason} for url: {url}",
                    response=response,
                )
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            delay = delays[attempt - 1]
            time.sleep(delay)
    assert last_error is not None
    raise DownloadError(str(last_error)) from last_error


def parse_api_feed(xml_text: str) -> tuple[int, list[Entry]]:
    root = ET.fromstring(xml_text)
    total = 0
    total_elem = root.find("opensearch:totalResults", NAMESPACES)
    if total_elem is not None and total_elem.text:
        try:
            total = int(total_elem.text.strip())
        except ValueError:
            total = 0

    entries: list[Entry] = []
    for entry in root.findall("atom:entry", NAMESPACES):
        id_elem = entry.find("atom:id", NAMESPACES)
        if id_elem is None or not id_elem.text:
            continue
        value = id_elem.text.strip()
        if "/abs/" in value:
            value = value.split("/abs/")[-1]

        authors: list[str] = []
        for author in entry.findall("atom:author", NAMESPACES):
            name_elem = author.find("atom:name", NAMESPACES)
            if name_elem is not None and name_elem.text:
                authors.append(name_elem.text.strip())

        entries.append(Entry(arxiv_id=value, authors=authors))

    return total, entries


def ensure_author_dir(root: Path, last_name: str, first_name: str) -> Path:
    author_dir = root / "AUTHORS" / f"{last_name}-{first_name}"
    author_dir.mkdir(parents=True, exist_ok=True)
    return author_dir


def ensure_output_dirs(author_dir: Path) -> tuple[Path, Path]:
    api_dir = author_dir / "API"
    html_dir = author_dir / "HTML"
    api_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)
    return api_dir, html_dir


def safe_write_text(path: Path, text: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def rand_sleep(min_seconds: float, max_seconds: float) -> None:
    if max_seconds < min_seconds:
        min_seconds, max_seconds = max_seconds, min_seconds
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)


def sanitize_id(arxiv_id: str) -> str:
    return arxiv_id.replace("/", "_")


def load_api_page(path: Path) -> tuple[int, list[Entry]] | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        xml_text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return parse_api_feed(xml_text)
    except ET.ParseError:
        return None


def normalize_tokens(value: str) -> list[str]:
    cleaned = re.sub(r"[^a-zA-Z]+", " ", value).strip().lower()
    if not cleaned:
        return []
    return [token for token in cleaned.split() if token]


def author_matches(first_name: str, last_name: str, author_names: Iterable[str]) -> bool:
    first_tokens = normalize_tokens(first_name)
    last_tokens = normalize_tokens(last_name)
    if not first_tokens or not last_tokens:
        return False

    first = first_tokens[0]
    first_initial = first[0]

    for name in author_names:
        tokens = normalize_tokens(name)
        if len(tokens) < len(last_tokens) + 1:
            continue

        if tokens[-len(last_tokens) :] == last_tokens:
            given = tokens[0]
            if given == first or given.startswith(first) or given == first_initial:
                return True

        if tokens[: len(last_tokens)] == last_tokens and len(tokens) > len(last_tokens):
            given = tokens[len(last_tokens)]
            if given == first or given.startswith(first) or given == first_initial:
                return True

    return False


def count_html_files(html_dir: Path) -> int:
    if not html_dir.exists():
        return 0
    return sum(1 for path in html_dir.glob("*.html") if path.is_file())


def fetch_total_results(
    session: requests.Session,
    last_name: str,
    first_name: str,
    *,
    retry_delays: list[int] | None = None,
) -> int:
    api_url = build_api_url(last_name, first_name, start=0, max_results=1)
    xml_text = fetch_text(
        session,
        api_url,
        referer=None,
        retry_delays=retry_delays,
        accept=API_ACCEPT,
    )
    try:
        total, _entries = parse_api_feed(xml_text)
    except ET.ParseError as exc:
        raise DownloadError(f"Invalid API XML from {api_url}") from exc
    return total


def download_author(
    last_name: str,
    first_name: str,
    *,
    root: Path,
    api_sleep_min: float = API_SLEEP_MIN,
    api_sleep_max: float = API_SLEEP_MAX,
    html_sleep_min: float = HTML_SLEEP_MIN,
    html_sleep_max: float = HTML_SLEEP_MAX,
    max_request_seconds: float | None = None,
    max_total_seconds: float | None = None,
    retry_delays: list[int] | None = None,
    show_progress: bool = True,
    progress_leave: bool = True,
) -> DownloadResult:
    author_dir = ensure_author_dir(root, last_name, first_name)
    api_dir, html_dir = ensure_output_dirs(author_dir)

    start_time = time.monotonic()
    start = 0
    api_pages = 0
    total_results = 0
    seen_ids: set[str] = set()
    matched_count = 0
    bar: tqdm | None = None

    with requests.Session() as session:
        while True:
            api_pages += 1
            api_path = api_dir / f"page-{api_pages}.xml"
            api_fetched = False
            parsed = load_api_page(api_path)

            if parsed is None:
                api_url = build_api_url(last_name, first_name, start=start)
                request_start = time.monotonic()
                api_xml = fetch_text(
                    session,
                    api_url,
                    referer=None,
                    retry_delays=retry_delays,
                    accept=API_ACCEPT,
                )
                request_elapsed = time.monotonic() - request_start
                if max_request_seconds and request_elapsed > max_request_seconds:
                    raise SlowDownloadError(
                        f"API request exceeded {max_request_seconds:.1f}s: {api_url}"
                    )
                safe_write_text(api_path, api_xml)
                api_fetched = True
                try:
                    parsed = parse_api_feed(api_xml)
                except ET.ParseError as exc:
                    raise DownloadError(f"Invalid API XML from {api_url}") from exc
            else:
                api_url = build_api_url(last_name, first_name, start=start)

            page_total, entries = parsed
            if api_pages == 1 and page_total:
                total_results = page_total
                if show_progress:
                    bar = tqdm(
                        total=total_results,
                        desc=f"{last_name}, {first_name}",
                        unit="paper",
                        leave=progress_leave,
                    )

            if not entries:
                break

            for entry in entries:
                if entry.arxiv_id in seen_ids:
                    continue
                seen_ids.add(entry.arxiv_id)
                if bar is not None:
                    bar.update(1)

                if not author_matches(first_name, last_name, entry.authors):
                    continue

                matched_count += 1
                safe_id = sanitize_id(entry.arxiv_id)
                html_path = html_dir / f"{safe_id}.html"
                if not html_path.exists() or html_path.stat().st_size == 0:
                    abs_url = f"https://arxiv.org/abs/{entry.arxiv_id}"
                    req_start = time.monotonic()
                    html = fetch_text(
                        session,
                        abs_url,
                        referer=api_url,
                        retry_delays=retry_delays,
                        accept=None,
                    )
                    req_elapsed = time.monotonic() - req_start
                    if max_request_seconds and req_elapsed > max_request_seconds:
                        raise SlowDownloadError(
                            f"Abs request exceeded {max_request_seconds:.1f}s: {abs_url}"
                        )

                    safe_write_text(html_path, html)
                    rand_sleep(html_sleep_min, html_sleep_max)

            start += PAGE_SIZE
            if total_results and start >= total_results:
                break

            if max_total_seconds:
                elapsed = time.monotonic() - start_time
                if elapsed > max_total_seconds:
                    raise SlowDownloadError(
                        f"Author exceeded {max_total_seconds:.1f}s total"
                    )

            if api_fetched:
                rand_sleep(api_sleep_min, api_sleep_max)

    if bar is not None:
        bar.close()

    return DownloadResult(
        pages=api_pages,
        total_results=total_results,
        papers=matched_count,
        html_dir=html_dir,
        api_dir=api_dir,
    )


def ensure_csv_header(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["last-name", "first-name"])


def append_author_to_csv(path: Path, last_name: str, first_name: str) -> None:
    ensure_csv_header(path)
    existing = set()
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        for row_index, row in enumerate(reader):
            if not row:
                continue
            if row_index == 0 and "last" in row[0].lower():
                continue
            if len(row) < 2:
                continue
            key = (row[0].strip().lower(), row[1].strip().lower())
            existing.add(key)

    key = (last_name.lower(), first_name.lower())
    if key in existing:
        return

    with path.open("a", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([last_name, first_name])


def iter_authors_csv(path: Path) -> list[tuple[str, str]]:
    authors: list[tuple[str, str]] = []
    if not path.exists():
        return authors
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        for row_index, row in enumerate(reader):
            if not row:
                continue
            if row_index == 0 and "last" in row[0].lower():
                continue
            if len(row) < 2:
                continue
            last = row[0].strip().strip(",")
            first = row[1].strip().strip(",")
            if not last or not first:
                continue
            authors.append((last, first))
    return authors


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download arXiv author pages.")
    parser.add_argument("last_name", nargs="?", help="Author last name")
    parser.add_argument("first_name", nargs="?", help="Author first name")
    parser.add_argument(
        "--csv-path",
        default=str(CSV_PATH),
        help=argparse.SUPPRESS,
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    root = Path(__file__).resolve().parent
    csv_path = Path(args.csv_path)

    if not args.last_name and not args.first_name:
        authors = iter_authors_csv(csv_path)
        if not authors:
            print(f"No authors found in {csv_path}", file=sys.stderr)
            raise SystemExit(2)

        with requests.Session() as session:
            for last_name, first_name in tqdm(authors, desc="Authors", unit="author"):
                try:
                    total_results = fetch_total_results(
                        session,
                        last_name,
                        first_name,
                        retry_delays=RETRY_DELAYS,
                    )
                except DownloadError as exc:
                    print(
                        f"Check failed for {last_name}, {first_name}: {exc}",
                        file=sys.stderr,
                    )
                    continue
                author_dir = root / "AUTHORS" / f"{last_name}-{first_name}"
                html_count = count_html_files(author_dir / "HTML")

                if total_results == 0 and html_count == 0:
                    continue

                if html_count >= total_results and total_results > 0:
                    continue

                try:
                    download_author(
                        last_name,
                        first_name,
                        root=root,
                        retry_delays=RETRY_DELAYS,
                        show_progress=True,
                        progress_leave=False,
                    )
                except DownloadError as exc:
                    print(
                        f"Download failed for {last_name}, {first_name}: {exc}",
                        file=sys.stderr,
                    )
                    continue
        return

    if not args.last_name or not args.first_name:
        print("Usage: python main.py <last-name> <first-name>", file=sys.stderr)
        raise SystemExit(2)

    last_name = args.last_name.strip().strip(",")
    first_name = args.first_name.strip().strip(",")
    append_author_to_csv(csv_path, last_name, first_name)

    try:
        result = download_author(
            last_name,
            first_name,
            root=root,
            retry_delays=RETRY_DELAYS,
            show_progress=True,
            progress_leave=True,
        )
    except DownloadError as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        raise SystemExit(1)

    if result.total_results:
        print(
            f"Saved {result.pages} API page(s), {result.papers} papers "
            f"({result.total_results} results) for {last_name}, {first_name} "
            f"to {result.api_dir} and {result.html_dir}"
        )
    else:
        print(
            f"Saved {result.pages} API page(s), {result.papers} papers "
            f"for {last_name}, {first_name} to {result.api_dir} and {result.html_dir}"
        )


if __name__ == "__main__":
    main()
