from __future__ import annotations

import random
import shutil
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode

import requests

API_URL = "https://export.arxiv.org/api/query?"
PAGE_SIZE = 50
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
SLEEP_MIN = 4.0
SLEEP_MAX = 8.0
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


def build_api_url(last_name: str, first_name: str, start: int) -> str:
    query = f'au:"{first_name} {last_name}"'
    params = {
        "search_query": query,
        "start": str(start),
        "max_results": str(PAGE_SIZE),
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
    raise last_error


def parse_api_feed(xml_text: str) -> tuple[int, list[str]]:
    root = ET.fromstring(xml_text)
    total = 0
    total_elem = root.find("opensearch:totalResults", NAMESPACES)
    if total_elem is not None and total_elem.text:
        try:
            total = int(total_elem.text.strip())
        except ValueError:
            total = 0

    ids: list[str] = []
    for entry in root.findall("atom:entry", NAMESPACES):
        id_elem = entry.find("atom:id", NAMESPACES)
        if id_elem is None or not id_elem.text:
            continue
        value = id_elem.text.strip()
        if "/abs/" in value:
            value = value.split("/abs/")[-1]
        ids.append(value)

    return total, ids


def ensure_author_dir(root: Path, last_name: str, first_name: str) -> Path:
    author_dir = root / "AUTHORS" / f"{last_name}-{first_name}"
    author_dir.mkdir(parents=True, exist_ok=True)
    return author_dir


def save_text(output_dir: Path, name: str, text: str) -> None:
    output_path = output_dir / name
    output_path.write_text(text, encoding="utf-8")


def rand_sleep(min_seconds: float, max_seconds: float) -> None:
    if max_seconds < min_seconds:
        min_seconds, max_seconds = max_seconds, min_seconds
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)


def sanitize_id(arxiv_id: str) -> str:
    return arxiv_id.replace("/", "_")


def download_author(
    last_name: str,
    first_name: str,
    *,
    root: Path,
    sleep_min: float = SLEEP_MIN,
    sleep_max: float = SLEEP_MAX,
    max_request_seconds: float | None = None,
    max_total_seconds: float | None = None,
    retry_delays: list[int] | None = None,
) -> DownloadResult:
    author_dir = ensure_author_dir(root, last_name, first_name)
    api_dir = author_dir / "API"
    html_dir = author_dir / "HTML"
    api_tmp = author_dir / "API.tmp"
    html_tmp = author_dir / "HTML.tmp"

    for tmp in (api_tmp, html_tmp):
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)

    start_time = time.monotonic()
    start = 0
    api_pages = 0
    total_results = 0
    paper_count = 0
    seen_ids: set[str] = set()

    try:
        with requests.Session() as session:
            while True:
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

                api_pages += 1
                save_text(api_tmp, f"page-{api_pages}.xml", api_xml)

                page_total, ids = parse_api_feed(api_xml)
                if api_pages == 1:
                    total_results = page_total

                if not ids:
                    break

                for arxiv_id in ids:
                    if arxiv_id in seen_ids:
                        continue
                    seen_ids.add(arxiv_id)

                    abs_url = f"https://arxiv.org/abs/{arxiv_id}"
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

                    safe_id = sanitize_id(arxiv_id)
                    save_text(html_tmp, f"{safe_id}.html", html)
                    paper_count += 1

                    rand_sleep(sleep_min, sleep_max)

                start += PAGE_SIZE
                if total_results and start >= total_results:
                    break

                if max_total_seconds:
                    elapsed = time.monotonic() - start_time
                    if elapsed > max_total_seconds:
                        raise SlowDownloadError(
                            f"Author exceeded {max_total_seconds:.1f}s total"
                        )

                rand_sleep(sleep_min, sleep_max)
    except Exception:
        for tmp in (api_tmp, html_tmp):
            shutil.rmtree(tmp, ignore_errors=True)
        if not any(author_dir.iterdir()):
            author_dir.rmdir()
        raise

    for target, tmp in ((api_dir, api_tmp), (html_dir, html_tmp)):
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        tmp.rename(target)

    return DownloadResult(
        pages=api_pages,
        total_results=total_results,
        papers=paper_count,
        html_dir=html_dir,
        api_dir=api_dir,
    )


def parse_args(argv: list[str]) -> tuple[str, str]:
    if len(argv) != 3:
        print("Usage: python main.py <last-name> <first-name>", file=sys.stderr)
        raise SystemExit(2)
    last_name = argv[1].strip().strip(",")
    first_name = argv[2].strip().strip(",")
    if not last_name or not first_name:
        print("Both last-name and first-name are required.", file=sys.stderr)
        raise SystemExit(2)
    return last_name, first_name


def main() -> None:
    last_name, first_name = parse_args(sys.argv)
    root = Path(__file__).resolve().parent
    try:
        result = download_author(last_name, first_name, root=root)
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
