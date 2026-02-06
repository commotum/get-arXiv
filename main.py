from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://arxiv.org/search/?"
PAGE_SIZE = 50
HEADERS = {"User-Agent": "get-arXiv/0.1 (+https://arxiv.org)"}
TOTAL_RE = re.compile(r"of\s+([0-9,]+)\s+results", re.IGNORECASE)


def build_search_url(last_name: str, first_name: str, start: int) -> str:
    query = f"{last_name}, {first_name}"
    params = {
        "searchtype": "all",
        "query": query,
        "abstracts": "show",
        "size": str(PAGE_SIZE),
        "order": "-announced_date_first",
        "start": str(start),
    }
    return f"{BASE_URL}{urlencode(params)}"


def fetch_html(session: requests.Session, url: str, *, max_attempts: int = 4) -> str:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
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
            time.sleep(2 * attempt)
    assert last_error is not None
    raise last_error


def parse_total_results(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    for text in soup.find_all(string=re.compile(r"Showing", re.IGNORECASE)):
        match = TOTAL_RE.search(text)
        if match:
            return int(match.group(1).replace(",", ""))
    match = TOTAL_RE.search(soup.get_text(" ", strip=True))
    if match:
        return int(match.group(1).replace(",", ""))
    return 0


def parse_next_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    next_link = soup.select_one("a.pagination-next")
    if not next_link:
        return None
    href = next_link.get("href")
    if not href:
        return None
    if "is-invisible" in (next_link.get("class") or []):
        return None
    return urljoin("https://arxiv.org", href)


def ensure_directories(root: Path, last_name: str, first_name: str) -> Path:
    author_dir = root / "AUTHORS" / f"{last_name}-{first_name}"
    html_dir = author_dir / "HTML"
    html_dir.mkdir(parents=True, exist_ok=True)
    return html_dir


def save_html(html_dir: Path, page_number: int, html: str) -> None:
    output_path = html_dir / f"page-{page_number}.html"
    output_path.write_text(html, encoding="utf-8")


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
    html_dir = ensure_directories(root, last_name, first_name)

    with requests.Session() as session:
        url = build_search_url(last_name, first_name, start=0)
        page_number = 1
        total_results = 0

        while url:
            html = fetch_html(session, url)
            save_html(html_dir, page_number, html)

            if page_number == 1:
                total_results = parse_total_results(html)

            next_url = parse_next_url(html)
            if not next_url or next_url == url:
                break
            url = next_url
            page_number += 1
            time.sleep(1)

    if total_results:
        print(
            f"Saved {page_number} page(s) "
            f"({total_results} results) for {last_name}, {first_name} to {html_dir}"
        )
    else:
        print(
            f"Saved {page_number} page(s) for {last_name}, {first_name} to {html_dir}"
        )


if __name__ == "__main__":
    main()
