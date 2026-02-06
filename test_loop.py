from __future__ import annotations

import csv
import os
import sys
import time
from pathlib import Path

from main import DownloadError, download_author

DEFAULT_CSV = Path(__file__).resolve().parent / "authors.csv"
DEFAULT_MAX_AUTHORS = 0
DEFAULT_MAX_REQUEST_SECONDS = 45.0
DEFAULT_MAX_TOTAL_SECONDS = 0.0
DEFAULT_STOP_ON_FAIL = True


def parse_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def parse_env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() not in {"0", "false", "no"}


def parse_retry_delays(value: str | None) -> list[int]:
    if not value:
        return []
    delays: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            delays.append(int(part))
        except ValueError:
            continue
    return delays


def iter_authors(csv_path: Path) -> list[tuple[str, str]]:
    authors: list[tuple[str, str]] = []
    with csv_path.open(newline="") as handle:
        reader = csv.reader(handle)
        for row_index, row in enumerate(reader):
            if not row:
                continue
            if row_index == 0 and "last" in row[0].lower():
                continue
            if len(row) < 2:
                continue
            last_name = row[0].strip().strip(",")
            first_name = row[1].strip().strip(",")
            if not last_name or not first_name:
                continue
            authors.append((last_name, first_name))
    return authors


def main() -> None:
    csv_path = Path(os.getenv("CSV_PATH", str(DEFAULT_CSV)))
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        raise SystemExit(2)

    max_authors = parse_env_int("MAX_AUTHORS", DEFAULT_MAX_AUTHORS)
    max_request_seconds = parse_env_float(
        "MAX_REQUEST_SECONDS", DEFAULT_MAX_REQUEST_SECONDS
    )
    max_total_seconds = parse_env_float("MAX_TOTAL_SECONDS", DEFAULT_MAX_TOTAL_SECONDS)
    stop_on_fail = parse_env_bool("STOP_ON_FAIL", DEFAULT_STOP_ON_FAIL)
    retry_delays = parse_retry_delays(os.getenv("RETRY_DELAYS"))
    retry_arg = retry_delays if retry_delays else None

    authors = iter_authors(csv_path)
    if max_authors > 0:
        authors = authors[:max_authors]

    root = Path(__file__).resolve().parent

    print(
        "Test loop starting: "
        f"authors={len(authors)}, "
        f"max_request_seconds={max_request_seconds:.1f}, "
        f"max_total_seconds={max_total_seconds:.1f}, "
        "api_sleep=3.1-6.9s, html_sleep=3.1-6.9s"
    )

    for index, (last_name, first_name) in enumerate(authors, start=1):
        label = f"{last_name}, {first_name}"
        print(f"[{index}/{len(authors)}] {label}: starting")
        start = time.monotonic()
        try:
            result = download_author(
                last_name,
                first_name,
                root=root,
                max_request_seconds=(None if max_request_seconds <= 0 else max_request_seconds),
                max_total_seconds=(None if max_total_seconds <= 0 else max_total_seconds),
                retry_delays=retry_arg,
            )
        except DownloadError as exc:
            elapsed = time.monotonic() - start
            print(
                f"[{index}/{len(authors)}] {label}: failed after {elapsed:.1f}s: {exc}",
                file=sys.stderr,
            )
            if stop_on_fail:
                break
            continue

        elapsed = time.monotonic() - start
        print(
            f"[{index}/{len(authors)}] {label}: "
            f"{result.pages} API page(s), {result.papers} papers in {elapsed:.1f}s"
        )

    print("Test loop complete.")


if __name__ == "__main__":
    main()
