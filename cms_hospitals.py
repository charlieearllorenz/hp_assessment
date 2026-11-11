#!/usr/bin/env python3
"""Incrementally sync CMS Provider Data "Hospitals" datasets.

What it does, per run:
  1. Reads the CMS provider-data metastore (DCAT catalog of every dataset).
  2. Keeps datasets whose ``theme`` contains "Hospitals".
  3. Compares each dataset's ``modified`` / ``released`` dates and CSV URLs with
     the metadata saved by the previous run and skips anything unchanged.
  4. Downloads the changed CSVs in parallel (thread pool), rewrites the header
     row so every column name is snake_case, and writes the result atomically.
  5. Saves per-dataset metadata (state file) and appends a line to a run
     history file, so the job can be scheduled daily.

Only the Python standard library plus ``requests`` is used, so it runs on any
Windows / Linux / macOS machine.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import shutil
import sys
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_METASTORE_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)
DEFAULT_THEME = "Hospitals"
STATE_VERSION = 1

log = logging.getLogger("cms_hospitals")


# --------------------------------------------------------------------------- #
# Column-name handling
# --------------------------------------------------------------------------- #
_APOSTROPHES = re.compile(r"[’‘ʼ'`´]")  # ’ ‘ ʼ ' ` ´
_NON_ALNUM = re.compile(r"[^0-9a-z]+")


def to_snake_case(name: str) -> str:
    """'Patients’ rating of the facility linear mean score'
    -> 'patients_rating_of_the_facility_linear_mean_score'.

    Apostrophes are dropped (so "Patients’" -> "patients", not "patients_"),
    accents are folded to ASCII, "&" becomes "and", and every other run of
    non-alphanumeric characters collapses to a single underscore.
    """
    s = _APOSTROPHES.sub("", name)
    s = s.replace("&", " and ")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = _NON_ALNUM.sub("_", s.lower()).strip("_")
    return s


def snake_case_headers(headers: Iterable[str]) -> list[str]:
    """snake_case a header row, keeping names unique and non-empty."""
    out: list[str] = []
    seen: dict[str, int] = {}
    for idx, raw in enumerate(headers, start=1):
        name = to_snake_case(raw) or f"column_{idx}"
        if name in seen:  # collision, e.g. "Rate %" and "Rate #"
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        seen[name] = seen.get(name, 1)
        out.append(name)
    return out


def _read_header_record(fin) -> bytes:
    """Read the first CSV record (a quoted header may span lines)."""
    raw = fin.readline()
    if not raw:
        raise ValueError("file is empty")
    while raw.count(b'"') % 2 == 1:
        more = fin.readline()
        if not more:
            break
        raw += more
    return raw


def rewrite_csv_header(src: Path, dst: Path) -> list[str]:
    """Copy ``src`` to ``dst`` with a snake_case header; return new header.

    Only the header is decoded/parsed. The body is copied byte-for-byte, so
    large files stream through with constant memory and no re-encoding risk.
    Output is UTF-8 without BOM.
    """
    with open(src, "rb") as fin:
        raw = _read_header_record(fin)
        text = raw.decode("utf-8-sig")
        terminator = text[len(text.rstrip("\r\n")):] or "\n"
        header = next(csv.reader(io.StringIO(text, newline="")))
        new_header = snake_case_headers(header)

        buf = io.StringIO(newline="")
        csv.writer(buf, lineterminator=terminator).writerow(new_header)
        with open(dst, "wb") as fout:
            fout.write(buf.getvalue().encode("utf-8"))
            shutil.copyfileobj(fin, fout, 1024 * 1024)
    return new_header


# --------------------------------------------------------------------------- #
# Metastore helpers
# --------------------------------------------------------------------------- #
def has_theme(item: dict[str, Any], theme: str) -> bool:
    raw = item.get("theme")
    if raw is None:
        return False
    values = [raw] if isinstance(raw, str) else list(raw)
    return any(str(v).strip().lower() == theme.strip().lower() for v in values)


def csv_urls(item: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for dist in item.get("distribution") or []:
        url = dist.get("downloadURL")
        media = str(dist.get("mediaType", "")).lower()
        if url and (media == "text/csv" or url.lower().split("?")[0].endswith(".csv")):
            if url not in urls:
                urls.append(url)
    return urls


def fingerprint(item: dict[str, Any], urls: list[str]) -> dict[str, Any]:
    """What identifies a version of the dataset as published by CMS."""
    return {
        "modified": item.get("modified"),
        "released": item.get("released"),
        "urls": sorted(urls),
    }


def slugify(text: str, limit: int = 80) -> str:
    return _NON_ALNUM.sub("_", text.lower()).strip("_")[:limit] or "dataset"


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
_thread_local = threading.local()


def get_session(retries: int) -> requests.Session:
    """One Session per worker thread (requests.Session is not guaranteed
    thread-safe), with automatic retry/backoff on transient HTTP errors."""
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        retry = Retry(
            total=retries,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=16)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        # Keep the default requests User-Agent: data.cms.gov's edge returns 403
        # for custom UA strings (verified), but accepts python-requests.
        _thread_local.session = session
    return session


def fetch_metastore(url: str, retries: int, timeout: int) -> list[dict[str, Any]]:
    resp = get_session(retries).get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"unexpected metastore payload: {type(data).__name__}")
    return data


def download_file(url: str, dest: Path, retries: int, timeout: int) -> int:
    """Stream ``url`` to ``dest``; retry whole transfer on mid-stream errors."""
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with get_session(retries).get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                size = 0
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                        size += len(chunk)
            return size
        except requests.RequestException as exc:
            last_exc = exc
            wait = min(2 ** attempt, 30)
            log.warning("download failed (%s), attempt %d/%d, retry in %ds",
                        exc, attempt + 1, retries + 1, wait)
            if attempt < retries:
                time.sleep(wait)
    assert last_exc is not None
    raise last_exc


# --------------------------------------------------------------------------- #
# State / run history
# --------------------------------------------------------------------------- #
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_state(path: Path) -> dict[str, Any]:
    if path.exists():
        with open(path, encoding="utf-8") as f:
            state = json.load(f)
        if state.get("version") == STATE_VERSION:
            return state
        log.warning("state file version mismatch; starting fresh")
    return {"version": STATE_VERSION, "last_run_started_at": None,
            "last_successful_run_at": None, "datasets": {}}


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


# --------------------------------------------------------------------------- #
# Per-dataset worker
# --------------------------------------------------------------------------- #
def process_dataset(item: dict[str, Any], urls: list[str], out_dir: Path,
                    tmp_dir: Path, retries: int, timeout: int) -> dict[str, Any]:
    """Download + clean every CSV of one dataset. Runs in a worker thread."""
    identifier = item["identifier"]
    base = f"{slugify(item.get('title', ''))}__{identifier}"
    files = []
    for n, url in enumerate(urls, start=1):
        name = f"{base}.csv" if len(urls) == 1 else f"{base}_{n}.csv"
        raw = tmp_dir / f"{identifier}_{n}.download"
        cleaned = tmp_dir / f"{identifier}_{n}.clean"
        try:
            size = download_file(url, raw, retries, timeout)
            header = rewrite_csv_header(raw, cleaned)
            os.replace(cleaned, out_dir / name)  # atomic publish
        finally:
            for p in (raw, cleaned):
                p.unlink(missing_ok=True)
        files.append({"name": name, "url": url, "bytes_downloaded": size,
                      "columns": header})
        log.info("saved %s (%d bytes, %d columns)", name, size, len(header))
    return {"files": files}


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run(args: argparse.Namespace) -> int:
    started = utc_now()
    out_dir = Path(args.output_dir)
    state_path = Path(args.state_file)
    history_path = state_path.with_name("run_history.jsonl")
    tmp_dir = out_dir / ".tmp"
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    items = fetch_metastore(args.metastore_url, args.retries, args.timeout)
    selected = [i for i in items if i.get("identifier") and has_theme(i, args.theme)]
    log.info("metastore: %d datasets, %d with theme %r",
             len(items), len(selected), args.theme)

    state = load_state(state_path)
    state["last_run_started_at"] = started

    todo: list[tuple[dict[str, Any], list[str], dict[str, Any]]] = []
    skipped = 0
    for item in selected:
        urls = csv_urls(item)
        if not urls:
            log.warning("no CSV distribution for %s (%s); skipping",
                        item["identifier"], item.get("title"))
            continue
        fp = fingerprint(item, urls)
        prev = state["datasets"].get(item["identifier"])
        files_present = prev is not None and all(
            (out_dir / f["name"]).exists() for f in prev.get("files", []))
        if not args.force and prev and prev.get("fingerprint") == fp and files_present:
            skipped += 1
            log.debug("unchanged: %s", item["identifier"])
            continue
        todo.append((item, urls, fp))

    log.info("%d to download, %d unchanged", len(todo), skipped)

    downloaded: list[str] = []
    failed: dict[str, str] = {}
    if todo:
        with ThreadPoolExecutor(max_workers=args.workers,
                                thread_name_prefix="dl") as pool:
            futures = {
                pool.submit(process_dataset, item, urls, out_dir, tmp_dir,
                            args.retries, args.timeout): (item, fp)
                for item, urls, fp in todo
            }
            for fut in as_completed(futures):
                item, fp = futures[fut]
                ident = item["identifier"]
                try:
                    result = fut.result()
                except Exception as exc:  # noqa: BLE001 - keep other datasets going
                    log.error("FAILED %s (%s): %s", ident, item.get("title"), exc)
                    failed[ident] = str(exc)
                    continue
                # Remove files left over from a previous version with other names.
                old = state["datasets"].get(ident, {}).get("files", [])
                new_names = {f["name"] for f in result["files"]}
                for f in old:
                    if f["name"] not in new_names:
                        (out_dir / f["name"]).unlink(missing_ok=True)
                state["datasets"][ident] = {
                    "title": item.get("title"),
                    "fingerprint": fp,
                    "downloaded_at": utc_now(),
                    **result,
                }
                downloaded.append(ident)
                # Persist after every dataset so a crash never loses progress.
                atomic_write_json(state_path, state)

    finished = utc_now()
    if not failed:
        state["last_successful_run_at"] = finished
    atomic_write_json(state_path, state)

    history = {
        "run_started_at": started, "run_finished_at": finished,
        "metastore_items": len(items), "theme_datasets": len(selected),
        "downloaded": len(downloaded), "unchanged": skipped,
        "failed": len(failed), "failed_ids": sorted(failed),
    }
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with open(history_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(history) + "\n")

    shutil.rmtree(tmp_dir, ignore_errors=True)
    log.info("done: %d downloaded, %d unchanged, %d failed",
             len(downloaded), skipped, len(failed))
    return 1 if failed else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--output-dir", default="data/hospitals",
                   help="where cleaned CSVs are written (default: %(default)s)")
    p.add_argument("--state-file", default="state/cms_hospitals_state.json",
                   help="run metadata; run_history.jsonl is written beside it")
    p.add_argument("--theme", default=DEFAULT_THEME)
    p.add_argument("--metastore-url", default=DEFAULT_METASTORE_URL)
    p.add_argument("--workers", type=int, default=8,
                   help="parallel downloads (default: %(default)s)")
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--timeout", type=int, default=120, help="seconds per request")
    p.add_argument("--force", action="store_true",
                   help="ignore saved metadata and re-download everything")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
    )
    try:
        return run(args)
    except Exception:  # noqa: BLE001
        log.exception("run aborted")
        return 2


if __name__ == "__main__":
    sys.exit(main())
