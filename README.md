# CMS "Hospitals" dataset sync

A daily-schedulable Python job that downloads every dataset in the CMS Provider Data
metastore whose theme is **Hospitals**, converts the CSV column names to `snake_case`,
and on later runs only re-downloads datasets CMS has modified.

Source catalog: <https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items>

## Run it

```bash
pip install -r requirements.txt        # only dependency: requests
python cms_hospitals.py                # writes ./data/hospitals and ./state
python cms_hospitals.py --help
```

Works on Windows and Linux/macOS (Python 3.9+). Nothing is cloud- or Databricks-specific.

| Option | Default | Purpose |
|---|---|---|
| `--output-dir` | `data/hospitals` | cleaned CSVs |
| `--state-file` | `state/cms_hospitals_state.json` | per-dataset metadata (`run_history.jsonl` is written beside it) |
| `--workers` | 8 | parallel downloads |
| `--force` | off | ignore saved metadata, re-download everything |
| `--theme` | `Hospitals` | theme to select |
| `--retries` / `--timeout` | 3 / 120 s | transient-failure handling |

Exit codes: `0` success, `1` some datasets failed (the rest are saved and the failures are retried next run), `2` the run aborted (e.g. metastore unreachable).

### Scheduling (daily)

* **Linux/macOS cron** (06:00 daily): `0 6 * * * cd /opt/cms_job && python3 cms_hospitals.py >> job.log 2>&1`
* **Windows Task Scheduler**: `schtasks /create /tn "CMS Hospitals Sync" /sc daily /st 06:00 /tr "cmd /c cd /d C:\cms_job && python cms_hospitals.py >> job.log 2>&1"`

## How it works

1. **Select**: fetch the metastore, keep items whose `theme` contains "Hospitals" (case-insensitive; handles list or string), and take their CSV `distribution` URLs.
2. **Decide what changed**: each dataset has a fingerprint of `modified`, `released` and its CSV download URLs. If it matches the fingerprint stored from the previous run *and* the output file still exists, the dataset is skipped. Otherwise it is downloaded. Comparing per dataset (rather than against a single "last run" timestamp) means a dataset that failed yesterday is automatically retried today, and nothing is missed if a run is skipped or delayed.
3. **Download in parallel**: a `ThreadPoolExecutor` (downloads are I/O-bound), one `requests.Session` per thread, streaming to disk in 1 MB chunks, with retry/backoff for 429/5xx and for mid-stream drops.
4. **Clean headers**: only the header row is parsed; the body is copied byte-for-byte, so large files use constant memory and values are never altered. Output is UTF-8 without BOM.
5. **Track metadata**: `state/cms_hospitals_state.json` holds, per dataset, the fingerprint, download time, file name/URL/size and the resulting column list. It is rewritten atomically after *each* dataset finishes, so a crash mid-run loses nothing. `state/run_history.jsonl` gets one line per run (counts downloaded / unchanged / failed).

### Column-name rules

`"Patients’ rating of the facility linear mean score"` → `patients_rating_of_the_facility_linear_mean_score`

Lower-cased; apostrophes (straight or curly) removed rather than turned into underscores; accents folded to ASCII; `&` → `and`; any other run of punctuation/whitespace → a single `_`; leading/trailing `_` trimmed. Duplicate results get `_2`, `_3`…; an empty result becomes `column_<n>`.

## Tests

```bash
python -m unittest discover -s tests -v
```

Unit tests for the naming rules and header rewrite, plus an end-to-end test against a local mock of the metastore that checks: only Hospitals-themed datasets are fetched; a second run downloads nothing; changing one dataset's `modified` date re-downloads only that one; a deleted output file is restored; a failed dataset is retried on the next run.

## Sample output

`sample_output/` comes from a **live run against data.cms.gov on 2026-10-29**:

* Run 1: 237 datasets in the metastore, **75 with theme "Hospitals"**, 75 downloaded, 0 failed (~295 MB, ~5 s with 8 workers).
* Run 2 (same day): 0 downloaded, **75 unchanged** (see `state/run_history.jsonl`).
* All 75 output files were checked: every header is valid snake_case with no blank or duplicate names.
* `sample_output/data/hospitals/` contains only the first 25 data rows of three files to keep it small; the full output of the run is in the separate full-data zip. `state/` is the real, unabridged metadata.

Note: `data.cms.gov` returns HTTP 403 for custom `User-Agent` strings but accepts the default python-requests one, so the script deliberately does not set its own.

## Assumptions and possible improvements

* "Modified" is determined from the catalog's `modified`/`released` fields and URLs. If CMS ever republished a file without changing either, it would not be detected; `--force` (or a weekly forced run) covers that. Sending `If-None-Match`/`If-Modified-Since` and storing ETags would be a cheap extra guard.
* Datasets removed from the Hospitals theme keep their last downloaded file (no deletion).
* Two overlapping runs are not guarded against; schedule it once a day, or add a lock file if runs can exceed 24 h.
* Beyond a few dozen CSVs per day, raise `--workers` modestly; CMS rate-limits aggressive clients (handled by `Retry-After`/backoff).
