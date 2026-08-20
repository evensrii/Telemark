# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository purpose

This is the data and content repository behind "Kunnskap om Telemark", a statistics/knowledge website for Telemark county (Norway). It is primarily a **Python ETL pipeline** that fetches public statistics (SSB, FHI, Udir, NVE, Elhub, etc.), normalizes them into CSVs, and publishes them to this GitHub repo, plus supporting web content (interactive maps, dashboards) that consume that data. There is no application server and no test suite — correctness is judged by "does the data update, and does the diff make sense."

## Repository layout

- `Python/Queries/` — one script per dataset, organized by topic folder matching `Data/` (e.g. `Queries/08_Folkehelse_og_levekår/...`). This is where almost all work happens.
- `Python/Helper_scripts/` — shared library code: `utility_functions.py` (`fetch_data`, temp-file cleanup) and `github_functions.py` (`handle_output_data`, `compare_to_github`, GitHub REST API upload/download).
- `Python/Automatisering/Task scheduler/master_script.py` — orchestrator. Contains a hard-coded `SCRIPTS` list of `(script_path, task_name)` tuples; runs each via `conda run -n analyse python <script>`, logs status per task, then triggers `email_when_run_completed.py`. This is the source of truth for "which scripts are live" — a new query script must be added here to run in production (via Windows Task Scheduler).
- `Data/` — the published output CSVs, organized by the same topic taxonomy as `Queries/`. Scripts write here (via `handle_output_data`) by pushing straight to GitHub, not via local commits.
- `Kart/`, `Egne applikasjoner/`, `Nettsider/` — static/web front-ends (Leaflet/ArcGIS maps, embeddable apps, iframe pages) that visualize the data in `Data/`. `Egne applikasjoner/Pilot web-app kart` is a standalone Create React App (own `package.json`; `npm start` / `npm run build` / `npm run deploy` via `gh-pages`).
- `Annet/` — environments, credentials, misc scripts, archived rules (`Annet/Gamle regler`), logs, backups. Not part of the pipeline itself.
- `.github/workflows/deploy.yml` — on push to `main`, injects the `MAPBOX_ACCESS_TOKEN` secret into `Kart/sirkulare_telemark/config.js` and commits it back. The only CI in this repo.

## Environment setup

Conda environment named `analyse` (defined in `Python/environment.yaml`):

```
conda env create --file Python/environment.yaml
conda activate analyse
conda update --file Python/environment.yaml --prune   # to sync after changes
```

The environment file also declares three env vars that must be set for scripts to run (adjust paths to your machine):
- `PYTHONPATH` → the `Python/` folder (so `from Helper_scripts...` imports resolve)
- `TEMP_FOLDER` → `Python/Temp`
- `LOG_FOLDER` → `Python/Log`

A `token.env` file (in `Python/`, alongside `PYTHONPATH`) must contain `GITHUB_TOKEN=...` — `github_functions.py` loads it via `python-dotenv` and fails hard at import time if missing. This is a personal-access token with write access to `evensrii/Telemark`, used for all GitHub read/write operations (scripts push data directly via the GitHub Contents API rather than local git commits).

Scripts are developed interactively: they're run cell-by-cell in a Jupyter/VS Code interactive window (not as standalone `python script.py` invocations during authoring), with a "manual refinement" section left in the middle of the script for inspecting `df.head()` etc. Keep that workflow in mind when editing query scripts — don't wrap the whole body in a function/`if __name__`, since that breaks interactive cell execution. Scripts are only invoked as standalone processes by `master_script.py` in production.

## Running things

- **Run a single dataset script**: `python "Python/Queries/<topic>/<script>.py"` from an activated `analyse` env, with the three env vars above set. Prints status ("New data detected..." / "No new data detected...") to stdout.
- **Run the full pipeline**: `python "Python/Automatisering/Task scheduler/master_script.py"` — iterates the entire `SCRIPTS` list, writes one log file per task into `Python/Automatisering/Task scheduler/logs/`, and a master log `00_master_run.log`, then sends a summary email.
- **React map app**: `cd "Egne applikasjoner/Pilot web-app kart" && npm start` (dev) / `npm run build` / `npm run deploy` (publishes to GitHub Pages under `kart-bedrifter`).
- No linter, formatter, or test runner is configured for the Python code.

## Standard query-script pattern

Every dataset script in `Python/Queries/` follows the same shape:

1. Import `fetch_data` from `Helper_scripts.utility_functions` and `handle_output_data` from `Helper_scripts.github_functions`.
2. Fetch data inside a `try/except`, using `fetch_data(url, payload, error_messages, query_name, response_type=...)`. `payload=None` → GET, a dict → POST.
   - SSB's current API (`https://data.ssb.no/api/pxwebapi/v2/tables/XXXXX/data?...`) supports GET requests with query params in the URL (`payload=None`); the response is JSON-stat2, parsed via `pyjstat`. Some older scripts still use the legacy `v0` POST API with a JSON payload — both work, but prefer GET for new scripts.
   - For non-SSB/non-JSON sources, use `response_type="csv"` with `delimiter`/`encoding` as needed.
3. A manual, top-level (not inside a function) section to clean/reshape the resulting DataFrame with pandas — meant to be run and inspected interactively.
4. Persist and sync with GitHub via `handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=True, value_columns=[...])`, then write a `new_data_status_<task>.log` file under `LOG_FOLDER`. `github_folder` should point into `Data/<topic>/...` mirroring the site's topic taxonomy.

### The GitHub compare/upload flow (`github_functions.py`)

`handle_output_data` → `compare_to_github` implements a strict, ordered diff so downstream reporting is meaningful:

1. **Header changes** (case-insensitive column name diffs, or year-in-header changes like `Andel 2023` → `Andel 2024`) — reported and uploaded immediately, no further checks.
2. **Row count changes** — reported (old vs. new count, sample added/removed rows) and uploaded immediately.
3. **Value changes** — only reached if headers and row count are identical; diffs are computed as strings (to avoid float precision/type noise) and, for large datasets, only over the **last 200 rows** (for performance — uploads still happen if any earlier-row differences exist, they just won't be previewed).

Key/identifier columns (`Kommune`, `Kommunenummer`, `Label`, date columns, NACE codes, etc.) are auto-excluded from value comparison via `identify_key_columns`; pass `value_columns` explicitly to restrict comparison to specific columns (recommended when a dataset has many non-numeric metadata columns).

When adding or modifying a query script, match this structure and reuse `fetch_data`/`handle_output_data` rather than writing bespoke HTTP/GitHub logic — the diff/report/upload behavior is meant to be centralized in `Helper_scripts`.

## Naming conventions

- `task_name` (used in log file names and the master script's `SCRIPTS` list) must not contain commas or periods usable as path separators; it's sanitized to build a log filename.
- Norwegian topic folder names under `Data/`/`Queries/` are numbered (`01_Befolkning`, `08_Folkehelse_og_levekår`, ...) — keep new scripts under the matching numbered topic, don't invent new top-level categories without checking `master_script.py`'s existing grouping comments first.
