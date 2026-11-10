import os
import re
from datetime import datetime

import requests
from dotenv import load_dotenv

## Shared code for the Datawrapper migration scripts: everything in Formatering/formatering_generelt.txt
## (palette, data source, number formats, footer, language, live data) plus the API connection.
## Used by the create_<chart type>.py scripts (create_line.py, create_column.py, create_column_grouped.py, create_bar.py,
## create_column_stacked.py, create_bar_grouped.py, create_bar_stacked.py).

API_BASE = "https://api.datawrapper.de/v3"
TEAM_NAME = "Team analyse"

ARIA_DESCRIPTION = "Graf utarbeidet av Telemark fylkeskommune"

## Palette: the 80 % shades from the county's color profile, OO3us colors first,
## then additional colors ordered for contrast
PALETTE = [
    "#337580",  # Vann
    "#c54564",  # Nype
    "#b7ad61",  # Korn
    "#676b9a",  # Blåveis
    "#c99251",  # Siv
    "#4caa81",  # Gress
    "#33afce",  # Himmel
    "#7b5881",  # Plomme
    "#a18965",  # Strand
    "#439ba3",  # Fjord
    "#ad8776",  # Bark
    "#599168",  # Gran
    "#498989",  # Hav
    "#8e8d81",  # Berg
    "#959595",  # Stein
]

## Grouped column and grouped bar charts only: #337580 (Vann) and #b7ad61 (Korn) first,
## then the rest of the palette in its order
GROUPED_MAIN_COLORS = ["#337580", "#b7ad61"]
GROUPED_PALETTE = GROUPED_MAIN_COLORS + [c for c in PALETTE if c not in GROUPED_MAIN_COLORS]

## Stacked column charts only: five main colors first, then the rest of the palette in its order
STACKED_MAIN_COLORS = ["#337580", "#b7ad61", "#c54564", "#c99251", "#676b9a"]
STACKED_PALETTE = STACKED_MAIN_COLORS + [c for c in PALETTE if c not in STACKED_MAIN_COLORS]

## Series/rows that represent the country or the county (styled differently per chart type)
COUNTRY_NAMES = ["Hele landet", "Norge", "Landet"]
COUNTY_NAMES = ["Telemark", "Fylket"]

## Data sources recognised in query scripts: URL domain -> name shown as "Kilde"
## (SSB also gets a link to the table, see find_data_source)
SOURCE_DOMAINS = {
    "ssb.no": "SSB",
    "nav.no": "NAV",
    "app-simapi-prod.azurewebsites.net": "IMDi",
    "imdi.no": "IMDi",
    "miljodirektoratet.no": "Miljødirektoratet",
    "norskeutslipp.no": "Miljødirektoratet",
    "nve.no": "NVE",
    "elhub.no": "Elhub",
    "fhi.no": "Helsedirektoratet",  # FHI data is credited to Helsedirektoratet (link: sources.csv)
    "udir.no": "Utdanningsdirektoratet",
    "barnehagefakta.no": "Utdanningsdirektoratet",
    "helsedirektoratet.no": "Helsedirektoratet",
    "nibio.no": "NIBIO",
    "brreg.no": "Brønnøysundregistrene",
    "vegvesen.no": "Statens vegvesen",
    "norges-bank.no": "Norges Bank",
    "entsoe.eu": "ENTSO-E",
    "vann-nett.no": "Vann-nett",
}


## Load the Datawrapper token from token.env (same location as GITHUB_TOKEN) and return a logged-in session
def get_session():
    pythonpath = os.environ.get("PYTHONPATH")
    if not pythonpath:
        raise ValueError("PYTHONPATH environment variable is not set.")

    env_file_path = os.path.join(pythonpath, "token.env")
    if not os.path.exists(env_file_path):
        raise ValueError(f"token.env file not found in: {env_file_path}")

    load_dotenv(env_file_path)
    token = os.getenv("DATAWRAPPER_CREATE_TOKEN")
    if not token:
        raise ValueError("DATAWRAPPER_CREATE_TOKEN not found in the loaded .env file.")

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    session.hooks["response"].append(_log_api_call)
    return session


## Log every Datawrapper API call (one line per call) to Python/Log/datawrapper_api_calls.log
## (LOG_FOLDER if set, otherwise PYTHONPATH/Log; git-ignored). Count calls with count_api_calls().
def _api_log_path():
    log_folder = os.environ.get("LOG_FOLDER") or os.path.join(os.environ["PYTHONPATH"], "Log")
    return os.path.join(log_folder, "datawrapper_api_calls.log")


def _log_api_call(response, *args, **kwargs):
    if not response.request.url.startswith(API_BASE):
        return
    path = _api_log_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    timestamp = datetime.now().isoformat(timespec="seconds")
    url = response.request.url[len(API_BASE):]
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{timestamp}\t{response.request.method}\t{url}\t{response.status_code}\n")


## Number of logged API calls, in total or since a date/time (ISO format, e.g. "2026-11-09")
def count_api_calls(since=None):
    path = _api_log_path()
    if not os.path.exists(path):
        return 0
    with open(path, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip() and (since is None or line[:19] >= since))


## Find the team id for TEAM_NAME
def get_team_id(session):
    response = session.get(f"{API_BASE}/teams")
    response.raise_for_status()
    teams = response.json().get("list", [])
    team_id = next((t["id"] for t in teams if t["name"] == TEAM_NAME), None)
    if team_id is None:
        raise ValueError(f"Team '{TEAM_NAME}' not found.")
    return team_id


## Find the data source from the first recognised data URL in the query script.
## SSB: link to the first table in the script (https://www.ssb.no/statbank/table/XXXXX), or no link if the
## table number is only known when the script runs (e.g. "tables/{table_id}"). Other sources: name only.
## If the script has no known URL itself, query files it names (e.g. "Mobbing 7. trinn treårig.txt", used by
## the FHI scripts) are searched for, up to 3 folders up from the script, and their URLs are used instead.
def source_from_text(text):
    for url in re.findall(r"https?://[^\s\"']+", text):
        domain = re.match(r"https?://([^/]+)", url).group(1)
        name = next((n for d, n in SOURCE_DOMAINS.items() if domain == d or domain.endswith("." + d)), None)
        if name is None:
            continue
        if name != "SSB":
            return name, ""
        table = re.search(r"/tables?/(\d{5})", url)
        if table:
            return name, f"https://www.ssb.no/statbank/table/{table.group(1)}"
        if "/table" in url:  # SSB table endpoint, but the number is set at runtime
            print(f"Note: SSB table number in {url} is set at runtime, using the general SSB link from sources.csv")
            return name, ""
    return None


def find_data_source(query_script):
    """query_script is relative to PYTHONPATH, or None if the data has no query script."""
    if query_script is None:
        print("Warning: no query script given, source left empty")
        return "", ""
    script_path = os.path.join(os.environ["PYTHONPATH"], query_script)
    with open(script_path, encoding="utf-8") as f:
        code = f.read()
    source = source_from_text(code)
    if source is None:
        # Search the script's whole topic folder (Queries/<topic>), e.g. FHI/scripts/... -> FHI/queries/...
        queries_root = os.path.join(os.environ["PYTHONPATH"], "Queries")
        relative = os.path.relpath(os.path.abspath(script_path), queries_root)
        search_root = os.path.join(queries_root, relative.split(os.sep)[0])
        for query_name in re.findall(r"[\"']([^\"']+\.txt)[\"']", code):
            query_name = os.path.basename(query_name)
            for root, _, files in os.walk(search_root):
                if query_name in files:
                    with open(os.path.join(root, query_name), encoding="utf-8") as f:
                        source = source_from_text(f.read())
                    break
            if source is not None:
                break
    if source is None:
        print("Warning: no known data source found in the query script")
        return "", ""
    name, url = source
    if not url:
        url = SOURCES_CSV_LINKS().get(name, "")
        if not url:
            print(f"Warning: no link for source '{name}' in sources.csv, source link left empty")
    return name, url


## Source names and general links for sources without a direct table link in the query script (e.g. FHI, whose
## scripts use API URLs): sources.csv next to this file, one "name,url" per line (no header)
SOURCES_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sources.csv")


def SOURCES_CSV_LINKS():
    links = {}
    with open(SOURCES_CSV, encoding="utf-8-sig") as f:
        for line in f:
            if "," in line:
                name, url = line.strip().split(",", 1)
                links[name.strip()] = url.strip()
    return links


## Number formats: shares ("Andel" in the title) get %, the value axis and value labels show whole numbers
## (one decimal if all values are below 10), tooltips one decimal. Never two decimals.
## Exception: if all values are whole numbers (e.g. counts, "512" or "512.0"), everything shows whole numbers.
def is_share(title):
    return "andel" in title.lower()


def number_formats(values, title):
    percent = "%" if is_share(title) else ""
    values = values.dropna().astype(float)
    if (values % 1 == 0).all():
        return "0,0" + percent, "0,0" + percent
    # "0,0": whole numbers with thousands separator (nb-NO: space, e.g. "4 198 139")
    value_format = ("0.0" if values.abs().max() < 10 else "0,0") + percent
    tooltip_format = "0,0.0" + percent
    return value_format, tooltip_format


## Metadata from the general rules: live data, source, alt text, footer
def general_metadata(csv_url, source_name, source_url):
    return {
        "data": {
            "upload-method": "external-data",
            "external-data": csv_url,
            "use-datawrapper-cdn": True,
        },
        "describe": {
            "source-name": source_name,
            "source-url": source_url,
            "aria-description": ARIA_DESCRIPTION,
        },
        "publish": {
            "blocks": {
                "get-the-data": True,
                "embed": True,
                "download-image": True,
                "download-pdf": False,
                "download-svg": False,
                "logo": {"enabled": False},
            },
            "autoDarkMode": True,
        },
    }


## Create an empty chart in the team, or use an existing one if chart_id is given
def create_or_get_chart(session, chart_id, title, chart_type):
    if chart_id is not None:
        print(f"Updating chart {chart_id}")
        return chart_id
    response = session.post(
        f"{API_BASE}/charts",
        json={"title": title, "type": chart_type, "organizationId": get_team_id(session)},
    )
    response.raise_for_status()
    chart_id = response.json()["id"]
    print(f"Created chart {chart_id}")
    return chart_id


## Apply title, type, language, theme and metadata, then pull the external data in now
## (Datawrapper refreshes it itself after the chart has been published once)
def apply_and_refresh(session, chart_id, title, chart_type, metadata):
    response = session.patch(
        f"{API_BASE}/charts/{chart_id}",
        json={
            "title": title,
            "type": chart_type,
            "language": "nb-NO",
            "theme": "datawrapper",
            "metadata": metadata,
        },
    )
    response.raise_for_status()
    print("Applied metadata")

    response = session.post(f"{API_BASE}/charts/{chart_id}/data/refresh")
    response.raise_for_status()
    print("Refreshed data from the CSV")

    ## (Not published - open the editor to review, publish from there or via POST /charts/{id}/publish)
    print(f"\nEdit:    https://app.datawrapper.de/chart/{chart_id}/visualize")
    print(f"Preview: https://datawrapper.dwcdn.net/{chart_id}/ (only after publishing)")
