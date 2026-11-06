import os
import re
import math
import pandas as pd
import requests
from dotenv import load_dotenv

## Pilot: create (or update) a live line chart in Datawrapper, formatted according to
## Formatering/formatering_linjediagram.txt (Part 1 custom rules override Part 2 reference from OO3us)

API_BASE = "https://api.datawrapper.de/v3"
TEAM_NAME = "Team analyse"

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Sandkasse/Datawrapper/Pilot/Dummy%20data/linjediagram_uten_landet_fylket.csv"
CHART_TITLE = "Andel av en variabel"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "loP3F"

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source
QUERY_SCRIPT = "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_sysselsatte_etter_bakgrunn.py"

## Text (Part 2: describe)
ARIA_DESCRIPTION = "Graf utarbeidet av Telemark fylkeskommune"

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
    "fhi.no": "FHI",
    "udir.no": "Udir",
    "barnehagefakta.no": "Udir",
    "brreg.no": "Brønnøysundregistrene",
    "vegvesen.no": "Statens vegvesen",
    "norges-bank.no": "Norges Bank",
    "entsoe.eu": "ENTSO-E",
    "vann-nett.no": "Vann-nett",
}

## Colors (Part 1), assigned to the ordinary series in column order: the 80 % shades from the county's
## color profile, OO3us colors first, then additional colors ordered for contrast
SERIES_COLORS = [
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

## Reference lines (Part 2: "Reference lines"): grey, dashed, no symbols
REFERENCE_COLOR = "#7b7b7a"
REFERENCE_LINES = {
    "Hele landet": "style1",  # dotted
    "Norge": "style1",
    "Landet": "style1",
    "Telemark": "style3",  # long dashes
    "Fylket": "style3",
}

## Symbols (Part 2), and Part 1: no symbols if 5 series or more (reference lines not counted)
SYMBOL_SIZE = 4.5
MAX_SERIES_FOR_SYMBOLS = 4

## Part 1: legend instead of series labels if more than 6 series in total (reference lines included)
MAX_SERIES_FOR_SERIES_LABELS = 6
## Part 1: reference lines go in the legend if there are 3 or more ordinary series
MIN_ORDINARY_SERIES_FOR_REFERENCE_LEGEND = 3


## Load the Datawrapper token from token.env (same location as GITHUB_TOKEN)
def get_datawrapper_token():
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
    return token


token = get_datawrapper_token()
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {token}"})


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
            print(f"Warning: SSB table number in {url} is set at runtime, source link left empty")
            return name, ""
    return None


def find_data_source(script_path):
    with open(script_path, encoding="utf-8") as f:
        code = f.read()
    source = source_from_text(code)
    if source is None:
        search_root = os.path.dirname(os.path.abspath(script_path))
        for _ in range(3):
            search_root = os.path.dirname(search_root)
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
    return source


source_name, source_url = find_data_source(os.path.join(os.environ["PYTHONPATH"], QUERY_SCRIPT))
print(f"Source: {source_name or '(none)'} {source_url}")


## Find the team id for TEAM_NAME
response = session.get(f"{API_BASE}/teams")
response.raise_for_status()
teams = response.json().get("list", [])
team_id = next((t["id"] for t in teams if t["name"] == TEAM_NAME), None)
if team_id is None:
    raise ValueError(f"Team '{TEAM_NAME}' not found.")


## Read the CSV: first column is the x-axis, the rest are series
## (names are stripped, as Datawrapper ignores leading/trailing spaces in headers)
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
series = list(df.columns[1:])
reference_series = [s for s in series if s in REFERENCE_LINES]
ordinary_series = [s for s in series if s not in REFERENCE_LINES]
print(f"Series: {ordinary_series}, reference lines: {reference_series}")
if len(ordinary_series) > len(SERIES_COLORS):
    print(f"Warning: more than {len(SERIES_COLORS)} ordinary series, colors are reused")


## Part 1: shares ("Andel" in the title) get percentage format, and the y-axis and value labels show
## whole numbers, except one decimal if all values are small (below 10) (value labels follow the y-axis format).
## Tooltips always show one decimal. Never two decimals.
is_share = "andel" in CHART_TITLE.lower()
percent = "%" if is_share else ""
y_values = df[series].stack()
y_grid_format = ("0.0" if y_values.abs().max() < 10 else "0") + percent
tooltip_format = "0.0" + percent
print(f"y-axis/value label format: {y_grid_format}, tooltip format: {tooltip_format}")


## Rounding step for the y-axis: a "nice" number (1, 2 or 5 x 10^n) close to a fifth of the data range,
## so the step scales with the data (e.g. 5 for values 44-72, 500 for values 1200-4000, 0.1 for values 0.2-0.8)
def nice_step(data_range):
    raw = data_range / 5
    magnitude = 10 ** math.floor(math.log10(raw))
    return min((m * magnitude for m in (1, 2, 5, 10)), key=lambda c: abs(math.log10(c / raw)))


## Part 1: y-axis range
## - min: below the lowest value (padding of 10 % of the data range), rounded down to a multiple of the step,
##        never below 0 unless the data has negative values
## - max: above the highest value (padding of 10 % of the data range), rounded up to a multiple of the step,
##        never above 100 for shares
## - minimum span: if the data range is less than 20 % of the largest value, it is widened around its middle
##   to that size first, so small changes don't look dramatic
def y_axis_range(values, is_share):
    low, high = values.min(), values.max()
    min_span = 0.2 * max(abs(low), abs(high)) or 1  # 1 if all values are 0
    if high - low < min_span:
        middle = (low + high) / 2
        low, high = middle - min_span / 2, middle + min_span / 2
    data_range = high - low
    step = nice_step(data_range)
    y_min = math.floor((low - data_range * 0.1) / step) * step
    y_max = math.ceil((high + data_range * 0.1) / step) * step
    if values.min() >= 0:
        y_min = max(0, y_min)
    if is_share:
        y_max = min(100, y_max)
    # round away floating point noise (e.g. 0.30000000000000004)
    return round(y_min, 10), round(y_max, 10)


y_min, y_max = y_axis_range(y_values, is_share)
print(f"Data range: {y_values.min()} - {y_values.max()}, y-axis: {y_min} - {y_max}")


## Colors: ordinary series in column order, reference lines grey
color_map = {name: SERIES_COLORS[i % len(SERIES_COLORS)] for i, name in enumerate(ordinary_series)}
color_map.update({name: REFERENCE_COLOR for name in reference_series})


## Lines: dash for reference lines, symbols only on ordinary series (and only if fewer than 5 of them)
use_symbols = len(ordinary_series) <= MAX_SERIES_FOR_SYMBOLS
use_legend = len(series) > MAX_SERIES_FOR_SERIES_LABELS
use_reference_legend = len(ordinary_series) >= MIN_ORDINARY_SERIES_FOR_REFERENCE_LEGEND
lines = {}
for name in series:
    is_reference = name in REFERENCE_LINES
    in_legend = use_legend or (is_reference and use_reference_legend)
    lines[name] = {
        "symbols": {
            "enabled": use_symbols and not is_reference,
            "on": "every",
            "size": SYMBOL_SIZE,
        },
        "valueLabels": {"enabled": True},
        "colorKey": in_legend,  # shown in the legend
        "directLabel": not in_legend,  # series label at the line end
    }
    if is_reference:
        lines[name]["dash"] = REFERENCE_LINES[name]
print(
    f"{len(ordinary_series)} ordinary series, symbols: {use_symbols}, "
    f"legend for all: {use_legend}, legend for reference lines: {use_reference_legend and bool(reference_series)}"
)


## 1) Create an empty line chart in the team (unless updating an existing one)
if CHART_ID is None:
    response = session.post(
        f"{API_BASE}/charts",
        json={"title": CHART_TITLE, "type": "d3-lines", "organizationId": team_id},
    )
    response.raise_for_status()
    chart_id = response.json()["id"]
    print(f"Created chart {chart_id}")
else:
    chart_id = CHART_ID
    print(f"Updating chart {chart_id}")


## 2) Link the CSV as external data (live) and apply the formatting
metadata = {
    "data": {
        "upload-method": "external-data",
        "external-data": CSV_URL,
        "use-datawrapper-cdn": True,
    },
    "describe": {
        "source-name": source_name,
        "source-url": source_url,
        "aria-description": ARIA_DESCRIPTION,
    },
    "visualize": {
        "color-category": {"map": color_map},
        "variate-colors": False,
        "label-colors": True,
        "value-label-colors": True,
        "lines": lines,
        "x-grid": "off",
        "y-grid": "on",
        "x-grid-format": "YYYY",
        "y-grid-format": y_grid_format,
        "tooltip-number-format": tooltip_format,
        "y-grid-labels": "auto",
        "y-grid-label-align": "left",
        "y-grid-subdivide": True,
        "scale-y": "linear",
        "custom-range-y": [y_min, y_max],
        "interpolation": "linear",
        "show-tooltips": True,
        "connector-lines": True,
        "label-margin": 0,
        "plotHeightMode": "ratio",
        "plotHeightRatio": 0.6,
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

response = session.patch(
    f"{API_BASE}/charts/{chart_id}",
    json={
        "title": CHART_TITLE,
        "type": "d3-lines",
        "language": "nb-NO",
        "theme": "datawrapper",
        "metadata": metadata,
    },
)
response.raise_for_status()
print("Applied metadata")


## 3) Pull the external data into the chart now (Datawrapper refreshes it itself after publishing)
response = session.post(f"{API_BASE}/charts/{chart_id}/data/refresh")
response.raise_for_status()
print("Refreshed data from CSV_URL")


## (Not published - open the editor to review, publish from there or via POST /charts/{id}/publish)
print(f"\nEdit:    https://app.datawrapper.de/chart/{chart_id}/visualize")
print(f"Preview: https://datawrapper.dwcdn.net/{chart_id}/ (only after publishing)")
