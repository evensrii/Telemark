import os
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

## Text (Part 2: describe)
SOURCE_NAME = "SSB"
SOURCE_URL = "https://www.ssb.no/statbank/"
ARIA_DESCRIPTION = "Graf utarbeidet av Telemark fylkeskommune"

## Colors (Part 2: color-category in OO3us), assigned to the ordinary series in column order
SERIES_COLORS = ["#337580", "#c54564", "#b7ad61"]

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


## Part 1: y-axis range
## - min: below the lowest value (padding of 10 % of the data range), rounded down to a multiple of 5, never below 0
## - max: above the highest value (padding of 10 % of the data range), rounded up to a multiple of 5, never above 100
y_values = df[series].stack()
y_range = y_values.max() - y_values.min()
y_min = max(0, math.floor((y_values.min() - y_range * 0.1) / 5) * 5)
y_max = min(100, math.ceil((y_values.max() + y_range * 0.1) / 5) * 5)
print(f"Data range: {y_values.min()} - {y_values.max()}, y-axis: {y_min} - {y_max}")


## Part 1: percentage format on the y-axis if the title contains "Andel", and always whole numbers
## (value labels follow the y-axis format)
y_grid_format = "0%" if "andel" in CHART_TITLE.lower() else "0"
print(f"y-axis format: {y_grid_format}")


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
        "source-name": SOURCE_NAME,
        "source-url": SOURCE_URL,
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
