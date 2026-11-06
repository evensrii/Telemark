import os
import math
import pandas as pd
import requests
from dotenv import load_dotenv

## Pilot: create (or update) a live line chart in Datawrapper from a CSV in this repo
## API reference: https://developer.datawrapper.de/reference/postcharts

API_BASE = "https://api.datawrapper.de/v3"
TEAM_NAME = "Team analyse"

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/09_Innvandrere%20og%20inkludering/Utdanningsniv%C3%A5%20Telemark/fullf%C3%B8rt_vgo_innv_befolk.csv"
CHART_TITLE = "Fullført videregående opplæring, innvandrere og hele befolkningen"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "HMOHj"

## From formatting_rules.txt (line graphs)
BASE_COLOR = "#00818c"  # Datawrapper default blue, used for 3 series or less
# Anchor colors for the diverging palette (4 series or more), interpolated across the series
DIVERGING_ANCHORS = ["#c71e1d", "#a0a0a0", "#00818c"]
LINE_WIDTH = "style1"  # 3 px (style0 = 2 px default, style1 = 3 px, style2 = 4 px, style3 = 1 px)
SYMBOL_SIZE = 5.5
MAX_POINTS_FOR_X_GRID = 7


## Interpolate n colors evenly across the anchor colors
def diverging_palette(n, anchors=DIVERGING_ANCHORS):
    rgb = [tuple(int(a[i : i + 2], 16) for i in (1, 3, 5)) for a in anchors]
    colors = []
    for k in range(n):
        pos = k / (n - 1) * (len(rgb) - 1)
        i = min(int(pos), len(rgb) - 2)
        t = pos - i
        c = [round(rgb[i][j] + (rgb[i + 1][j] - rgb[i][j]) * t) for j in range(3)]
        colors.append("#{:02x}{:02x}{:02x}".format(*c))
    return colors


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


## Read the CSV to get the series names (first column is the x-axis) and the value range
df = pd.read_csv(CSV_URL)
series = list(df.columns[1:])
print(f"Series: {series}")


## Y-axis range:
## - min: below the lowest value (padding of half the data range), rounded down to a multiple of 10, never below 0
## - max: above the highest value (padding of 10 % of the data range), rounded up to a multiple of 10, never above 100
## Datawrapper still extends the axis if future data falls outside this range.
y_values = df[series].stack()
y_range = y_values.max() - y_values.min()
y_min = max(0, math.floor((y_values.min() - y_range * 0.5) / 10) * 10)
y_max = min(100, math.ceil((y_values.max() + y_range * 0.1) / 10) * 10)
print(f"Data range: {y_values.min()} - {y_values.max()}, y-axis: {y_min} - {y_max}")


## Colors and symbols:
## - 3 series or less: Datawrapper base color (its own shades per line) and circle symbols
## - 4 series or more: diverging palette, no symbols
few_series = len(series) <= 3
if few_series:
    color_map = {}  # empty map clears any earlier per-series colors
else:
    color_map = dict(zip(series, diverging_palette(len(series))))

lines = {
    name: {
        "width": LINE_WIDTH,
        "symbols": {
            "enabled": few_series,
            "shape": "circle",
            "size": SYMBOL_SIZE,
            "style": "fill",
            "on": "every",
        },
    }
    for name in series
}

## Vertical grid lines if there are few x-axis points
x_grid = "on" if len(df) <= MAX_POINTS_FOR_X_GRID else "off"
print(f"{len(series)} series, {len(df)} x-axis points, x-grid: {x_grid}")


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


## 2) Link the CSV as external data (live) and apply formatting_rules.txt
metadata = {
    "data": {
        "upload-method": "external-data",
        "external-data": CSV_URL,
        "use-datawrapper-cdn": True,
    },
    "visualize": {
        "base-color": BASE_COLOR,
        "color-category": {"map": color_map},
        "lines": lines,
        "x-grid": x_grid,
        "labeling": "right",
        "y-grid-format": "0 %",
        "custom-range-y": [y_min, y_max],
    },
    "publish": {
        "blocks": {
            "get-the-data": True,
            "embed": True,
        },
    },
}

response = session.patch(
    f"{API_BASE}/charts/{chart_id}",
    json={"title": CHART_TITLE, "type": "d3-lines", "metadata": metadata},
)
response.raise_for_status()
print("Applied metadata")


## 3) Pull the external data into the chart now (Datawrapper refreshes it itself afterwards)
response = session.post(f"{API_BASE}/charts/{chart_id}/data/refresh")
response.raise_for_status()
print("Refreshed data from CSV_URL")


## (Not published - open the editor to review, publish from there or via POST /charts/{id}/publish)
print(f"\nEdit:    https://app.datawrapper.de/chart/{chart_id}/visualize")
print(f"Preview: https://datawrapper.dwcdn.net/{chart_id}/ (only after publishing)")
