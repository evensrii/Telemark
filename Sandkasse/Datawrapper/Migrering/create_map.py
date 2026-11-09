import re

import pandas as pd
import requests

from datawrapper_generelt import (
    API_BASE,
    get_session,
    find_data_source,
    is_share,
    general_metadata,
    create_or_get_chart,
    apply_and_refresh,
)

## Migrering: create (or update) a live choropleth map of the municipalities in Telemark in Datawrapper,
## formatted according to Formatering/formatering_map.txt (maps don't use formatering_generelt.txt)

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Sandkasse/Datawrapper/Migrering/Dummy%20data/andel_flyktninger.csv"
CHART_TITLE = "Andel flyktninger i Telemark (test av formatering kart)"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "pFsxq"

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = None

# Number of color steps: 5 (standard) or 3, given per map (e.g. in the Excel file for batch creation)
NUM_STEPS = 5

## Colors (formatering_map.txt Part 1), light to dark
STEP_COLORS = {
    5: ["#f6efd5", "#f0e1bd", "#e0c58e", "#d4b67c", "#c7a869"],
    3: ["#f0e1bd", "#e0c58e", "#c7a869"],
}

## Basemap: the municipalities in Telemark (TopoJSON in the repo), uploaded to each map as the asset
## "<chart id>.map.json"
BASEMAP_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Kart/Kartfiler/Datawrapper_kommuner_Telemark.json"

# Column in the CSV with the map labels (always "Label+Value").
# The region names (matched against the basemap) are the first column, whatever it's called
# ("Region", "Kommune", "Område" ...).
LABEL_COLUMN = "Label+Value"


session = get_session()

source_name, source_url = find_data_source(QUERY_SCRIPT)
print(f"Source: {source_name or '(none)'} {source_url}")


## Read the CSV: the first column is the region names, the value column is the first column after it
## that is not "Label" or "Label+Value"
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
key_column = df.columns[0]
value_column = next(c for c in df.columns[1:] if c not in ("Label", LABEL_COLUMN))
print(f"Regions: {len(df)} rows from '{key_column}', colors from '{value_column}', labels from '{LABEL_COLUMN}'")


## Number formats: % for shares ("Andel" in the value column name or in the title)
## Legend: Datawrapper's default format (up to two decimals, e.g. "1,53–1,67")
## Tooltip: the value with up to one decimal and a decimal comma, via FORMAT (%REGION_VALUE% shows "1.6").
## The tooltip refers to the value column by its variable name: lowercase, spaces to "_", special characters
## removed (Datawrapper's columnNameToVariable), e.g. "Forsørgerevne (2026)" -> "forsrgerevne_2026"
share = is_share(value_column) or is_share(CHART_TITLE)


def column_name_to_variable(name):
    name = re.sub(r"\s+", "_", str(name).lower())
    name = re.sub(r"[^\w-]+", "", name, flags=re.ASCII)
    name = re.sub(r"__+", "_", name.replace("-", "_")).strip("_")
    name = re.sub(r"^(\d)", r"_\1", name)
    return re.sub(r"^(and|or|in|true|false)$", r"\1_", name)


value_variable = column_name_to_variable(value_column)
legend_format = "0,0.[00]" + ("%" if share else "")
tooltip_body = f'{{{{ FORMAT({value_variable}, "0.[0]") }}}}' + (" %" if share else "")
print(f"Legend format: {legend_format}, tooltip: {tooltip_body}")


## Color steps: equal distance between the colors (positions 0 ... 1)
colors = STEP_COLORS[NUM_STEPS]
color_stops = [
    {"color": color, "position": round(i / (NUM_STEPS - 1), 4)} for i, color in enumerate(colors)
]


## 1) Create an empty map in the team (unless updating an existing one)
chart_id = create_or_get_chart(session, CHART_ID, CHART_TITLE, "d3-maps-choropleth")


## 2) Upload the basemap to the chart (the map is empty without it)
basemap = requests.get(BASEMAP_URL)
basemap.raise_for_status()
response = session.put(
    f"{API_BASE}/charts/{chart_id}/assets/{chart_id}.map.json",
    data=basemap.content,
    headers={"Content-Type": "application/json"},
)
response.raise_for_status()
print("Uploaded basemap")


## 3) Link the CSV as external data (live) and apply the formatting
metadata = general_metadata(CSV_URL, source_name, source_url)
metadata["data"]["transpose"] = False
metadata["axes"] = {
    "keys": key_column,
    "values": value_column,
    "labels": LABEL_COLUMN,
}
metadata["visualize"] = {
    # Basemap
    "basemap": "custom_upload",
    "basemapFilename": "Kommuner_Telemark-S_topo.json",
    "basemapRegions": "Kommuner-S",
    "basemapProjection": "geoAzimuthalEqualArea",
    "basemapShowExtraOptions": True,
    "map-key-attr": "name",
    "map-type-set": True,
    "chart-type-set": True,
    "mapView": "full",
    "mapViewCropPadding": 10,
    "map-padding": 0,
    "map-align": "center",
    "max-map-height": 650,
    "hide-empty-regions": False,
    "hide-region-borders": False,
    "zoomable": True,
    "zoom-button-pos": "br",
    "miniMap": {
        "enabled": True,
        "type": "region",
        "position": "bottom-left",
        "size": 120,
        "offsetX": 1,
        "offsetY": 1,
        "innerBorders": True,
    },
    # Colors: steps, equal width (editor: Type "Steps", Steps "Linear (equi-distant)")
    "colorscale": {
        "mode": "discrete",
        "stops": "equidistant",
        "interpolation": "equidistant",
        "stopCount": NUM_STEPS,
        "colors": color_stops,
        "palette": 1,
        "rangeMin": "",
        "rangeMax": "",
        "rangeCenter": "",
        "map": {},
    },
    # Labels from the "Label+Value" column
    "labels": {"enabled": True, "type": "column", "max": 34},
    "min-label-zoom": 1,
    "avoid-label-overlap": False,
    # Legend: top right inside the map, vertical, ranges, no title
    "legends": {
        "color": {
            "enabled": True,
            "position": "inside-right-top",
            "orientation": "vertical",
            "labels": "ranges",
            "labelFormat": legend_format,
            "titleEnabled": False,
            "title": value_column,
            "size": 300,
            "reverse": False,
            "interactive": True,
            "offsetX": 0,
            "offsetY": 0,
        },
    },
    # Tooltips: municipality name and value
    "tooltip": {
        "enabled": True,
        "sticky": True,
        "title": "%REGION_NAME%",
        "body": tooltip_body,
    },
    "patterns": {"enabled": False},
    "dark-mode-invert": True,
}

apply_and_refresh(session, chart_id, CHART_TITLE, "d3-maps-choropleth", metadata)
