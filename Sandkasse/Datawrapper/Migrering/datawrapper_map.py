import re

import pandas as pd
import requests

from datawrapper_generelt import (
    API_BASE,
    find_data_source,
    is_share,
    general_metadata,
    create_or_get_chart,
    apply_and_refresh,
)

## Migrering: create (or update) a live choropleth map of the municipalities in Telemark in Datawrapper,
## formatted according to Formatering/formatering_map.txt (maps don't use formatering_generelt.txt).
## Used by create_map.py (one map) and create_maps_from_list.py (batch).

## Colors (formatering_map.txt Part 1), light to dark
STEP_COLORS = {
    5: ["#f6efd5", "#f0e1bd", "#e0c58e", "#d4b67c", "#c7a869"],
    3: ["#f0e1bd", "#e0c58e", "#c7a869"],
    2: ["#f0e1bd", "#d4b67c"],  # second lightest and second darkest of the 5 colors
}

## Basemap: the municipalities in Telemark (TopoJSON in the repo), uploaded to each map as the asset
## "<chart id>.map.json"
BASEMAP_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Kart/Kartfiler/Datawrapper_kommuner_Telemark.json"

# Column in the CSV with the map labels (always "Label+Value").
# The region names (matched against the basemap) are the first column, whatever it's called
# ("Region", "Kommune", "Område" ...).
LABEL_COLUMN = "Label+Value"

# Column with the year/period of the data in the dw_ files (e.g. "2026" or "2022-2026"), shown in the tooltip
# title as "Vinje (2026)". Set to text in the chart, otherwise Datawrapper reads "2026" as a date
# ("Thu Jan 01 2026 00:00:00 GMT+0100 ...").
YEAR_COLUMN = "Data_fra_aar"


## The tooltip refers to columns by their variable name: lowercase, spaces to "_", special characters
## removed (Datawrapper's columnNameToVariable), e.g. "Forsørgerevne (2026)" -> "forsrgerevne_2026"
def column_name_to_variable(name):
    name = re.sub(r"\s+", "_", str(name).lower())
    name = re.sub(r"[^\w-]+", "", name, flags=re.ASCII)
    name = re.sub(r"__+", "_", name.replace("-", "_")).strip("_")
    name = re.sub(r"^(\d)", r"_\1", name)
    return re.sub(r"^(and|or|in|true|false)$", r"\1_", name)


## Colors for other numbers of steps (e.g. 4): evenly spread from the lightest to the darkest of the 5 colors,
## interpolated along the 5-color scale (the colors in between are calculated)
def spread_colors(num_steps, scale=STEP_COLORS[5]):
    rgb = [tuple(int(c[i:i + 2], 16) for i in (1, 3, 5)) for c in scale]
    result = []
    for k in range(num_steps):
        t = k / (num_steps - 1) * (len(rgb) - 1)  # position on the scale, 0 ... 4
        i = min(int(t), len(rgb) - 2)
        f = t - i
        result.append("#" + "".join(f"{round(a + (b - a) * f):02x}" for a, b in zip(rgb[i], rgb[i + 1])))
    return result


def step_colors(num_steps):
    return STEP_COLORS.get(num_steps) or spread_colors(num_steps)


def create_map(session, csv_url, title, chart_id=None, query_script=None, num_steps=5, description="",
               folder_id=None, publish=False):
    """Create (chart_id=None) or update a map. Returns the chart id.

    csv_url:      raw GitHub URL of the dw_ file
    query_script: query script relative to PYTHONPATH (for the data source), or None
    num_steps:    number of color steps (5 standard; 3 and 2 have fixed colors, others are calculated)
    description:  Datawrapper "Description" (subtitle below the title), metadata.describe.intro
    folder_id:    Datawrapper folder to put the map in (None: leave where it is / team root)
    publish:      publish the map (Datawrapper only refreshes live data automatically after the first publish)
    """
    source_name, source_url = find_data_source(query_script)
    print(f"Source: {source_name or '(none)'} {source_url}")

    ## Read the CSV: the first column is the region names, the value column is the first column after it
    ## that is not "Label", "Label+Value" or "Data_fra_aar" (the dw_ files made by Helper_scripts/datawrapper_functions.py)
    df = pd.read_csv(csv_url)
    df.columns = [c.strip() for c in df.columns]
    key_column = df.columns[0]
    value_column = next(c for c in df.columns[1:] if c not in ("Label", LABEL_COLUMN, YEAR_COLUMN))
    has_year = YEAR_COLUMN in df.columns
    print(f"Regions: {len(df)} rows from '{key_column}', colors from '{value_column}', labels from '{LABEL_COLUMN}'")

    ## Number formats: % for shares ("Andel" in the value column name or in the title)
    ## Legend: shares as whole numbers ("41%–44%"), other values with up to one decimal ("1,5–1,7"), never two
    ## Tooltip: the value with a decimal comma, via FORMAT (%REGION_VALUE% shows "1.6"): shares always with one
    ## decimal ("41,5 %"), other values with up to one decimal.
    share = is_share(value_column) or is_share(title)
    value_variable = column_name_to_variable(value_column)
    ## Legend: same rule as the map labels: shares whole numbers; other values one decimal if the largest value
    ## is 10 or lower, otherwise whole numbers
    max_value = pd.to_numeric(df[value_column], errors="coerce").max()
    legend_format = "0%" if share else ("0,0.[0]" if max_value <= 10 else "0,0")
    tooltip_body = (
        f'{{{{ FORMAT({value_variable}, "0.0") }}}} %' if share else f'{{{{ FORMAT({value_variable}, "0.[0]") }}}}'
    )
    ## Tooltip title: the municipality, with the year in parentheses if the CSV has Data_fra_aar ("Vinje (2026)")
    if has_year:
        tooltip_title = (
            f"{{{{ {column_name_to_variable(key_column)} }}}} ({{{{ {column_name_to_variable(YEAR_COLUMN)} }}}})"
        )
    else:
        tooltip_title = "%REGION_NAME%"
    print(f"Tooltip: title {tooltip_title}, body {tooltip_body}; legend format {legend_format}")

    ## Color steps: equal distance between the colors (positions 0 ... 1)
    colors = step_colors(num_steps)
    print(f"Colors ({num_steps} steps): {colors}")
    color_stops = [
        {"color": color, "position": round(i / (num_steps - 1), 4)} for i, color in enumerate(colors)
    ]

    ## 1) Create an empty map in the team (unless updating an existing one), and put it in its folder
    chart_id = create_or_get_chart(session, chart_id, title, "d3-maps-choropleth")
    if folder_id is not None:
        response = session.patch(f"{API_BASE}/charts/{chart_id}", json={"folderId": folder_id})
        response.raise_for_status()
        print(f"Moved to folder {folder_id}")

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
    metadata = general_metadata(csv_url, source_name, source_url)
    metadata["describe"]["intro"] = description or ""
    metadata["data"]["transpose"] = False
    if has_year:
        metadata["data"]["column-format"] = {YEAR_COLUMN: {"type": "text"}}
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
            "stopCount": num_steps,
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
        "avoid-label-overlap": False,  # always show all labels (editor: "Prevent label overlapping" off)
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
        # Tooltips: "Vinje (2026)" and the value
        "tooltip": {
            "enabled": True,
            "sticky": True,
            "title": tooltip_title,
            "body": tooltip_body,
        },
        "patterns": {"enabled": False},
        "dark-mode-invert": True,
    }

    apply_and_refresh(session, chart_id, title, "d3-maps-choropleth", metadata)

    ## 4) Publish (live data is only refreshed automatically after the first publish)
    if publish:
        response = session.post(f"{API_BASE}/charts/{chart_id}/publish")
        response.raise_for_status()
        print(f"Published: https://datawrapper.dwcdn.net/{chart_id}/")

    return chart_id
