import pandas as pd

from datawrapper_generelt import (
    STACKED_PALETTE,
    get_session,
    find_data_source,
    number_formats,
    general_metadata,
    create_or_get_chart,
    apply_and_refresh,
)

## Pilot: create (or update) a live stacked column chart in Datawrapper, formatted according to
## Formatering/formatering_generelt.txt and Formatering/formatering_stacked_stolpediagram.txt

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/09_Innvandrere%20og%20inkludering/Utdanningsniv%C3%A5%20Telemark/h%C3%B8yeste_utdanning_innv.csv"
CHART_TITLE = "Test av formatering stacked stolpediagram"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "tHLoE"

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = "Queries/09_Innvandrere_og_inkludering/Utdanning/innv_hoyeste_utdanning.py"

## Part 1: "Stack percentages" if every row sums to 100 within this tolerance
STACK_PERCENT_MIN, STACK_PERCENT_MAX = 98, 102


session = get_session()

source_name, source_url = find_data_source(QUERY_SCRIPT)
print(f"Source: {source_name or '(none)'} {source_url}")


## Read the CSV: first column is the columns on the x-axis, the other columns are the stacked segments
## (names are stripped, as Datawrapper ignores leading/trailing spaces in headers).
## Datawrapper transposes the data itself (metadata.data.transpose), the CSV is not changed.
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
category_column = df.columns[0]
segments = list(df.columns[1:])
print(f"Columns: {list(df[category_column])}, segments: {segments}")
if len(segments) > len(STACKED_PALETTE):
    print(f"Warning: more than {len(STACKED_PALETTE)} segments, colors are reused")


## Part 1: stack percentages if every row sums to about 100
row_sums = df[segments].sum(axis=1)
stack_percentages = bool(row_sums.between(STACK_PERCENT_MIN, STACK_PERCENT_MAX).all())
print(f"Row sums: {list(row_sums.round(1))}, stack percentages: {stack_percentages}")


## Number formats (general rules) for value labels and tooltips; the y-axis always uses "0" (Part 1)
value_format, tooltip_format = number_formats(df[segments].stack(), CHART_TITLE)
print(f"Value label format: {value_format}, y-axis format: 0, tooltip format: {tooltip_format}")


## Colors: segments in column order, five main colors first, then the rest of the general palette
color_map = {name: STACKED_PALETTE[i % len(STACKED_PALETTE)] for i, name in enumerate(segments)}
print(f"Colors: {color_map}")


## 1) Create an empty stacked column chart in the team (unless updating an existing one)
chart_id = create_or_get_chart(session, CHART_ID, CHART_TITLE, "stacked-column-chart")


## 2) Link the CSV as external data (live), transpose it, and apply the formatting
metadata = general_metadata(CSV_URL, source_name, source_url)
metadata["data"]["transpose"] = True
metadata["visualize"] = {
    "color-by-column": True,
    "color-category": {"map": color_map},
    "label-colors": True,
    "value-label-colors": True,
    "negativeColor": {"enabled": False},
    "normalize": stack_percentages,  # "Stack percentages"
    "totalsLabels": False,
    "connect-bars": False,
    "category": "direct",  # segment names as labels to the right, no separate legend
    "categoryLabels": {"enabled": True, "position": "direct"},
    "show-color-key": False,
    "label-space": 30,
    "connector-lines": True,
    "valueLabels": {
        "enabled": True,
        "show": "always",
        "placement": "outside",
        "format": value_format,
        "preventOverlap": True,
    },
    "y-grid": "on",
    "grid-lines": "show",
    "x-grid": "off",
    "y-grid-format": "0",
    "tooltip-number-format": tooltip_format,  # key taken from line charts, not checked for stacked columns
    "y-grid-labels": "auto",
    "y-grid-label-align": "left",
    "scale-y": "linear",
    "custom-range": ["", ""],  # Datawrapper default: starts at 0
    "custom-ticks": "",
    "rotate-labels": "auto",
    "bar-padding": 60,
    "sort-values": False,
    "reverse-order": False,
    "show-tooltips": True,
    "plotHeightMode": "ratio",
    "plotHeightRatio": 0.7,
}

apply_and_refresh(session, chart_id, CHART_TITLE, "stacked-column-chart", metadata)
