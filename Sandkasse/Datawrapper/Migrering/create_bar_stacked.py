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

## Migrering: create (or update) a live stacked bar chart (horizontal bars) in Datawrapper, formatted according to
## Formatering/formatering_generelt.txt and Formatering/formatering_bar_stacked.txt

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Sandkasse/Datawrapper/Migrering/Dummy%20data/h%C3%B8yeste_utdanning_innv_test_2.csv"
CHART_TITLE = "Test av formatering stacked barchart 2"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "l3OLw"

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = None

## Part 1: "Stack percentages" if every row sums to 100 within this tolerance, otherwise "Show totals"
STACK_PERCENT_MIN, STACK_PERCENT_MAX = 98, 102


session = get_session()

source_name, source_url = find_data_source(QUERY_SCRIPT)
print(f"Source: {source_name or '(none)'} {source_url}")


## Read the CSV: first column is the bars, the other columns are the stacked segments
## (names are stripped, as Datawrapper ignores leading/trailing spaces in headers).
## Unlike stacked column charts, stacked bars read the CSV as it is (no transpose).
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
bar_column = df.columns[0]
segments = list(df.columns[1:])
print(f"Bars: {list(df[bar_column])}, segments: {segments}")
if len(segments) > len(STACKED_PALETTE):
    print(f"Warning: more than {len(STACKED_PALETTE)} segments, colors are reused")


## Part 1: stack percentages if every row sums to about 100, otherwise show totals
row_sums = df[segments].sum(axis=1)
stack_percentages = bool(row_sums.between(STACK_PERCENT_MIN, STACK_PERCENT_MAX).all())
print(f"Row sums: {list(row_sums.round(1))}, stack percentages: {stack_percentages}, totals: {not stack_percentages}")


## Number formats (general rules): % for shares, whole numbers (one decimal below 10)
value_format, _ = number_formats(df[segments].stack(), CHART_TITLE)
print(f"Value label format: {value_format}")


## Colors: segments in column order, five main colors first, then the rest of the general palette
color_map = {name: STACKED_PALETTE[i % len(STACKED_PALETTE)] for i, name in enumerate(segments)}
print(f"Colors: {color_map}")


## 1) Create an empty stacked bar chart in the team (unless updating an existing one)
chart_id = create_or_get_chart(session, CHART_ID, CHART_TITLE, "d3-bars-stacked")


## 2) Link the CSV as external data (live) and apply the formatting
metadata = general_metadata(CSV_URL, source_name, source_url)
metadata["data"]["transpose"] = False
metadata["visualize"] = {
    "color-by-column": True,
    "color-category": {"map": color_map},
    "label-colors": False,
    "value-label-colors": True,
    "negativeColor": {"enabled": False},
    "stack-percentages": stack_percentages,  # "Stack percentages" (not "normalize" as in stacked columns)
    "totals-labels": not stack_percentages,  # "Show totals" (not "totalsLabels" as in stacked columns)
    "totals-labels-position": "after",  # totals to the right of the bar
    "show-color-key": True,  # legend above the chart
    "show-group-labels": True,
    "label-alignment": "right",
    "show-value-labels": True,
    "value-label-visibility": "always",
    "value-label-mode": "left",
    "value-label-alignment": "left",
    "value-label-format": value_format,
    "force-grid": False,  # no value axis or gridlines
    "custom-range": ["", ""],
    "thick": True,
    "background": True,
    "rules": False,
    "space-between-cols": 10,
    "sort-bars": False,
    "reverse-order": False,
    "group-by-column": False,
    "plotHeightMode": "fixed",
    "plotHeightFixed": 300,
}

apply_and_refresh(session, chart_id, CHART_TITLE, "d3-bars-stacked", metadata)
