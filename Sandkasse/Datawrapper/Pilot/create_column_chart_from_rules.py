import pandas as pd

from datawrapper_generelt import (
    COUNTRY_NAMES,
    COUNTY_NAMES,
    get_session,
    find_data_source,
    number_formats,
    general_metadata,
    create_or_get_chart,
    apply_and_refresh,
)

## Pilot: create (or update) a live column chart in Datawrapper, formatted according to
## Formatering/formatering_generelt.txt and Formatering/formatering_stolpediagram.txt

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Sandkasse/Datawrapper/Pilot/Dummy%20data/column_diagram.csv"
CHART_TITLE = "Andel sysselsatt"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = None

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = None

## Colors (formatering_stolpediagram.txt Part 1): fixed, not the general palette
MAIN_COLOR = "#337580"  # Vann, all columns
COUNTRY_COLOR = "#b7ad61"  # Korn
COUNTY_COLOR = "#c54564"  # Nype


session = get_session()

source_name, source_url = find_data_source(QUERY_SCRIPT)
print(f"Source: {source_name or '(none)'} {source_url}")


## Read the CSV: first column is the column labels, second column the values
## (names are stripped, as Datawrapper ignores leading/trailing spaces in headers)
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
label_column, value_column = df.columns[0], df.columns[1]
if len(df.columns) > 2:
    print(f"Warning: {len(df.columns) - 1} value columns, only '{value_column}' is shown (grouped columns not covered)")
labels = df[label_column].astype(str).str.strip()
print(f"Columns: {list(labels)}, values from '{value_column}'")


## Number formats (general rules): % for shares, whole numbers (one decimal below 10), tooltips one decimal
value_format, tooltip_format = number_formats(df[value_column], CHART_TITLE)
print(f"Value axis/label format: {value_format}, tooltip format: {tooltip_format}")


## Colors: country and county rows get their own color, the rest MAIN_COLOR; both kept out of the legend
color_map = {label: COUNTRY_COLOR for label in labels if label in COUNTRY_NAMES}
color_map.update({label: COUNTY_COLOR for label in labels if label in COUNTY_NAMES})
print(f"Country/county columns: {color_map or '(none)'}")


## 1) Create an empty column chart in the team (unless updating an existing one)
chart_id = create_or_get_chart(session, CHART_ID, CHART_TITLE, "column-chart")


## 2) Link the CSV as external data (live) and apply the formatting
metadata = general_metadata(CSV_URL, source_name, source_url)
metadata["axes"] = {"column": value_column}
metadata["visualize"] = {
    "base-color": MAIN_COLOR,
    "color-by-column": True,
    "color-category": {
        "map": color_map,
        "excludeFromKey": list(color_map),
    },
    "show-color-key": True,
    "label-colors": False,
    "value-label-colors": True,
    "negativeColor": {"enabled": False},
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
    "y-grid-format": value_format,
    "tooltip-number-format": tooltip_format,  # key taken from line charts, not checked for column charts
    "y-grid-labels": "auto",
    "y-grid-label-align": "left",
    "scale-y": "linear",
    "custom-range": ["", ""],  # automatic: value axis starts at 0
    "rotate-labels": "auto",
    "bar-padding": 60,
    "sort-values": False,
    "reverse-order": False,
    "show-tooltips": True,
    "plotHeightMode": "ratio",
    "plotHeightRatio": 0.5,
}

apply_and_refresh(session, chart_id, CHART_TITLE, "column-chart", metadata)
