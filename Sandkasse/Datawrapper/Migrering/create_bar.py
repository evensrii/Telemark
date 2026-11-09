import pandas as pd

from datawrapper_generelt import (
    get_session,
    find_data_source,
    number_formats,
    general_metadata,
    create_or_get_chart,
    apply_and_refresh,
)

## Migrering: create (or update) a live bar chart (horizontal bars) in Datawrapper, formatted according to
## Formatering/formatering_generelt.txt and Formatering/formatering_bar.txt

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Sandkasse/Datawrapper/Migrering/Dummy%20data/bar_chart.csv"
CHART_TITLE = "Andel som oppgir ulike årsaker (test av formatering bar chart)"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "6qxhD"

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = None

## Colors (formatering_bar.txt Part 1): one color for all bars
MAIN_COLOR = "#337580"  # Vann


session = get_session()

source_name, source_url = find_data_source(QUERY_SCRIPT)
print(f"Source: {source_name or '(none)'} {source_url}")


## Read the CSV: first column is the bar labels, second column the values
## (names are stripped, as Datawrapper ignores leading/trailing spaces in headers).
## The CSV is read as it is (no transpose), in its own row order (no sorting).
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
label_column, value_column = df.columns[0], df.columns[1]
if len(df.columns) > 2:
    print(f"Warning: {len(df.columns) - 1} value columns, only '{value_column}' is shown (use grouped or stacked bars)")
print(f"Bars: {len(df)}, values from '{value_column}'")


## Number formats (general rules): % for shares, whole numbers (one decimal below 10)
value_format, _ = number_formats(df[value_column], CHART_TITLE)
print(f"Value label format: {value_format}")


## 1) Create an empty bar chart in the team (unless updating an existing one)
chart_id = create_or_get_chart(session, CHART_ID, CHART_TITLE, "d3-bars")


## 2) Link the CSV as external data (live) and apply the formatting
metadata = general_metadata(CSV_URL, source_name, source_url)
metadata["data"]["transpose"] = False
metadata["visualize"] = {
    "base-color": MAIN_COLOR,
    "color-by-column": False,
    "label-colors": False,
    "value-label-colors": True,
    "negativeColor": {"enabled": False},
    "show-group-labels": True,
    "show-category-labels": True,
    "label-alignment": "right",  # bar labels right-aligned next to the bars
    "show-value-labels": True,
    "value-label-alignment": "right",  # value labels at the end of the bar
    "value-label-format": value_format,
    "force-grid": False,  # no value axis or gridlines
    "custom-range": ["", ""],
    "thick": True,
    "background": False,
    "rules": False,
    "sort-bars": False,  # keep the row order from the CSV
    "reverse-order": False,
}

apply_and_refresh(session, chart_id, CHART_TITLE, "d3-bars", metadata)
