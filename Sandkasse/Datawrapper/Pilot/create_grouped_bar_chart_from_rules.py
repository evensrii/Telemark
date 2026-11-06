import pandas as pd

from datawrapper_generelt import (
    GROUPED_PALETTE,
    get_session,
    find_data_source,
    number_formats,
    general_metadata,
    create_or_get_chart,
    apply_and_refresh,
)

## Pilot: create (or update) a live grouped bar chart (horizontal bars) in Datawrapper, formatted according to
## Formatering/formatering_generelt.txt and Formatering/formatering_gruppert_liggende_stolpediagram.txt

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Sandkasse/Datawrapper/Pilot/Dummy%20data/grouped_column_diagram.csv"
CHART_TITLE = "Test av formatering gruppert liggende stolpediagram"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "RtNRv"

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = None


session = get_session()

source_name, source_url = find_data_source(QUERY_SCRIPT)
print(f"Source: {source_name or '(none)'} {source_url}")


## Read the CSV: first column is the groups (e.g. age groups), the other columns are the series
## (names are stripped, as Datawrapper ignores leading/trailing spaces in headers).
## Unlike grouped column charts, grouped bars read the CSV as it is (no transpose).
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
group_column = df.columns[0]
series = list(df.columns[1:])
print(f"Groups: {list(df[group_column])}, series: {series}")
if len(series) > len(GROUPED_PALETTE):
    print(f"Warning: more than {len(GROUPED_PALETTE)} series, colors are reused")


## Number formats (general rules): % for shares, whole numbers (one decimal below 10)
value_format, _ = number_formats(df[series].stack(), CHART_TITLE)
print(f"Value label format: {value_format}")


## Colors: series in column order, #337580 (Vann) and #b7ad61 (Korn) first, then the rest of the general
## palette in its order (no fixed colors for country/county)
color_map = {name: GROUPED_PALETTE[i % len(GROUPED_PALETTE)] for i, name in enumerate(series)}
print(f"Colors: {color_map}")


## 1) Create an empty grouped bar chart in the team (unless updating an existing one)
chart_id = create_or_get_chart(session, CHART_ID, CHART_TITLE, "d3-bars-grouped")


## 2) Link the CSV as external data (live) and apply the formatting
metadata = general_metadata(CSV_URL, source_name, source_url)
metadata["data"]["transpose"] = False
metadata["visualize"] = {
    "custom-colors": color_map,  # this chart type ignores color-category
    "show-color-key": True,  # legend above the chart
    "value-label-format": value_format,
}

apply_and_refresh(session, chart_id, CHART_TITLE, "d3-bars-grouped", metadata)
