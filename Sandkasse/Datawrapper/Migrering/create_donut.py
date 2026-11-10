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

## Migrering: create (or update) a live donut chart in Datawrapper, formatted according to
## Formatering/formatering_generelt.txt and Formatering/formatering_donut.txt

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Data/04_Klima%20og%20ressursforvaltning/Klimagassutslipp/klimagassutslipp_donut.csv"
CHART_TITLE = "Utslipp av klimagasser i Telemark fordelt etter sektor"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = None

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = "Queries/04_Klima_og_energi/Klimagassutslipp/klimagassutslipp.py"


session = get_session()

source_name, source_url = find_data_source(QUERY_SCRIPT)
print(f"Source: {source_name or '(none)'} {source_url}")


## Read the CSV: first column is the slice labels, second column the values (one donut)
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
label_column, value_column = df.columns[0], df.columns[1]
slices = list(df[label_column])
print(f"Slices: {slices}, values from '{value_column}'")


## Number formats: slices show their share of the total as a whole percent; the tooltip shows the value
## itself (general rules: whole numbers, one decimal below 10)
_, tooltip_format = number_formats(df[value_column], CHART_TITLE)


## Colors: as stacked charts, five main colors first, then the rest of the general palette, in row order
color_map = {name: STACKED_PALETTE[i % len(STACKED_PALETTE)] for i, name in enumerate(slices)}
print(f"Colors: {color_map}")


## 1) Create an empty donut chart in the team (unless updating an existing one)
chart_id = create_or_get_chart(session, CHART_ID, CHART_TITLE, "d3-donuts")


## 2) Link the CSV as external data (live) and apply the formatting
metadata = general_metadata(CSV_URL, source_name, source_url)
metadata["data"]["transpose"] = False
metadata["axes"] = {"labels": label_column}
metadata["visualize"] = {
    "color-category": {"map": color_map},
    "donut_hole_size": 50,
    "pie_size": {"inside_labels": 75, "outside_labels": 50},
    "inside_labels": {"enabled": True},
    "outside_labels": {"enabled": False, "edge": False, "color": False},
    "label_text_slices": True,
    "label_values_slices": True,
    "show_percent": True,  # share of the total in the slices
    "value-label-format": "0%",
    "donut_center_label": {"type": "total", "enabled": False},
    "color_key": {"enabled": True, "position": "top", "stack": False, "label_values": False},
    "slice_order": "original",  # keep the CSV order (already sorted largest first)
    "group": {"num_slices": len(slices)},  # don't merge small slices into "other"
    "connector-lines": True,
    "show-tooltips": True,
    "tooltip-number-format": tooltip_format,
    "dark-mode-invert": True,
}

apply_and_refresh(session, chart_id, CHART_TITLE, "d3-donuts", metadata)
