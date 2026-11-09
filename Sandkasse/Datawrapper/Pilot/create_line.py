import math
import pandas as pd

from datawrapper_generelt import (
    PALETTE,
    COUNTRY_NAMES,
    COUNTY_NAMES,
    get_session,
    find_data_source,
    is_share,
    number_formats,
    general_metadata,
    create_or_get_chart,
    apply_and_refresh,
)

## Pilot: create (or update) a live line chart in Datawrapper, formatted according to
## Formatering/formatering_generelt.txt and Formatering/formatering_line.txt

CSV_URL = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/Sandkasse/Datawrapper/Pilot/Dummy%20data/linjediagram_uten_landet_fylket.csv"
CHART_TITLE = "Andel av en variabel"

# Set to an existing chart id to update it, or None to create a new chart
CHART_ID = "loP3F"

# Query script that fetches the data (relative to PYTHONPATH), used to find the data source.
# None if the data has no query script (e.g. dummy data): the source is left empty.
QUERY_SCRIPT = "Queries/09_Innvandrere_og_inkludering/Arbeid_og_inntekt/andel_sysselsatte_etter_bakgrunn.py"

## Reference lines (country and county): grey, dashed, no symbols
REFERENCE_COLOR = "#7b7b7a"
REFERENCE_LINES = {name: "style1" for name in COUNTRY_NAMES}  # dotted
REFERENCE_LINES.update({name: "style3" for name in COUNTY_NAMES})  # long dashes

## Symbols (Part 2), and Part 1: no symbols if 5 series or more (reference lines not counted)
SYMBOL_SIZE = 4.5
MAX_SERIES_FOR_SYMBOLS = 4

## Part 1: legend instead of series labels if more than 6 series in total (reference lines included)
MAX_SERIES_FOR_SERIES_LABELS = 6
## Part 1: reference lines go in the legend if there are 3 or more ordinary series
MIN_ORDINARY_SERIES_FOR_REFERENCE_LEGEND = 3


session = get_session()

source_name, source_url = find_data_source(QUERY_SCRIPT)
print(f"Source: {source_name or '(none)'} {source_url}")


## Read the CSV: first column is the x-axis, the rest are series
## (names are stripped, as Datawrapper ignores leading/trailing spaces in headers)
df = pd.read_csv(CSV_URL)
df.columns = [c.strip() for c in df.columns]
series = list(df.columns[1:])
reference_series = [s for s in series if s in REFERENCE_LINES]
ordinary_series = [s for s in series if s not in REFERENCE_LINES]
print(f"Series: {ordinary_series}, reference lines: {reference_series}")
if len(ordinary_series) > len(PALETTE):
    print(f"Warning: more than {len(PALETTE)} ordinary series, colors are reused")


## Number formats (general rules): % for shares, whole numbers (one decimal below 10), tooltips one decimal.
## In line charts the value labels follow the y-axis format.
y_values = df[series].stack()
y_grid_format, tooltip_format = number_formats(y_values, CHART_TITLE)
print(f"y-axis/value label format: {y_grid_format}, tooltip format: {tooltip_format}")


## Rounding step for the y-axis: a "nice" number (1, 2 or 5 x 10^n) close to a fifth of the data range,
## so the step scales with the data (e.g. 5 for values 44-72, 500 for values 1200-4000, 0.1 for values 0.2-0.8)
def nice_step(data_range):
    raw = data_range / 5
    magnitude = 10 ** math.floor(math.log10(raw))
    return min((m * magnitude for m in (1, 2, 5, 10)), key=lambda c: abs(math.log10(c / raw)))


## Part 1: y-axis range
## - min: below the lowest value (padding of 10 % of the data range), rounded down to a multiple of the step,
##        never below 0 unless the data has negative values
## - max: above the highest value (padding of 10 % of the data range), rounded up to a multiple of the step,
##        never above 100 for shares
## - minimum span: if the data range is less than 20 % of the largest value, it is widened around its middle
##   to that size first, so small changes don't look dramatic
def y_axis_range(values, is_share):
    low, high = values.min(), values.max()
    min_span = 0.2 * max(abs(low), abs(high)) or 1  # 1 if all values are 0
    if high - low < min_span:
        middle = (low + high) / 2
        low, high = middle - min_span / 2, middle + min_span / 2
    data_range = high - low
    step = nice_step(data_range)
    y_min = math.floor((low - data_range * 0.1) / step) * step
    y_max = math.ceil((high + data_range * 0.1) / step) * step
    if values.min() >= 0:
        y_min = max(0, y_min)
    if is_share:
        y_max = min(100, y_max)
    # round away floating point noise (e.g. 0.30000000000000004)
    return round(y_min, 10), round(y_max, 10)


y_min, y_max = y_axis_range(y_values, is_share(CHART_TITLE))
print(f"Data range: {y_values.min()} - {y_values.max()}, y-axis: {y_min} - {y_max}")


## Colors: ordinary series in column order from the general palette, reference lines grey
color_map = {name: PALETTE[i % len(PALETTE)] for i, name in enumerate(ordinary_series)}
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
chart_id = create_or_get_chart(session, CHART_ID, CHART_TITLE, "d3-lines")


## 2) Link the CSV as external data (live) and apply the formatting
metadata = general_metadata(CSV_URL, source_name, source_url)
metadata["visualize"] = {
    "color-category": {"map": color_map},
    "variate-colors": False,
    "label-colors": True,
    "value-label-colors": True,
    "lines": lines,
    "x-grid": "off",
    "y-grid": "on",
    "x-grid-format": "YYYY",
    "y-grid-format": y_grid_format,
    "tooltip-number-format": tooltip_format,
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
}

apply_and_refresh(session, chart_id, CHART_TITLE, "d3-lines", metadata)
