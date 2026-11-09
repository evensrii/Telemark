import math

import pandas as pd

from Helper_scripts.github_functions import handle_output_data

## Datawrapper versions ("dw_<file name>") of the CSVs used for maps.
## The original CSVs (used by Everviz) are left untouched; the dw_ version is uploaded next to them with:
##   - the region column and "Label" as in the original
##   - the value column under a fixed name without the year (e.g. "Andel aleneboende"), so the Datawrapper
##     maps keep working when the year changes
##   - "Label+Value": map label with the name and the value on two lines, e.g. "Bamble<br>37%"
##   - "Data_fra_aar": the year (or period) of the data, e.g. "2026" or "2022-2026", for use in tooltips
## The dw_ upload is not part of the "new data" status in the run email (the Everviz file still decides that).
## Rules: Sandkasse/Datawrapper/Migrering/Formatering/formatering_map.txt


def is_share(name):
    return "andel" in str(name).lower()


def _round_half_up(value, decimals=0):
    factor = 10**decimals
    return math.floor(value * factor + 0.5) / factor


def _label_value(value, share, one_decimal):
    """Value as shown in the map label (tooltips show the value column with one decimal):
    - shares: whole number with "%"
    - other values: one decimal (decimal comma) if the largest value in the data set is 10 or lower,
      otherwise a whole number; no unit"""
    if pd.isna(value):
        return ""
    if share:
        return f"{int(_round_half_up(value))}%"
    if one_decimal:
        return f"{_round_half_up(value, 1):.1f}".replace(".", ",")
    return f"{int(_round_half_up(value))}"


def make_dw_map_df(df, value_column, value_name, year, label_column="Label"):
    """Build the Datawrapper version of a map data set.

    df:            the data set (first column: region names, e.g. "Region" or "Kommune")
    value_column:  the value column in df (may hold more decimals than the Everviz file; it is rounded
                   to one decimal here)
    value_name:    fixed name for the value column in the dw_ file, without the year (e.g. "Andel aleneboende")
    year:          the year or period of the data (e.g. 2026 or "2022-2026"), stored in "Data_fra_aar"
    """
    region_column = df.columns[0]
    raw_values = pd.to_numeric(df[value_column], errors="coerce")
    share = is_share(value_name)
    one_decimal = (not share) and raw_values.max() <= 10

    dw_df = pd.DataFrame(
        {
            region_column: df[region_column].values,
            value_name: raw_values.round(1).values,
            label_column: df[label_column].values,
        }
    )
    # Labels are rounded from the unrounded values (rounding 41.46 to 41.5 first would give 42 instead of 41)
    dw_df["Label+Value"] = [
        f"{label}<br>{_label_value(value, share, one_decimal)}"
        for label, value in zip(dw_df[label_column], raw_values)
    ]
    dw_df["Data_fra_aar"] = str(year)
    return dw_df


def handle_dw_map_output(df, value_column, value_name, year, file_name, github_folder, temp_folder, label_column="Label"):
    """Build the dw_ version of a map data set and upload it as "dw_<file_name>" to the same GitHub folder.
    Errors are printed, not raised, so a problem with the dw_ file never stops the Everviz file's script."""
    try:
        dw_df = make_dw_map_df(df, value_column, value_name, year, label_column)
        print(dw_df.head())
        handle_output_data(
            dw_df,
            f"dw_{file_name}",
            github_folder,
            temp_folder,
            keepcsv=True,
            value_columns=[value_name, "Label+Value", "Data_fra_aar"],
        )
    except Exception as e:
        print(f"Warning: could not create/upload dw_{file_name}: {e}")
