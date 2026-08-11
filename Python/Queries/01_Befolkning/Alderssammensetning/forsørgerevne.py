import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Query befolkning etter alder (table 07459), latest year, for:
#         a) Telemark municipalities
#         b) Telemark county
#         c) Norway (the whole country)
#         Age groups (agg_Funksjonell1a): 0-5, 6-15, 16-66, 67+ år
# ============================================================

GET_URL_KOMMUNER = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Personer1"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
    "&valueCodes[Alder]=F301,F302,F303,F304"
    "&codelist[Region]=agg_KommSummer"
    "&outputValues[Region]=aggregated"
    "&codelist[Alder]=agg_Funksjonell1a"
    "&outputValues[Alder]=aggregated"
)

GET_URL_FYLKE = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Personer1"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=F-40"
    "&valueCodes[Alder]=F301,F302,F303,F304"
    "&codelist[Region]=agg_KommFylker"
    "&outputValues[Region]=aggregated"
    "&codelist[Alder]=agg_Funksjonell1a"
    "&outputValues[Alder]=aggregated"
)

GET_URL_LANDET = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/07459/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Personer1"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=0"
    "&valueCodes[Alder]=F301,F302,F303,F304"
    "&codelist[Region]=vs_Landet"
    "&codelist[Alder]=agg_Funksjonell1a"
    "&outputValues[Alder]=aggregated"
)

try:
    df_kommuner_raw = fetch_data(
        url=GET_URL_KOMMUNER,
        payload=None,
        error_messages=error_messages,
        query_name="Forsørgerevne kommuner",
        response_type="json",
    )
    df_fylke_raw = fetch_data(
        url=GET_URL_FYLKE,
        payload=None,
        error_messages=error_messages,
        query_name="Forsørgerevne Telemark fylke",
        response_type="json",
    )
    df_landet_raw = fetch_data(
        url=GET_URL_LANDET,
        payload=None,
        error_messages=error_messages,
        query_name="Forsørgerevne Norge",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(f"Kommuner raw data: {len(df_kommuner_raw)} rows")
print(df_kommuner_raw.head(10))
print(f"Fylke raw data: {len(df_fylke_raw)} rows")
print(df_fylke_raw.head(10))
print(f"Landet raw data: {len(df_landet_raw)} rows")
print(df_landet_raw.head(10))

# ============================================================
# Step 2: Calculate the ratio between the working-age population
#         (16-66 år) and the population outside that interval
#         (0-5 + 6-15 + 67 år eller eldre).
# ============================================================


def calculate_ratio(df):
    """Given a raw json-stat2 dataframe (columns: region, alder, år, value, ...),
    return a dataframe with one row per region containing the ratio between
    the working-age population (16-66 år) and everyone outside that
    interval (i.e. number of people 16-66 år per person below 16 or above
    67 år). Also returns the year used."""
    df = df.copy()
    df.columns = [col.strip() for col in df.columns]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    year = df["år"].astype(str).iloc[0]

    df_pivot = df.pivot_table(index="region", columns="alder", values="value", aggfunc="sum").reset_index()

    working_age_cols = [c for c in df_pivot.columns if "16-66" in c]
    dependent_cols = [c for c in df_pivot.columns if c != "region" and c not in working_age_cols]

    if len(working_age_cols) != 1:
        raise ValueError(f"Expected exactly one '16-66 år' column, found: {working_age_cols}")

    df_pivot["arbeidsfør_alder"] = df_pivot[working_age_cols[0]]
    df_pivot["utenfor_arbeidsfør_alder"] = df_pivot[dependent_cols].sum(axis=1)

    df_pivot["ratio"] = (
        df_pivot["arbeidsfør_alder"] / df_pivot["utenfor_arbeidsfør_alder"]
    ).round(1)

    return df_pivot[["region", "ratio"]], year


df_ratio_kommuner, year_kommuner = calculate_ratio(df_kommuner_raw)
df_ratio_fylke, year_fylke = calculate_ratio(df_fylke_raw)
df_ratio_landet, year_landet = calculate_ratio(df_landet_raw)

# Sanity check: all three queries should refer to the same year (they all use top(1))
if not (year_kommuner == year_fylke == year_landet):
    print(
        "Warning: Year differs between kommune-, fylke- and landstall "
        f"(kommuner: {year_kommuner}, fylke: {year_fylke}, landet: {year_landet})."
    )

ratio_col = f"Forsørgerevne ({year_kommuner})"

for df_ratio in (df_ratio_kommuner, df_ratio_fylke, df_ratio_landet):
    df_ratio.rename(columns={"ratio": ratio_col}, inplace=True)
    # Remove number prefix like "4001 " (municipalities); leaves "Telemark"/"Norge" untouched
    df_ratio["Region"] = df_ratio["region"].str.replace(r"^\d+\s+", "", regex=True)
    df_ratio["Label"] = df_ratio["Region"]

print(f"Calculated forsørgerevne for {year_kommuner}")

# ============================================================
# Step 3: Combine municipalities, county and country into one
#         standard Everviz table
# ============================================================

df_final = pd.concat(
    [
        df_ratio_kommuner[["Region", ratio_col, "Label"]],
        df_ratio_fylke[["Region", ratio_col, "Label"]],
        df_ratio_landet[["Region", ratio_col, "Label"]],
    ],
    ignore_index=True,
)

print(df_final)

# ============================================================
# Step 4: Save to CSV, compare and upload to GitHub
# ============================================================

file_name = "forsørgerevne.csv"
task_name = "Befolkning - Forsørgerevne"
github_folder = "Data/01_Befolkning/Alderssammensetning"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_final, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
