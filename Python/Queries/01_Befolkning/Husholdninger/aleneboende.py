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
# Step 1: Query privathusholdninger etter husholdningstype (table 06070), for:
#         a) Norway (the whole country)
#         b) Telemark county
#         c) Telemark municipalities
#         HushType 001 = "Aleneboende", the rest are the other household types.
# ============================================================

GET_URL_LANDET = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/06070/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Husholdninger"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=0"
    "&valueCodes[HushType]=001,002,003,004,005,006,007,008,009,010,000"
)

GET_URL_FYLKE = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/06070/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Husholdninger"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=F-40"
    "&valueCodes[HushType]=001,002,003,004,005,006,007,008,009,010,000"
    "&codelist[Region]=agg_KommFylker"
    "&outputValues[Region]=aggregated"
)

GET_URL_KOMMUNER = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/06070/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Husholdninger"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
    "&valueCodes[HushType]=001,002,003,004,005,006,007,008,009,010,000"
    "&codelist[Region]=agg_KommSummer"
    "&outputValues[Region]=aggregated"
)

try:
    df_landet_raw = fetch_data(
        url=GET_URL_LANDET,
        payload=None,
        error_messages=error_messages,
        query_name="Aleneboende Norge",
        response_type="json",
    )
    df_fylke_raw = fetch_data(
        url=GET_URL_FYLKE,
        payload=None,
        error_messages=error_messages,
        query_name="Aleneboende Telemark fylke",
        response_type="json",
    )
    df_kommuner_raw = fetch_data(
        url=GET_URL_KOMMUNER,
        payload=None,
        error_messages=error_messages,
        query_name="Aleneboende kommuner",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(f"Landet raw data: {len(df_landet_raw)} rows")
print(df_landet_raw.head(15))
print(f"Fylke raw data: {len(df_fylke_raw)} rows")
print(df_fylke_raw.head(15))
print(f"Kommuner raw data: {len(df_kommuner_raw)} rows")
print(df_kommuner_raw.head(15))

# ============================================================
# Step 2: Calculate the percentage of "Aleneboende" households
#         out of all households, per region.
# ============================================================


def calculate_percentage(df):
    """Given a raw json-stat2 dataframe (columns: region, husholdningstype,
    år, value, ...), return a dataframe with one row per region containing
    the percentage of "Aleneboende" households out of all households.
    Also returns the year used."""
    df = df.copy()
    df.columns = [col.strip() for col in df.columns]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    year = df["år"].astype(str).iloc[0]

    df_pivot = df.pivot_table(index="region", columns="husholdningstype", values="value", aggfunc="sum").reset_index()

    aleneboende_cols = [c for c in df_pivot.columns if "Aleneboende" in c]
    all_type_cols = [c for c in df_pivot.columns if c != "region"]

    if len(aleneboende_cols) != 1:
        raise ValueError(f"Expected exactly one 'Aleneboende' column, found: {aleneboende_cols}")

    df_pivot["aleneboende"] = df_pivot[aleneboende_cols[0]]
    df_pivot["alle_husholdninger"] = df_pivot[all_type_cols].sum(axis=1)

    df_pivot["andel"] = (
        df_pivot["aleneboende"] / df_pivot["alle_husholdninger"] * 100
    ).round(0).astype(int)

    return df_pivot[["region", "andel"]], year


df_andel_landet, year_landet = calculate_percentage(df_landet_raw)
df_andel_fylke, year_fylke = calculate_percentage(df_fylke_raw)
df_andel_kommuner, year_kommuner = calculate_percentage(df_kommuner_raw)

# Sanity check: all three queries should refer to the same year
if not (year_landet == year_fylke == year_kommuner):
    print(
        "Warning: Year differs between landet-, fylke- og kommunetall "
        f"(landet: {year_landet}, fylke: {year_fylke}, kommuner: {year_kommuner})."
    )

andel_col = f"Andel aleneboende ({year_landet})"

for df_andel in (df_andel_landet, df_andel_fylke, df_andel_kommuner):
    df_andel.rename(columns={"andel": andel_col}, inplace=True)
    # Remove number prefix like "4001 " (municipalities); leaves "Telemark"/"Norge" untouched
    df_andel["Region"] = df_andel["region"].str.replace(r"^\d+\s+", "", regex=True)
    df_andel["Label"] = df_andel["Region"]

print(f"Calculated andel aleneboende for {year_landet}")

# ============================================================
# Step 3: Combine municipalities, county and country into one
#         standard Everviz table
# ============================================================

df_final = pd.concat(
    [
        df_andel_kommuner[["Region", andel_col, "Label"]],
        df_andel_fylke[["Region", andel_col, "Label"]],
        df_andel_landet[["Region", andel_col, "Label"]],
    ],
    ignore_index=True,
)

print(df_final)

# ============================================================
# Step 4: Save to CSV, compare and upload to GitHub
# ============================================================

file_name = "aleneboende.csv"
task_name = "Befolkning - Aleneboende"
github_folder = "Data/01_Befolkning/Husholdninger"
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
