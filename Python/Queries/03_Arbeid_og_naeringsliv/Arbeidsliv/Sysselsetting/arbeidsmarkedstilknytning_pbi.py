import os
import pandas as pd
from pyjstat import pyjstat

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Query prioritert arbeidsstyrkestatus (table 13563)
#         for the Telemark municipalities, Telemark fylke and
#         Hele landet, from 2008 until the latest available year.
#         Municipalities and fylke use SSB's merged time series
#         (2024 structure), so they are comparable back to 2008.
# ============================================================

BASE_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/13563/data?lang=no"
    "&valueCodes[ContentsCode]=Bosatte"
    "&valueCodes[Tid]=from(2008)"
    "&valueCodes[HovArbStyrkStatus]=TOT2,A.01,A.09,U.01,U.03,U.04-U.05,U.06-U.07,U.90A"
    "&valueCodes[Alder]=15-29,30-61,15-74"
    "&valueCodes[InnvandrKat]=A-G"
    "&codelist[HovArbStyrkStatus]=vs_ArbStatus2018niva2a"
)

REGION_QUERIES = {
    "Kommuner": (
        "&valueCodes[Region]=K-4001,K-4003,K-4005,K-4010,K-4012,K-4014,K-4016,K-4018,K-4020,"
        "K-4022,K-4024,K-4026,K-4028,K-4030,K-4032,K-4034,K-4036"
        "&codelist[Region]=agg_KommSummer"
        "&outputValues[Region]=aggregated"
    ),
    "Telemark": (
        "&valueCodes[Region]=F-40"
        "&codelist[Region]=agg_KommFylker"
        "&outputValues[Region]=aggregated"
    ),
    "Hele landet": "&valueCodes[Region]=0",
}

dfs = []
for name, region_params in REGION_QUERIES.items():
    try:
        df_part = fetch_data(
            url=BASE_URL + region_params,
            payload=None,
            error_messages=error_messages,
            query_name=f"Arbeidsmarkedstilknytning ({name})",
            response_type="json",
        )
    except Exception as e:
        print(f"Error occurred: {e}")
        raise RuntimeError(
            "A critical error occurred during data fetching, stopping execution."
        )
    dfs.append(df_part)

df_raw = pd.concat(dfs, ignore_index=True)

print(f"Raw data: {len(df_raw)} rows")
print(df_raw.head(10))

# ============================================================
# Step 2: Clean up to long format for Power BI
# ============================================================

df = df_raw.copy()
df.columns = ["region", "status", "alder", "innvandringskategori", "statistikkvariabel", "år", "value"]
df["value"] = pd.to_numeric(df["value"], errors="coerce")

df_final = pd.DataFrame(
    {
        "Region": df["region"],
        "Arbeidsstyrkestatus": df["status"],
        "Alder": df["alder"],
        # Date column for Power BI (1. januar of the year)
        "År": pd.to_datetime(df["år"] + "-01-01").dt.strftime("%Y-%m-%d"),
        "Antall": df["value"].astype("Int64"),
    }
)

# Andel (fraction, 0-1) of "I alt" for the same region, age group and year
total = df_final[df_final["Arbeidsstyrkestatus"] == "I alt"].set_index(["Region", "Alder", "År"])["Antall"]
df_final["Andel"] = (
    df_final["Antall"]
    / df_final.set_index(["Region", "Alder", "År"]).index.map(total).astype(float)
).round(3)

print(df_final.head(20))
print(f"Regions: {df_final['Region'].nunique()}, years: {df_final['År'].min()} - {df_final['År'].max()}, rows: {len(df_final)}")

# ============================================================
# Step 3: Save to CSV, compare and upload to GitHub
# ============================================================

file_name = "arbeidsmarkedstilknytning_pbi.csv"
task_name = "Arbeid og naeringsliv - Arbeidsmarkedstilknytning pbi"
github_folder = "Data/03_Arbeid og næringsliv/01_Arbeidsliv/Sysselsetting"
temp_folder = os.environ.get("TEMP_FOLDER")

value_columns = ["Antall", "Andel"]

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df_final, file_name, github_folder, temp_folder, keepcsv=True, value_columns=value_columns
)

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
