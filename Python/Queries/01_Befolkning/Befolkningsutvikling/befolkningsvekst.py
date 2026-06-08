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
# Step 1: Query folkemengde per kommune i Telemark (table 06913)
#         From 2022, all Telemark municipalities.
# ============================================================

GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/06913/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[Tid]=from(2022)"
    "&valuecodes[Region]=K_4001,K_4003,K_4005,K_4010,K_4012,K_4014,K_4016,K_4018,K_4020,K_4022,K_4024,K_4026,K_4028,K_4030,K_4032,K_4034,K_4036"
    "&codelist[Region]=agg_KommSummerHist"
    "&outputValues[Region]=aggregated"
    "&valuecodes[ContentsCode]=Folkemengde"
    "&heading=Tid,ContentsCode"
    "&stub=Region"
)

try:
    df = fetch_data(
        url=GET_URL,
        payload=None,
        error_messages=error_messages,
        query_name="Befolkningsvekst Telemark",
        response_type="json",
    )
except Exception as e:
    print(f"Error occurred: {e}")
    notify_errors(error_messages, script_name=script_name)
    raise RuntimeError(
        "A critical error occurred during data fetching, stopping execution."
    )

print(f"Raw data: {len(df)} rows")
print(df.head(10))

# ============================================================
# Step 2: Calculate percentage change per municipality
#         (latest_year - earliest_year) / earliest_year * 100
# ============================================================

# Rename columns for easier handling
df.columns = [col.strip() for col in df.columns]
print(df.columns.tolist())

# Identify the year column and value column
# SSB json-stat2 typically returns columns like: region, statistikkvariabel, år, value
# Adjust column names based on actual output
df["år"] = df["år"].astype(str)
df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Get earliest and latest year
earliest_year = df["år"].min()
latest_year = df["år"].max()
print(f"Calculating change from {earliest_year} to {latest_year}")

# Pivot to get one row per municipality with earliest and latest population
df_earliest = df[df["år"] == earliest_year][["region", "value"]].rename(columns={"value": "earliest"})
df_latest = df[df["år"] == latest_year][["region", "value"]].rename(columns={"value": "latest"})

df_change = df_earliest.merge(df_latest, on="region")

# Calculate percentage change, rounded to nearest integer
df_change["Andel"] = ((df_change["latest"] - df_change["earliest"]) / df_change["earliest"] * 100).round(0).astype(int)

# Rename "Andel" column to include the year range
andel_col = f"Andel ({earliest_year}-{latest_year})"
df_change = df_change.rename(columns={"Andel": andel_col})

# Extract municipality name (remove number prefix like "4001 ")
df_change["Kommune"] = df_change["region"].str.replace(r"^\d+\s+", "", regex=True)
df_change["Label"] = df_change["Kommune"]

# Select and order final columns
df_final = df_change[["Kommune", andel_col, "Label"]].reset_index(drop=True)

print(df_final)

# ============================================================
# Step 3: Save to CSV, compare and upload to GitHub
# ============================================================

file_name = "befolkningsvekst.csv"
task_name = "Befolkning - Befolkningsvekst"
github_folder = "Data/01_Befolkning/Befolkningsutvikling"
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
