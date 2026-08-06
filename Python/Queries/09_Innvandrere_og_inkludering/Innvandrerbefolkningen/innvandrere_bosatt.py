import os
import pandas as pd

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# List to collect errors during execution
error_messages = []

# ============================================================
# Step 1: Query andel innvandrere bosatt (table 09817), latest year, for:
#         a) Telemark municipalities
#         b) Telemark county
#         c) Norway (the whole country)
#         InnvandrKat=B (innvandrere), Landbakgrunn=999 (alle land)
# ============================================================

GET_URL_KOMMUNER = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/09817/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=AndelBefolkning"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036"
    "&valueCodes[InnvandrKat]=B"
    "&valueCodes[Landbakgrunn]=999"
)

GET_URL_FYLKE = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/09817/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=AndelBefolkning"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=40"
    "&valueCodes[InnvandrKat]=B"
    "&valueCodes[Landbakgrunn]=999"
)

GET_URL_LANDET = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/09817/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=AndelBefolkning"
    "&valueCodes[Tid]=top(1)"
    "&valueCodes[Region]=0"
    "&valueCodes[InnvandrKat]=B"
    "&valueCodes[Landbakgrunn]=999"
)

try:
    df_kommuner_raw = fetch_data(
        url=GET_URL_KOMMUNER,
        payload=None,
        error_messages=error_messages,
        query_name="Andel bosatt kommuner",
        response_type="json",
    )
    df_fylke_raw = fetch_data(
        url=GET_URL_FYLKE,
        payload=None,
        error_messages=error_messages,
        query_name="Andel bosatt Telemark fylke",
        response_type="json",
    )
    df_landet_raw = fetch_data(
        url=GET_URL_LANDET,
        payload=None,
        error_messages=error_messages,
        query_name="Andel bosatt Norge",
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
# Step 2: Process each dataset (AndelBefolkning is already a
#         percentage, so just clean up and round the value).
# ============================================================


def process_andel(df):
    """Given a raw json-stat2 dataframe (columns: region, år, value, ...),
    return a dataframe with one row per region containing the rounded
    percentage of innvandrere bosatt. Also returns the year used."""
    df = df.copy()
    df.columns = [col.strip() for col in df.columns]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    year = df["år"].astype(str).iloc[0]
    df["andel"] = df["value"].round(0).astype(int)

    return df[["region", "andel"]], year


df_andel_kommuner, year_kommuner = process_andel(df_kommuner_raw)
df_andel_fylke, year_fylke = process_andel(df_fylke_raw)
df_andel_landet, year_landet = process_andel(df_landet_raw)

# Sanity check: all three queries should refer to the same year (they all use top(1))
if not (year_kommuner == year_fylke == year_landet):
    print(
        "Warning: Year differs between kommune-, fylke- and landstall "
        f"(kommuner: {year_kommuner}, fylke: {year_fylke}, landet: {year_landet})."
    )

andel_col = f"Andel bosatt {year_kommuner}"

for df_andel in (df_andel_kommuner, df_andel_fylke, df_andel_landet):
    df_andel.rename(columns={"andel": andel_col}, inplace=True)
    # Remove number prefix like "4001 " (municipalities); leaves "Telemark"/"Norge" untouched
    df_andel["Region"] = df_andel["region"].str.replace(r"^\d+\s+", "", regex=True)
    df_andel["Label"] = df_andel["Region"]

print(f"Calculated andel bosatt for {year_kommuner}")

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

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "andel_innvandrere_bosatt.csv"
task_name = "Innvandrere - Bosatt"
github_folder = "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call the function and get the "New Data" status
is_new_data = handle_output_data(df_final, file_name, github_folder, temp_folder, keepcsv=True)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = task_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
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
