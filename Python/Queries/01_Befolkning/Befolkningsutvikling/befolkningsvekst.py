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
# Step 1: Query folkemengde (table 06913), last 5 years, for:
#         a) Telemark municipalities (agg_KommSummerHist keeps municipality
#            boundaries consistent over time)
#         b) Norway (the whole country)
#         Telemark county is NOT queried directly, since fylke 40 (Telemark)
#         only exists under that code from 2024 onwards (it was part of
#         "Vestfold og Telemark" in 2020-2023). Instead, the county total is
#         derived by summing all municipalities per year (see Step 2).
# ============================================================

GET_URL_KOMMUNER = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/06913/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Folkemengde"
    "&valueCodes[Tid]=top(5)"
    "&valueCodes[Region]=K_4001,K_4003,K_4005,K_4010,K_4012,K_4014,K_4016,K_4018,K_4020,K_4022,K_4024,K_4026,K_4028,K_4030,K_4032,K_4034,K_4036"
    "&codelist[Region]=agg_KommSummerHist"
    "&outputValues[Region]=aggregated"
)

GET_URL_LANDET = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/06913/data?lang=no"
    "&outputFormat=json-stat2"
    "&valueCodes[ContentsCode]=Folkemengde"
    "&valueCodes[Tid]=top(5)"
    "&valueCodes[Region]=0"
    "&codelist[Region]=vs_Landet"
)

try:
    df_kommuner_raw = fetch_data(
        url=GET_URL_KOMMUNER,
        payload=None,
        error_messages=error_messages,
        query_name="Befolkningsvekst kommuner",
        response_type="json",
    )
    df_landet_raw = fetch_data(
        url=GET_URL_LANDET,
        payload=None,
        error_messages=error_messages,
        query_name="Befolkningsvekst Norge",
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
print(f"Landet raw data: {len(df_landet_raw)} rows")
print(df_landet_raw.head(10))

# ============================================================
# Step 2: Calculate percentage change per region
#         (latest_year - earliest_year) / earliest_year * 100
# ============================================================


def build_fylke_raw(df_kommuner_raw):
    """Derive Telemark county totals by summing all municipalities per year."""
    df = df_kommuner_raw.copy()
    df.columns = [col.strip() for col in df.columns]
    df["år"] = df["år"].astype(str)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df_fylke = df.groupby("år", as_index=False)["value"].sum()
    df_fylke["region"] = "Telemark"

    return df_fylke[["region", "år", "value"]]


df_fylke_raw = build_fylke_raw(df_kommuner_raw)
print("Fylke (summed from kommuner):")
print(df_fylke_raw)


def calculate_change(df):
    """Given a raw json-stat2 dataframe (columns: region, år, value, ...),
    return a dataframe with one row per region containing the earliest and
    latest population value, plus the earliest/latest year used."""
    df = df.copy()
    df.columns = [col.strip() for col in df.columns]
    df["år"] = df["år"].astype(str)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    earliest_year = df["år"].min()
    latest_year = df["år"].max()

    df_earliest = df[df["år"] == earliest_year][["region", "value"]].rename(columns={"value": "earliest"})
    df_latest = df[df["år"] == latest_year][["region", "value"]].rename(columns={"value": "latest"})

    df_change = df_earliest.merge(df_latest, on="region")

    # Guard against NaN (e.g. mismatched region labels between years, or
    # missing values in the source data) before casting to int.
    missing = df_change[df_change["earliest"].isna() | df_change["latest"].isna()]
    if not missing.empty:
        print("Warning: missing earliest/latest value for the following region(s), dropping them:")
        print(missing)
        df_change = df_change.dropna(subset=["earliest", "latest"])

    df_change["Andel"] = ((df_change["latest"] - df_change["earliest"]) / df_change["earliest"] * 100).round(0).astype(int)

    return df_change, earliest_year, latest_year


df_change_kommuner, earliest_year, latest_year = calculate_change(df_kommuner_raw)
df_change_fylke, earliest_year_fylke, latest_year_fylke = calculate_change(df_fylke_raw)
df_change_landet, earliest_year_landet, latest_year_landet = calculate_change(df_landet_raw)

# Sanity check: all three queries should cover the same year range (they all use top(5))
if not (earliest_year == earliest_year_fylke == earliest_year_landet
        and latest_year == latest_year_fylke == latest_year_landet):
    print(
        "Warning: Year ranges differ between kommune-, fylke- and landstall "
        f"(kommuner: {earliest_year}-{latest_year}, fylke: {earliest_year_fylke}-{latest_year_fylke}, "
        f"landet: {earliest_year_landet}-{latest_year_landet})."
    )

andel_col = f"Andel ({earliest_year}-{latest_year})"

for df_change in (df_change_kommuner, df_change_fylke, df_change_landet):
    df_change.rename(columns={"Andel": andel_col}, inplace=True)
    # Remove number prefix like "4001 " (municipalities/county); leaves "Norge" untouched
    df_change["Region"] = df_change["region"].str.replace(r"^\d+\s+", "", regex=True)
    df_change["Label"] = df_change["Region"]

print(f"Calculating change from {earliest_year} to {latest_year}")

# ============================================================
# Step 3: Combine municipalities, county and country into one
#         standard Everviz table
# ============================================================

df_final = pd.concat(
    [
        df_change_kommuner[["Region", andel_col, "Label"]],
        df_change_fylke[["Region", andel_col, "Label"]],
        df_change_landet[["Region", andel_col, "Label"]],
    ],
    ignore_index=True,
)

print(df_final)

# ============================================================
# Step 4: Save to CSV, compare and upload to GitHub
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
