import os
import pandas as pd

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

script_name = os.path.basename(__file__)

error_messages = []


def download_standard_for_naeringsgruppering(
    classification_id: int = 6,
    from_date: str = "2009-01-01",
    language: str = "nb",
    csv_separator: str = ";",
) -> pd.DataFrame:
    base_url = f"https://data.ssb.no/api/klass/v1/classifications/{classification_id}/codes.csv"
    url = (
        f"{base_url}?from={from_date}"
        f"&language={language}"
        f"&csvSeparator={csv_separator}"
    )

    df = fetch_data(
        url=url,
        payload=None,
        error_messages=error_messages,
        query_name="SSB Klass - Standard for næringsgruppering",
        response_type="csv",
        delimiter=csv_separator,
        encoding="ISO-8859-1",
    )

    df = df.copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().replace("nan", "")

    return df


def restructure_nace_data(df: pd.DataFrame) -> pd.DataFrame:
    df_5digit = df[df["level"] == "5"].copy()
    
    code_to_info = {}
    for _, row in df.iterrows():
        code_to_info[row["code"]] = row.to_dict()
    
    result_rows = []
    
    for _, row in df_5digit.iterrows():
        new_row = {}
        
        new_row["Nace_5_nr"] = row["code"]
        
        current_code = row["code"]
        parent_code = row["parentCode"]
        
        if parent_code in code_to_info:
            new_row["Nace_4_nr"] = parent_code
            parent_code = code_to_info[parent_code]["parentCode"]
        else:
            new_row["Nace_4_nr"] = ""
        
        if parent_code in code_to_info:
            new_row["Nace_3_nr"] = parent_code
            parent_code = code_to_info[parent_code]["parentCode"]
        else:
            new_row["Nace_3_nr"] = ""
        
        if parent_code in code_to_info:
            new_row["Nace_2_nr"] = parent_code
            parent_code = code_to_info[parent_code]["parentCode"]
        else:
            new_row["Nace_2_nr"] = ""
        
        if parent_code in code_to_info:
            new_row["Nace_1_nr"] = parent_code
        else:
            new_row["Nace_1_nr"] = ""
        
        for col in row.index:
            if col not in ["code", "parentCode", "level"]:
                new_row[col] = row[col]
        
        result_rows.append(new_row)
    
    df_result = pd.DataFrame(result_rows)
    
    df_result = df_result.rename(columns={
        "name": "Nace_5_navn",
        "notes": "Kommentar"
    })
    
    columns_to_drop = ["shortName", "presentationName"]
    df_result = df_result.drop(columns=[col for col in columns_to_drop if col in df_result.columns])
    
    column_order = ["Nace_1_nr", "Nace_2_nr", "Nace_3_nr", "Nace_4_nr", "Nace_5_nr"]
    other_cols = [col for col in df_result.columns if col not in column_order]
    df_result = df_result[column_order + other_cols]
    
    for col in df_result.columns:
        df_result[col] = df_result[col].astype(str).str.strip().replace("nan", "")
    
    return df_result


df_raw = download_standard_for_naeringsgruppering()
df_standard_for_naeringsgruppering = restructure_nace_data(df_raw)

file_name = "standard_for_naeringsgruppering.csv"
task_name = "Arbeid og naeringsliv - Standard for naeringsgruppering"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Virksomheter/Nace-tabell"
temp_folder = os.environ.get("TEMP_FOLDER")

is_new_data = handle_output_data(
    df_standard_for_naeringsgruppering,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
)

log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
