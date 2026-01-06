"""Download and restructure NACE classification data from SSB Klass API.

This script downloads the 'Standard for næringsgruppering' (NACE classification)
from Statistics Norway's Klass API, restructures it into a hierarchical format
with one row per 5-digit code, and uploads it to GitHub if changes are detected.
"""

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
    """Download NACE classification data from SSB Klass API.
    
    Args:
        classification_id: SSB Klass classification ID (6 = Standard for næringsgruppering)
        from_date: Start date for valid codes (YYYY-MM-DD format)
        language: Language code (nb = Norwegian Bokmål)
        csv_separator: CSV delimiter used by the API
    
    Returns:
        DataFrame with raw NACE classification data from SSB
    """
    # Construct API URL with parameters
    base_url = f"https://data.ssb.no/api/klass/v1/classifications/{classification_id}/codes.csv"
    url = (
        f"{base_url}?from={from_date}"
        f"&language={language}"
        f"&csvSeparator={csv_separator}"
    )

    # Fetch data from SSB Klass API (ISO-8859-1 encoding for Norwegian characters)
    df = fetch_data(
        url=url,
        payload=None,
        error_messages=error_messages,
        query_name="SSB Klass - Standard for næringsgruppering",
        response_type="csv",
        delimiter=csv_separator,
        encoding="ISO-8859-1",
    )

    # Clean data: convert all columns to strings and replace 'nan' with empty strings
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().replace("nan", "")

    return df


def restructure_nace_data(df: pd.DataFrame) -> pd.DataFrame:
    """Restructure NACE data to one row per 5-digit code with full hierarchy.
    
    Transforms the raw NACE data (which has multiple rows per code at different levels)
    into a format where each row represents a 5-digit code with all parent levels
    (Nace_1 through Nace_5) as separate columns.
    
    Args:
        df: Raw NACE classification DataFrame from SSB
    
    Returns:
        Restructured DataFrame with one row per 5-digit code and hierarchical columns
    """
    # Filter to only level 5 (5-digit codes)
    df_5digit = df[df["level"] == "5"].copy()
    
    # Create lookup dictionary for all codes and their information
    code_to_info = {}
    for _, row in df.iterrows():
        code_to_info[row["code"]] = row.to_dict()
    
    result_rows = []
    
    # Build restructured rows by tracing up the hierarchy for each 5-digit code
    for _, row in df_5digit.iterrows():
        new_row = {}
        
        # Level 5: 5-digit code (e.g., 01.110)
        new_row["Nace_5_nr"] = row["code"]
        new_row["Nace_5_navn"] = row["name"]
        
        current_code = row["code"]
        parent_code = row["parentCode"]
        
        # Level 4: 4-digit code (e.g., 01.11)
        if parent_code in code_to_info:
            new_row["Nace_4_nr"] = parent_code
            new_row["Nace_4_navn"] = code_to_info[parent_code]["name"]
            parent_code = code_to_info[parent_code]["parentCode"]
        else:
            new_row["Nace_4_nr"] = ""
            new_row["Nace_4_navn"] = ""
        
        # Level 3: 3-digit code (e.g., 01.1)
        if parent_code in code_to_info:
            new_row["Nace_3_nr"] = parent_code
            new_row["Nace_3_navn"] = code_to_info[parent_code]["name"]
            parent_code = code_to_info[parent_code]["parentCode"]
        else:
            new_row["Nace_3_nr"] = ""
            new_row["Nace_3_navn"] = ""
        
        # Level 2: 2-digit code (e.g., 01)
        if parent_code in code_to_info:
            new_row["Nace_2_nr"] = parent_code
            new_row["Nace_2_navn"] = code_to_info[parent_code]["name"]
            parent_code = code_to_info[parent_code]["parentCode"]
        else:
            new_row["Nace_2_nr"] = ""
            new_row["Nace_2_navn"] = ""
        
        # Level 1: Letter code (e.g., A)
        if parent_code in code_to_info:
            new_row["Nace_1_nr"] = parent_code
            new_row["Nace_1_navn"] = code_to_info[parent_code]["name"]
        else:
            new_row["Nace_1_nr"] = ""
            new_row["Nace_1_navn"] = ""
        
        result_rows.append(new_row)
    
    # Convert list of dictionaries to DataFrame
    df_result = pd.DataFrame(result_rows)
    
    # Reorder columns to show hierarchy from level 1 to level 5
    column_order = [
        "Nace_1_nr", "Nace_1_navn",
        "Nace_2_nr", "Nace_2_navn",
        "Nace_3_nr", "Nace_3_navn",
        "Nace_4_nr", "Nace_4_navn",
        "Nace_5_nr", "Nace_5_navn"
    ]
    df_result = df_result[column_order]
    
    # Clean all columns: convert to strings and replace 'nan' with empty strings
    for col in df_result.columns:
        df_result[col] = df_result[col].astype(str).str.strip().replace("nan", "")
    
    # Create combined columns (code + name) for each level
    # Level 1 uses " - " separator, levels 2-5 use space separator
    df_result["Nace_1_nr_navn"] = df_result.apply(
        lambda row: f"{row['Nace_1_nr']} - {row['Nace_1_navn']}" if row['Nace_1_nr'] and row['Nace_1_navn'] else "",
        axis=1
    )
    df_result["Nace_2_nr_navn"] = df_result.apply(
        lambda row: f"{row['Nace_2_nr']} {row['Nace_2_navn']}" if row['Nace_2_nr'] and row['Nace_2_navn'] else "",
        axis=1
    )
    df_result["Nace_3_nr_navn"] = df_result.apply(
        lambda row: f"{row['Nace_3_nr']} {row['Nace_3_navn']}" if row['Nace_3_nr'] and row['Nace_3_navn'] else "",
        axis=1
    )
    df_result["Nace_4_nr_navn"] = df_result.apply(
        lambda row: f"{row['Nace_4_nr']} {row['Nace_4_navn']}" if row['Nace_4_nr'] and row['Nace_4_navn'] else "",
        axis=1
    )
    df_result["Nace_5_nr_navn"] = df_result.apply(
        lambda row: f"{row['Nace_5_nr']} {row['Nace_5_navn']}" if row['Nace_5_nr'] and row['Nace_5_navn'] else "",
        axis=1
    )
    
    # Reorder columns to include combined columns after each pair
    final_column_order = [
        "Nace_1_nr", "Nace_1_navn", "Nace_1_nr_navn",
        "Nace_2_nr", "Nace_2_navn", "Nace_2_nr_navn",
        "Nace_3_nr", "Nace_3_navn", "Nace_3_nr_navn",
        "Nace_4_nr", "Nace_4_navn", "Nace_4_nr_navn",
        "Nace_5_nr", "Nace_5_navn", "Nace_5_nr_navn"
    ]
    df_result = df_result[final_column_order]
    
    # Remove exact duplicates (all columns identical)
    rows_before = len(df_result)
    df_result = df_result.drop_duplicates()
    rows_after = len(df_result)
    duplicates_removed = rows_before - rows_after
    
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} exact duplicate rows.")
    else:
        print("No exact duplicate rows found.")
    
    # Remove near-duplicates (same Nace_5_nr but different Nace_5_navn)
    # This handles cases where SSB has multiple versions of the same code with slightly different text
    duplicates_before = df_result.duplicated(subset=["Nace_5_nr"], keep="first").sum()
    if duplicates_before > 0:
        print(f"Found {duplicates_before} near-duplicate rows with same Nace_5_nr but different Nace_5_navn.")
        
        # Show top 10 near-duplicates being removed for inspection
        duplicate_mask = df_result.duplicated(subset=["Nace_5_nr"], keep="first")
        duplicates_to_remove = df_result[duplicate_mask].head(10)
        
        print("\nTop 10 near-duplicate rows being removed:")
        print("-" * 80)
        for idx, row in duplicates_to_remove.iterrows():
            print(f"Nace_5_nr: {row['Nace_5_nr']}")
            print(f"Nace_5_navn: {row['Nace_5_navn']}")
            print("-" * 80)
        
        # Keep first instance of each Nace_5_nr, remove subsequent duplicates
        df_result = df_result.drop_duplicates(subset=["Nace_5_nr"], keep="first")
        print(f"\nKept first instance of each Nace_5_nr, removed {duplicates_before} near-duplicates.")
    else:
        print("No near-duplicates found based on Nace_5_nr.")
    
    # Verify that all Nace_5_nr values are unique
    unique_nace5 = df_result["Nace_5_nr"].nunique()
    total_rows = len(df_result)
    print(f"Final dataset: {total_rows} rows with {unique_nace5} unique Nace_5_nr values.")
    
    if unique_nace5 == total_rows:
        print("✓ Verification passed: All Nace_5_nr values are unique.")
    else:
        print(f"⚠ Warning: {total_rows - unique_nace5} duplicate Nace_5_nr values still exist!")
    
    return df_result


# Main execution
# Download raw NACE data from SSB Klass API
df_raw = download_standard_for_naeringsgruppering()

# Restructure data to hierarchical format with one row per 5-digit code
df_standard_for_naeringsgruppering = restructure_nace_data(df_raw)

# Configure output settings
file_name = "standard_for_naeringsgruppering.csv"
task_name = "Arbeid og naeringsliv - Standard for naeringsgruppering"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Virksomheter/Nace-tabell"
temp_folder = os.environ.get("TEMP_FOLDER")

# Compare with existing data on GitHub and upload if changes detected
is_new_data = handle_output_data(
    df_standard_for_naeringsgruppering,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
)

# Write status log for tracking data updates
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

# Report results
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
