import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
import re
import glob

# Configuration
YEARS_TO_PROCESS = [2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]  # Add years you want to process

# Dictionary of year-specific URLs
hovedtall_yearly = {
    "2024": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2024",
    "2023": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2023",
    "2022": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2022",
    "2021": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2021",
    "2020": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2020",
    "2019": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet%202019-copy",
    "2018": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet4",
    "2017": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet3",
    "2016": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet2",
    "2015": "https://www.nav.no/no/nav-og-samfunn/statistikk/aap-nedsatt-arbeidsevne-og-uforetrygd-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet1",
    "2014": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2014",
    "2013": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2013",
    "2012": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2012",
    "2011": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2011",
    #"2010": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-hovedtall-om-arbeidsmarkedet-2010",
    #"2008": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-2008-hovedtall-om-arbeidsmarkedet",
    #"2007": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-2007-hovedtall-om-arbeidsmarkedet",
    #"2006": "https://www.nav.no/no/nav-og-samfunn/statistikk/arbeidssokere-og-stillinger-statistikk/relatert-informasjon/arkiv-2006-hovedtall-om-arbeidsmarkedet"
}

# Create directory for CSV files if it doesn't exist
csv_dir = os.path.join(os.path.dirname(__file__), 'historiske_tall')
os.makedirs(csv_dir, exist_ok=True)

def find_county_column(df):
    """Find the column containing county names (contains 'Oslo')"""
    for col in df.columns:
        if df[col].astype(str).str.contains('Oslo').any():
            return col
    return None

def is_numeric_column(series):
    """Check if a column contains numeric values (including those with % signs)"""
    # Skip the first few rows as they might be headers
    data_rows = series.iloc[3:].copy()  # Skip first 3 rows which might be headers
    
    # Print first few values for debugging
    print(f"Checking column values (after headers): {data_rows.head().tolist()}")
    
    try:
        # Remove % signs and commas, handle negative numbers
        cleaned = data_rows.astype(str).str.replace('%', '').str.replace(',', '.').str.strip()
        # Handle negative numbers with spaces (e.g. "- 4")
        cleaned = cleaned.str.replace(r'-\s+', '-', regex=True)
        numeric_values = pd.to_numeric(cleaned, errors='coerce')
        
        # Count how many actual numeric values we have
        non_na_count = numeric_values.notna().sum()
        total_count = len(data_rows)
        
        # If at least 15% of non-NA values can be converted to numbers, consider it numeric
        # Also check if we have at least 5 numeric values to avoid false positives
        is_numeric = (non_na_count / total_count > 0.15 and non_na_count >= 5) if total_count > 0 else False
        
        print(f"Numeric values: {non_na_count}/{total_count} = {non_na_count/total_count if total_count > 0 else 0}")
        return is_numeric
    except Exception as e:
        print(f"Error checking numeric: {e}")
        return False

def should_include_column(df, col, year):
    """Determine if a column should be included based on content and header"""
    # Get the column header (usually in row 1)
    header = str(df[col].iloc[1]).lower() if len(df[col]) > 1 else ""
    
    # Check if it's a numeric column
    is_numeric = is_numeric_column(df[col])
    
    # For 2018, include both numeric columns and "Endring" columns (even if empty)
    if year == 2018 and "endring" in header:
        print(f"Including 'Endring' column for 2018 (numeric={is_numeric}): {col}")
        return True
    
    # For all other cases, only include if numeric
    return is_numeric

def find_numeric_columns(df, county_col, is_old_format=False, year=None):
    """Find columns containing numeric data"""
    numeric_cols = []
    
    # Get index of county column
    county_idx = df.columns.get_loc(county_col)
    
    print(f"\nLooking for numeric columns after column {county_col} (index {county_idx})")
    print(f"All columns: {df.columns.tolist()}")
    
    # Look at columns after the county column
    for col in df.columns[county_idx + 1:]:
        print(f"\nChecking column {col}:")
        print(f"Sample values: {df[col].dropna().head().tolist()}")
        
        if should_include_column(df, col, year):
            print(f"Including column: {col}")
            numeric_cols.append(col)
    
    # Sort columns based on their position in the dataframe
    sorted_cols = sorted(numeric_cols, key=lambda x: df.columns.get_loc(x))
    print("\nSorted columns:")
    print(sorted_cols)
    return sorted_cols

def clean_dataframe(df, year, month, sheet_name):
    """Clean the dataframe using a dynamic approach to find columns"""
    print(f"\nProcessing {month} {year}")
    
    # Print first few rows of raw data
    print("\nFirst few rows of raw data:")
    print(df.head())
    
    # Special handling for 2018 - use hardcoded column indices
    if year == 2018:
        print("\nUsing hardcoded columns for 2018")
        # Skip the first few rows which might be headers
        df = df.iloc[2:].copy()
        df = df.reset_index(drop=True)
        
        # Create new dataframe with standardized column names
        new_df = pd.DataFrame()
        
        # Map columns directly
        new_df['Fylke'] = df.iloc[:, 1]  # Column B
        new_df['Antall'] = pd.to_numeric(df.iloc[:, 3], errors='coerce')  # Column D
        new_df['Prosent av arbeidsstyrken'] = pd.to_numeric(df.iloc[:, 4], errors='coerce')  # Column E
        new_df['Endring fra i fjor (antall)'] = pd.to_numeric(df.iloc[:, 5], errors='coerce')  # Column F
        new_df['Endring fra i fjor (prosent)'] = pd.to_numeric(df.iloc[:, 6], errors='coerce')  # Column G
        new_df['Prosent av arbeidsstyrken i fjor'] = pd.to_numeric(df.iloc[:, 7], errors='coerce')  # Column H
        
        # Add year and month
        new_df['År'] = year
        new_df['Måned'] = month
        
        # Clean up the dataframe
        # Remove rows where Fylke is NaN or empty
        new_df = new_df[new_df['Fylke'].notna() & (new_df['Fylke'] != '')]
        
        # Replace any remaining NaN values with empty string
        new_df = new_df.fillna('')
        
        print("\nFinal dataframe sample:")
        print(new_df.head())
        
        return new_df
    
    # For older format (before October 2012), handle percentages differently
    is_old_format = "Tabell 2) Helt ledige fordelt p" in sheet_name
    is_3a_sheet = "3a. Helt ledige fylke" in sheet_name
    
    # Files from January 2015 and newer have 5 columns
    has_fifth_column = (year > 2015) or (year == 2015 and month.lower() != 'desember')
    
    # For old format, only use rows 4-25
    if is_old_format:
        df = df.iloc[3:25].copy()  # 0-based indexing, so 4-25 becomes 3-24
    else:
        # Skip the first few rows which might be headers
        df = df.iloc[2:].copy()
    
    df = df.reset_index(drop=True)
    
    # Drop completely empty columns
    df = df.dropna(axis=1, how='all')
    
    # Find the county column
    county_col = find_county_column(df)
    if county_col is None:
        raise ValueError("Could not find county column containing 'Oslo'")
    
    print(f"\nFound county column: {county_col}")
    print(f"Sample county values:\n{df[county_col].head()}")
    
    # Find numeric columns
    numeric_cols = find_numeric_columns(df, county_col, is_old_format, year)
    
    # Verify we have enough columns
    min_cols = 4  # Always require at least 4 columns
    if len(numeric_cols) < min_cols:
        raise ValueError(f"Expected at least {min_cols} numeric columns, found {len(numeric_cols)}")
    
    print(f"\nFound {len(numeric_cols)} numeric columns: {numeric_cols}")
    
    # Create new dataframe with standardized column names
    new_df = pd.DataFrame()
    
    # Copy county column
    new_df['Fylke'] = df[county_col]
    
    # Map numeric columns to standard names
    column_names = [
        'Antall',
        'Prosent av arbeidsstyrken',
        'Endring fra i fjor (antall)',
        'Endring fra i fjor (prosent)'
    ]
    
    # Add fifth column name if it exists in the data
    if has_fifth_column and len(numeric_cols) >= 5:
        column_names.append('Prosent av arbeidsstyrken i fjor')
    
    print("\nMapping columns:")
    # Copy and clean numeric columns
    for col_name, source_col in zip(column_names, numeric_cols):
        print(f"{col_name} <- {source_col}")
        print(f"Sample values before cleaning: {df[source_col].head()}")
        
        # Clean the values
        values = df[source_col].astype(str).str.replace(',', '.').str.strip()
        
        # If this is a percentage column
        if 'prosent' in col_name.lower():
            # Remove % sign if present and convert
            values = values.str.replace('%', '').str.strip()
            values = pd.to_numeric(values, errors='coerce')
            
            # Multiply by 100 for old format (Tabell 2) or if it's not the 3a sheet
            if is_old_format or not is_3a_sheet:
                values = values * 100
        else:
            # For non-percentage columns, just convert to numeric
            values = pd.to_numeric(values, errors='coerce')
            
        new_df[col_name] = values
        print(f"Sample values after cleaning: {new_df[col_name].head()}")
    
    # Add year and month
    new_df['År'] = year
    new_df['Måned'] = month
    
    # Clean up the dataframe
    # Remove rows where Fylke is NaN or empty
    new_df = new_df[new_df['Fylke'].notna() & (new_df['Fylke'] != '')]
    
    # Replace any remaining NaN values with empty string
    new_df = new_df.fillna('')
    
    print("\nFinal dataframe sample:")
    print(new_df.head())
    
    return new_df

def get_sheet_name(excel_file):
    """Get the correct sheet name based on available sheets"""
    # Get all sheet names
    sheets = pd.ExcelFile(excel_file).sheet_names
    print(f"Available sheets: {sheets}")
    
    # First try exact matches
    for sheet in sheets:
        if sheet in ["3a. Helt ledige fylke", "Tabell 2) Helt ledige fordelt p"]:
            print(f"Using new format sheet: {sheet}")
            return sheet
    
    # Then try partial matches
    for sheet in sheets:
        if "helt ledige" in sheet.lower() and "fylke" in sheet.lower():
            print(f"Using old format sheet: {sheet}")
            return sheet
    
    # If no match found, use first sheet
    print(f"No matching sheet found, using first sheet: {sheets[0]}")
    return sheets[0]

def is_hovedtall_link(text):
    """Check if link text indicates a Hovedtall document"""
    return ("Hovedtall" in text and 
            "delvis" not in text.lower() and 
            "permitterte" not in text.lower())

def extract_month_year(text, current_year):
    # First try to find month and year pattern
    month_year_match = re.search(r'(\w+)\s+\d{4}', text)
    if month_year_match:
        month = month_year_match.group(1)
        return month
            
    # If not found, try to find just month
    month_match = re.search(r'(\w+)\s*\(pdf\)', text)
    if month_match:
        month = month_match.group(1)
        return month
            
    return None

# Process each year
for year in YEARS_TO_PROCESS:
    print(f"\nProcessing year: {year}")
    
    # Convert year to string for dictionary lookup
    year_str = str(year)
    year_url = hovedtall_yearly.get(year_str)
    
    if year_url is None:
        print(f"Warning: No URL found for year {year}")
        continue
    
    year_response = requests.get(year_url)
    year_soup = BeautifulSoup(year_response.text, "html.parser")
    
    # Print all links for debugging
    print(f"\nFound links for {year}:")
    all_links = year_soup.find_all("a", href=True)
    
    # Process links based on year
    if int(year) >= 2022:
        # For 2022 onwards, look for direct XLS links
        for link in all_links:
            if is_hovedtall_link(link.text) and "(xls)" in link.text:
                print(f"Found XLS link: {link.text}")
                
                # Extract month from filename
                month = extract_month_year(link.text, year)
                if month:
                    # Get the XLS URL
                    xls_url = link["href"]
                    if not xls_url.startswith("http"):
                        xls_url = f"https://www.nav.no{xls_url}"
                    
                    # Download and process Excel file
                    xls_response = requests.get(xls_url)
                    xls_response.raise_for_status()
                    
                    # Read Excel file from memory with appropriate sheet name
                    excel_file = pd.ExcelFile(BytesIO(xls_response.content))
                    sheet_name = get_sheet_name(excel_file)
                    
                    df = pd.read_excel(BytesIO(xls_response.content), sheet_name=sheet_name)
                    
                    # Clean the dataframe
                    df = clean_dataframe(df, year, month, sheet_name)
                    
                    # Save as CSV
                    csv_filename = f"arbeidsledighet_{year}_{month}.csv"
                    csv_path = os.path.join(csv_dir, csv_filename)
                    df.to_csv(csv_path, index=False)
                    print(f"Processed and saved: {csv_filename}")
                    
                else:
                    print(f"Could not extract month from {link.text}")
    else:
        # For 2021 and earlier, look for PDF+XLS pairs
        print(f"\nDebug: Processing year {year}")
        print("All relevant links found:")
        for link in all_links:
            if "Hovedtall" in link.text:
                print(f"  {link.text} -> {link['href']}")
        
        # Now process PDF+XLS pairs
        for i, link in enumerate(all_links):
            if is_hovedtall_link(link.text) and "(pdf)" in link.text:
                print(f"\nFound PDF link: {link.text}")
                # Found a PDF link, check if next link is XLS
                if i + 1 < len(all_links):
                    next_link = all_links[i + 1]
                    print(f"Next link: {next_link.text}")
                    if "(xls)" in next_link.text:
                        xls_link = next_link
                        pdf_text = link.text
                        print(f"Found pair - PDF: {pdf_text}")
                        print(f"            XLS: {xls_link.text}")
                        print(f"            XLS href: {xls_link['href']}")
                        
                        # Extract month from the PDF link text (it's more complete)
                        month = extract_month_year(pdf_text, year)
                        if month:
                            # Get the XLS URL
                            xls_url = xls_link["href"]
                            if not xls_url.startswith("http"):
                                xls_url = f"https://www.nav.no{xls_url}"
                            
                            # Download and process Excel file
                            xls_response = requests.get(xls_url)
                            xls_response.raise_for_status()
                            
                            # Read Excel file from memory with appropriate sheet name
                            excel_file = pd.ExcelFile(BytesIO(xls_response.content))
                            sheet_name = get_sheet_name(excel_file)
                            
                            df = pd.read_excel(BytesIO(xls_response.content), sheet_name=sheet_name)
                            
                            # Clean the dataframe
                            df = clean_dataframe(df, year, month, sheet_name)
                            
                            # Save as CSV
                            csv_filename = f"arbeidsledighet_{year}_{month}.csv"
                            csv_path = os.path.join(csv_dir, csv_filename)
                            df.to_csv(csv_path, index=False)
                            print(f"Processed and saved: {csv_filename}")
                            
                        else:
                            print(f"Could not extract month from {pdf_text}")
                    else:
                        print(f"Warning: PDF link found but next link is not XLS")
                else:
                    print(f"Warning: PDF link found but it's the last link")

# After processing all files, concatenate them
print("\nConcatenating all CSV files...")
csv_files = glob.glob(os.path.join(csv_dir, f"arbeidsledighet_{YEARS_TO_PROCESS[0]}_*.csv"))
if len(YEARS_TO_PROCESS) > 1:
    for year in YEARS_TO_PROCESS[1:]:
        csv_files.extend(glob.glob(os.path.join(csv_dir, f"arbeidsledighet_{year}_*.csv")))
    
if csv_files:
    combined_df = pd.concat([pd.read_csv(f) for f in csv_files])
    combined_df.to_csv(os.path.join(csv_dir, "arbeidsledighet_all.csv"), index=False)
    print("Created combined CSV file: arbeidsledighet_all.csv")
else:
    print("No CSV files found to combine")
