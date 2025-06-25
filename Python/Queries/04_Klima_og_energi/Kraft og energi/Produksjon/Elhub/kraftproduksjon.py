import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import time

from Helper_scripts.github_functions import handle_output_data, download_github_file

# List of Telemark municipalities
TELEMARK_MUNICIPALITIES = [
    '4001', '4003'
]

# Get the GITHUB_TOKEN from the environment
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not found in the loaded .env file.")

print(f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')} GITHUB_TOKEN loaded successfully.")

def get_timestamp():
    """Get current timestamp for logging"""
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def query_elhub_municipality(municipality_id, start_date, end_date):
    """Query Elhub API for a specific municipality and date range"""
    url = f"https://api.elhub.no/energy-data/v0/municipalities/{municipality_id}?dataset=INSTALLED_CAPACITY_PER_METERING_POINT_TYPE_GROUP_MUNICIPALITY_DAILY&startDate={start_date}&endDate={end_date}"
    
    headers = {"Accept": "application/json"}
    print(f"{get_timestamp()} Querying URL: {url}")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"{get_timestamp()} Error: Status code {response.status_code}")
        print(f"{get_timestamp()} Response: {response.text}")
        return None
        
    print(f"{get_timestamp()} Got successful response")
    
    # Return the raw JSON for inspection
    return response.json()

def extract_data(json_data):
    """Extract data from the API response"""
    if not json_data or 'data' not in json_data:
        print(f"{get_timestamp()} No data found in response")
        return pd.DataFrame()
    
    # Print the structure of the response
    print(f"{get_timestamp()} Response keys: {list(json_data.keys())}")
    
    if len(json_data['data']) > 0:
        print(f"{get_timestamp()} First data item keys: {list(json_data['data'][0].keys())}")
        
        if 'attributes' in json_data['data'][0]:
            print(f"{get_timestamp()} First attributes keys: {list(json_data['data'][0]['attributes'].keys())}")
    
    # Extract records
    records = []
    
    for item in json_data['data']:
        if not isinstance(item, dict) or 'attributes' not in item:
            continue
            
        # Extract municipality info
        muni_id = item.get('id')
        attributes = item.get('attributes', {})
        muni_name = attributes.get('name', '')
        
        # Extract daily data points
        daily_data = attributes.get('installedCapacityPerMeteringPointTypeGroupMunicipalityDaily', [])
        
        if not daily_data:
            print(f"{get_timestamp()} No daily data found for municipality {muni_id}")
            continue
            
        print(f"{get_timestamp()} Found {len(daily_data)} data points for {muni_name} ({muni_id})")
        
        # Process each daily data entry
        for entry in daily_data:
            if not isinstance(entry, dict):
                continue
                
            # Format date from usageDateId
            usage_date_id = entry.get('usageDateId')
            if usage_date_id:
                date_str = str(usage_date_id)
                if len(date_str) == 8:
                    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                else:
                    formatted_date = None
            else:
                formatted_date = None
            
            # Create record
            record = {
                'Kommunenummer': entry.get('municipalityId', muni_id),
                'Kommune': muni_name,
                'MeteringPointTypeCode': entry.get('meteringPointTypeCode', ''),
                'Kilde': entry.get('productionGroup', ''),
                'InstallertKapasitet': entry.get('installedCapacity', 0),
                'Dato': formatted_date,
                'SistOppdatert': entry.get('lastUpdatedTime')
            }
            records.append(record)
    
    if not records:
        print(f"{get_timestamp()} No records extracted")
        return pd.DataFrame()
    
    # Create DataFrame
    df = pd.DataFrame(records)
    print(f"{get_timestamp()} Extracted {len(df)} records")
    
    # Print sample data
    print("\nSample data:")
    print(df.head().to_string())
    
    return df

def main():
    """Main function to query and aggregate data for all Telemark municipalities, appending only new data since the latest date in installed_capacity.csv on Github."""
    print(f"{get_timestamp()} Starting Elhub query for all Telemark municipalities (incremental mode)")

    temp_folder = os.environ.get("TEMP_FOLDER", ".")
    github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Elhub/Installert_kapasitet"
    combined_file_name = "installed_capacity.csv"
    github_combined_path = os.path.join(github_folder, combined_file_name).replace("\\", "/")
    print(f"{get_timestamp()} Checking GitHub path: {github_combined_path}")

    # 1. Download/read existing installed_capacity.csv from Github
    try:
        existing_df = download_github_file(github_combined_path)
    except Exception as e:
        print(f"{get_timestamp()} Could not download existing installed_capacity.csv from Github: {e}")
        existing_df = None

    # 2. Find latest date in 'Dato'
    latest_date = None
    if existing_df is not None and not existing_df.empty and 'Dato' in existing_df.columns:
        try:
            existing_df['Dato'] = pd.to_datetime(existing_df['Dato'])
            latest_date = existing_df['Dato'].max()
            print(f"{get_timestamp()} Latest date found in existing installed_capacity.csv: {latest_date.date()}")
            query_start_date = (latest_date + pd.Timedelta(days=1)).date()
            print(f"{get_timestamp()} Will query for data from: {query_start_date}")
        except Exception as e:
            print(f"{get_timestamp()} Error parsing 'Dato' in existing file: {e}")
            query_start_date = None
    else:
        print(f"{get_timestamp()} No existing installed_capacity.csv found or file is empty. Will query all data.")
        query_start_date = None

    # 3. Determine months/years to query
    current_year = 2025
    current_month = 6  # June (as of current local time 2025-06-25)
    years = [2024, 2025]
    months_by_year = {
    yearly_results = {2024: pd.DataFrame(), 2025: pd.DataFrame()}

    # 4. Query only for months after the latest date
    for municipality_id in TELEMARK_MUNICIPALITIES:
        print(f"\n{get_timestamp()} Querying municipality {municipality_id} for new data...")
        for year in years:
            for m in range(1, 13):
                # Determine if we should start from this year/month
                if year < start_year or (year == start_year and m < start_month):
                    continue
                if year > end_year or (year == end_year and m > end_month):
                    break
                # Calculate start and end of month
                month_first_date = datetime(year, m, 1).date()
                # If this is the first month and year, use the actual query_start_date (may be mid-month)
                if year == start_year and m == start_month:
                    start_date = query_start_date
                else:
                    start_date = month_first_date
                # Calculate end of month
                if m == 12:
                    end_date = datetime(year, 12, 31).date()
                else:
                    next_month = datetime(year, m, 1) + timedelta(days=32)
                    end_date = (datetime(next_month.year, next_month.month, 1) - timedelta(days=1)).date()
                print(f"{get_timestamp()} Querying municipality {municipality_id} for period {start_date} to {end_date}")
                json_data = query_elhub_municipality(municipality_id, str(start_date), str(end_date))
                if json_data:
                    print(f"{get_timestamp()} Extracting data from response for municipality {municipality_id}...")
                    df = extract_data(json_data)
                    if not df.empty:
                        yearly_results[year] = pd.concat([yearly_results[year], df], ignore_index=True)
                        print(f"{get_timestamp()} Added {len(df)} records from {municipality_id} for {start_date} to {end_date}")
                    else:
                        print(f"{get_timestamp()} No data found for {municipality_id} for {start_date} to {end_date}")
                else:
                    print(f"{get_timestamp()} Failed to get data for {municipality_id} for {start_date} to {end_date}")
                time.sleep(1)

    # 5. Combine new data with existing data
    combined_df = pd.DataFrame()
    for year in years:
        df_year = yearly_results[year]
        if not df_year.empty:
            # Sort by Dato
            if 'Dato' in df_year.columns:
                df_year = df_year.sort_values('Dato')
            combined_df = pd.concat([combined_df, df_year], ignore_index=True)

    if existing_df is not None and not existing_df.empty:
        combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
    if 'Dato' in combined_df.columns:
        combined_df['Dato'] = pd.to_datetime(combined_df['Dato'], errors='coerce')
        combined_df = combined_df.drop_duplicates(subset=['Kommunenummer', 'Dato', 'MeteringPointTypeCode', 'Kilde'], keep='last')
        combined_df = combined_df.sort_values('Dato')

    # 6. Save and upload only the combined file
    if not combined_df.empty:
        handle_output_data(
            combined_df,
            file_name=combined_file_name,
            github_folder=github_folder,
            temp_folder=temp_folder,
            keepcsv=True
        )
        print(f"\n{get_timestamp()} Combined file processed and uploaded via handle_output_data")
        print(f"{get_timestamp()} Total records in combined file: {len(combined_df)}")
    else:
        print(f"\n{get_timestamp()} No new or existing data to save.")
    print(f"\n{get_timestamp()} Query completed")

if __name__ == "__main__":
    main()
