import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import time

from Helper_scripts.github_functions import handle_output_data

# List of Telemark municipalities
TELEMARK_MUNICIPALITIES = [
    '4001', '4003'
]

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
    """Main function to query and aggregate data for all Telemark municipalities and all months in 2024 and 2025 (so far)."""
    print(f"{get_timestamp()} Starting Elhub query for all Telemark municipalities, all months in 2024 and 2025 so far")
    
    # Determine months to query for both years
    years = [2024, 2025]
    current_month_2025 = 6  # June (as of current local time 2025-06-25)
    months_by_year = {
        2024: list(range(1, 13)),
        2025: list(range(1, current_month_2025 + 1))
    }
    
    yearly_results = {2024: pd.DataFrame(), 2025: pd.DataFrame()}
    
    for municipality_id in TELEMARK_MUNICIPALITIES:
        print(f"\n{get_timestamp()} Querying municipality {municipality_id} for all months in 2024 and 2025 so far...")
        for year in years:
            for m in months_by_year[year]:
                start_date = f"{year}-{m:02d}-01"
                # Calculate end of month
                if m == 12:
                    end_date = f"{year}-12-31"
                else:
                    next_month = datetime(year, m, 1) + timedelta(days=32)
                    end_date = (datetime(next_month.year, next_month.month, 1) - timedelta(days=1)).strftime("%Y-%m-%d")
                
                print(f"{get_timestamp()} Querying municipality {municipality_id} for period {start_date} to {end_date}")
                json_data = query_elhub_municipality(municipality_id, start_date, end_date)
                
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
    
    # Save the results for each year
    temp_folder = os.environ.get("TEMP_FOLDER", ".")
    github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Elhub/Installert_kapasitet"
    combined_df = pd.DataFrame()
    for year in years:
        df_year = yearly_results[year]
        if not df_year.empty:
            # Sort by Dato
            if 'Dato' in df_year.columns:
                df_year = df_year.sort_values('Dato')
            file_name = f"installed_capacity_telemark_{year}.csv"
            # Use handle_output_data to save and upload
            handle_output_data(
                df_year,
                file_name=file_name,
                github_folder=github_folder,
                temp_folder=temp_folder,
                keepcsv=True
            )
            print(f"\n{get_timestamp()} Data for {year} processed and uploaded via handle_output_data")
            print(f"{get_timestamp()} Total records for {year}: {len(df_year)}")
            print(f"\nSummary by municipality for {year}:")
            summary = df_year.groupby('Kommune').agg({
                'InstallertKapasitet': 'sum',
                'Kommunenummer': 'first',
                'Dato': 'nunique'
            }).sort_values('InstallertKapasitet', ascending=False)
            print(summary.to_string())
            # Add to combined DataFrame
            combined_df = pd.concat([combined_df, df_year], ignore_index=True)
        else:
            print(f"\n{get_timestamp()} No data found for any municipality in {year}")
    # Save and upload combined file
    if not combined_df.empty:
        if 'Dato' in combined_df.columns:
            combined_df = combined_df.sort_values('Dato')
        combined_file_name = "installed_capacity.csv"
        handle_output_data(
            combined_df,
            file_name=combined_file_name,
            github_folder=github_folder,
            temp_folder=temp_folder,
            keepcsv=True
        )
        print(f"\n{get_timestamp()} Combined file processed and uploaded via handle_output_data")
        print(f"{get_timestamp()} Total records in combined file: {len(combined_df)}")
    print(f"\n{get_timestamp()} Query completed")

if __name__ == "__main__":
    main()
