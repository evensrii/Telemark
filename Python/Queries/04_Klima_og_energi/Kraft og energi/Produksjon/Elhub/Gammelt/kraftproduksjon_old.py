import requests
import pandas as pd
from datetime import datetime, timedelta
import json
from dotenv import load_dotenv
import os
import sys
import time
import io
from base64 import b64encode
from dateutil.relativedelta import relativedelta
from urllib.parse import quote

# GitHub configuration
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Elhub/Solkraft"

# List of Telemark municipalities
TELEMARK_MUNICIPALITIES = [
    '4001', '4003', '4005', '4010', '4012', '4014', '4016', '4018',
    '4020', '4022', '4024', '4026', '4028', '4030', '4032', '4034', '4036'
]

from Helper_scripts.github_functions import handle_output_data
import pandas as pd

# Custom GitHub handler to interface with handle_output_data
def github_handler(action, path, content=None):
    """Custom GitHub handler to interface with handle_output_data
    
    Args:
        action (str): Action to perform: 'list', 'read', or 'write'
        path (str): Path to the file or directory
        content (str, optional): Content to write if action is 'write'
        
    Returns:
        list or str: List of files if action is 'list', file content if action is 'read'
    """
    try:
        if action == 'list':
            # For listing files, we need to create a temporary DataFrame
            # This is a workaround since handle_output_data expects a DataFrame
            print(f"{get_timestamp()} Listing files in {path}")
            
            # Create a dummy DataFrame with a single row
            dummy_df = pd.DataFrame([{'path': path}])
            
            # Use the compare_to_github function directly if available
            # Otherwise, we'll need to implement a custom solution
            try:
                # For now, simulate file listing with known patterns
                # In a real implementation, you would query GitHub API
                if "installed_capacity" in path:
                    # Return simulated yearly files
                    current_year = datetime.now().year
                    files = [f"{year}_installed_capacity.csv" for year in range(2020, current_year + 1)]
                    files.append("installed_capacity_telemark.csv")
                    return files
                return []
            except Exception as e:
                print(f"{get_timestamp()} Error listing files: {str(e)}")
                return []
            
        elif action == 'read':
            # For reading files, we need to create a temporary DataFrame
            print(f"{get_timestamp()} Reading file {path}")
            
            # Extract filename from path
            filename = path.split('/')[-1]
            
            # Create a dummy DataFrame to simulate file content
            # In a real implementation, you would read from GitHub
            try:
                # For now, return empty content
                # In a real implementation, you would query GitHub API
                return ""
            except Exception as e:
                print(f"{get_timestamp()} Error reading file {path}: {str(e)}")
                return ""
            
        elif action == 'write':
            # For writing files, we need to create a DataFrame from the content
            if content is None:
                raise ValueError("Content must be provided for 'write' action")
                
            print(f"{get_timestamp()} Writing to {path}")
            
            # Extract filename and directory from path
            filename = path.split('/')[-1]
            github_dir = '/'.join(path.split('/')[:-1])
            
            # Convert content to DataFrame if it's a CSV string
            if isinstance(content, str) and ',' in content:
                try:
                    # Parse CSV content into DataFrame
                    df = pd.read_csv(io.StringIO(content))
                    
                    # Use handle_output_data to write the DataFrame to GitHub
                    # This function handles the actual GitHub API calls
                    result = handle_output_data(df, filename, github_dir, temp_folder)
                    
                    print(f"{get_timestamp()} Successfully wrote {len(df)} rows to {filename}")
                    return result
                except Exception as e:
                    print(f"{get_timestamp()} Error converting content to DataFrame: {str(e)}")
                    import traceback
                    print(f"{get_timestamp()} Error details: {traceback.format_exc()}")
                    return False
            else:
                print(f"{get_timestamp()} Content is not a valid CSV string")
                return False
        else:
            raise ValueError(f"Invalid action: {action}")
    except Exception as e:
        print(f"{get_timestamp()} Error in github_handler: {str(e)}")
        import traceback
        print(f"{get_timestamp()} Error details: {traceback.format_exc()}")
        return None

# Get the GITHUB_TOKEN from the token.env file
pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    raise ValueError("PYTHONPATH environment variable is not set.")

# Construct the full path to the token.env
env_file_path = os.path.join(pythonpath, "token.env")
if not os.path.exists(env_file_path):
    raise ValueError(f"token.env file not found in: {env_file_path}")

# Load the .env file
load_dotenv(env_file_path)
print(f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')} Loaded .env file from: {env_file_path}")

# Get the GITHUB_TOKEN from the environment
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN not found in the loaded .env file.")

print(f"{datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')} GITHUB_TOKEN loaded successfully.")

# Add PYTHONPATH to sys.path
PYTHON_PATH = os.environ.get("PYTHONPATH")
if PYTHON_PATH not in sys.path:
    sys.path.append(PYTHON_PATH)

# Script parameters
task_name = "Klima og energi - Solkraftproduksjon (Elhub)"
temp_folder = os.environ.get("TEMP_FOLDER")
DATA_PATH = github_folder + "/"

# Function to get current timestamp
def get_timestamp():
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def query_elhub_municipality(municipality_id, start_date, end_date):
    """Query Elhub API for a specific municipality and date range"""
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    url = f"https://api.elhub.no/energy-data/v0/municipalities/{municipality_id}?dataset=INSTALLED_CAPACITY_PER_METERING_POINT_TYPE_GROUP_MUNICIPALITY_DAILY&startDate={start_str}&endDate={end_str}"
    
    headers = {"Accept": "application/json"}
    print(f"{get_timestamp()} Querying URL: {url}")
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"{get_timestamp()} Error: Status code {response.status_code} for municipality {municipality_id}")
        print(f"{get_timestamp()} Response: {response.text}")
        return pd.DataFrame()
        
    print(f"{get_timestamp()} Got successful response for {municipality_id} ({start_str} to {end_str})")
    
    try:
        # Parse JSON response
        json_data = response.json()
        
        if not json_data or 'data' not in json_data:
            print(f"{get_timestamp()} No data found in response for municipality {municipality_id}")
            return pd.DataFrame()
        
        # Print the structure of the response (limited to avoid excessive output)
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
            muni_name_no = attributes.get('nameNo', muni_name)  # Use Norwegian name if available
            
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
                    'Kommune': muni_name_no,
                    'MeteringPointTypeCode': entry.get('meteringPointTypeCode', ''),
                    'Kilde': entry.get('productionGroup', ''),
                    'InstallertKapasitet': entry.get('installedCapacity', 0),
                    'Dato': formatted_date,
                    'SistOppdatert': entry.get('lastUpdatedTime', json_data.get('meta', {}).get('lastUpdated'))
                }
                records.append(record)
        
        if not records:
            print(f"{get_timestamp()} No records extracted for municipality {municipality_id}")
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(records)
        print(f"{get_timestamp()} Extracted {len(df)} records for municipality {municipality_id}")
        
        # Convert date string to datetime
        if 'Dato' in df.columns:
            df['Dato'] = pd.to_datetime(df['Dato'])
            
        return df
        
    except Exception as e:
        print(f"{get_timestamp()} Error processing data for municipality {municipality_id}: {str(e)}")
        import traceback
        print(f"{get_timestamp()} Error details: {traceback.format_exc()}")
        return pd.DataFrame()

def get_latest_date():
    """Get the latest date from existing yearly CSV files on GitHub"""
    try:
        # List files in the GitHub directory
        files = github_handler('list', github_folder)
        if not files:
            print(f"{get_timestamp()} No files found in GitHub directory")
            return datetime(2020, 1, 1) - timedelta(days=1)  # Start from Jan 1, 2020 instead of 2016
        
        latest_date = None
        csv_files = [f for f in files if f.endswith(".csv") and "installed_capacity" in f]
        
        for file in csv_files:
            try:
                content = github_handler('read', f"{github_folder}/{file}")
                if content and isinstance(content, str):
                    # Properly convert string content to DataFrame using StringIO
                    df = pd.read_csv(io.StringIO(content))
                    if not df.empty and 'Dato' in df.columns:
                        # Convert date column to datetime for comparison
                        df['Dato'] = pd.to_datetime(df['Dato'])
                        year_latest = df['Dato'].max()
                        year = int(file.split('_')[0])  # Extract year from filename
                        if latest_date is None or year_latest > latest_date:
                            latest_date = year_latest
                            print(f"{get_timestamp()} Found data for {year} up to {year_latest.strftime('%Y-%m-%d')}")
            except Exception as e:
                print(f"{get_timestamp()} Error reading file {file}: {str(e)}")
                continue
        
        if latest_date:
            return latest_date
            
    except Exception as e:
        print(f"{get_timestamp()} Error checking existing data: {str(e)}")
        import traceback
        print(f"{get_timestamp()} Error details: {traceback.format_exc()}")
    
    print(f"{get_timestamp()} No existing data found, starting from January 2020")
    return datetime(2020, 1, 1) - timedelta(days=1)  # Start from Jan 1, 2020 instead of 2016

def process_data(df):
    """Process and clean the data from the API response"""
    if df.empty:
        print(f"{get_timestamp()} No data to process")
        return df
    
    print(f"{get_timestamp()} Processing {len(df)} records")
    print(f"{get_timestamp()} DataFrame columns: {df.columns.tolist()}")
    print(f"{get_timestamp()} Sample data: {df.head(1).to_dict('records')}")
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Convert energy source names to Norwegian
    kilde_map = {
        'hydro': 'Vann',
        'solar': 'Sol',
        'wind': 'Vind',
        'other': 'Annet',
        'thermal': 'Termisk'
    }
    
    # Apply mapping if the column exists
    if 'Kilde' in df.columns:
        df['Kilde'] = df['Kilde'].map(lambda x: kilde_map.get(x, x))
    
    # Handle date fields - they should already be in datetime format from query_elhub_municipality
    # But let's ensure they're properly formatted
    if 'Dato' in df.columns:
        # Check if Dato is already a datetime
        if not pd.api.types.is_datetime64_any_dtype(df['Dato']):
            # Try to convert to datetime if it's not already
            try:
                df['Dato'] = pd.to_datetime(df['Dato'])
            except Exception as e:
                print(f"{get_timestamp()} Error converting Dato to datetime: {str(e)}")
        
        # Format as string YYYY-MM-DD
        df['Dato'] = df['Dato'].dt.strftime('%Y-%m-%d')
    
    # Format SistOppdatert if it exists
    if 'SistOppdatert' in df.columns:
        try:
            df['SistOppdatert'] = pd.to_datetime(df['SistOppdatert']).dt.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"{get_timestamp()} Error formatting SistOppdatert: {str(e)}")
    
    # Filter for solar production only if specified
    if 'Kilde' in df.columns:
        solar_df = df[df['Kilde'] == 'Sol']
        if len(solar_df) < len(df):
            print(f"{get_timestamp()} Filtered {len(df) - len(solar_df)} non-solar records")
            df = solar_df
    
    # Sort the data
    if 'Dato' in df.columns and 'Kommunenummer' in df.columns:
        df = df.sort_values(["Dato", "Kommunenummer"])
    
    print(f"{get_timestamp()} Processed data: {len(df)} records")
    return df



# Function to save data to GitHub
def save_data_by_year(df):
    """Save data to separate yearly files and update the combined file"""
    if df.empty:
        print(f"{get_timestamp()} No data to save")
        return
    
    # Ensure Dato is in datetime format for processing
    df = df.copy()  # Avoid modifying the original dataframe
    if isinstance(df['Dato'].iloc[0], str):
        df['Dato'] = pd.to_datetime(df['Dato'])
    
    # Group and process data by year
    for year, year_df in df.groupby(df['Dato'].dt.year):
        print(f"\n{get_timestamp()} === Processing Year {year} ====")
        
        # Create year summary once
        date_range = (year_df['Dato'].min(), year_df['Dato'].max())
        print(f"{get_timestamp()} Date range: {date_range[0].strftime('%Y-%m-%d')} to {date_range[1].strftime('%Y-%m-%d')}")
        print(f"{get_timestamp()} Processing {len(year_df)} records for year {year}")
        
        # Format dates as strings for CSV
        save_df = year_df.copy()
        save_df['Dato'] = save_df['Dato'].dt.strftime('%Y-%m-%d')
        save_df = save_df.sort_values(['Dato', 'Kommunenummer'])
        
        # Save to yearly file
        filename = f"{year}_installed_capacity.csv"
        csv_content = save_df.to_csv(index=False, line_terminator='\n')
        
        try:
            # Save yearly file to GitHub
            github_path = f"{github_folder}/{filename}"
            if isinstance(csv_content, str):
                # Use our custom github_handler to write to GitHub
                github_handler('write', github_path, csv_content)
                
                # Save to temp folder if configured
                if temp_folder := os.environ.get("TEMP_FOLDER"):
                    temp_path = os.path.join(temp_folder, filename)
                    with open(temp_path, 'w', encoding='utf-8', newline='') as f:
                        f.write(csv_content)
                    print(f"{get_timestamp()} Saved to temp folder: {temp_path}")
                
                print(f"{get_timestamp()} Saved yearly file: {filename}")
                
                # Update combined file
                combined_filename = "installed_capacity_telemark.csv"
                combined_path = f"{github_folder}/{combined_filename}"
                
                try:
                    # Try to read existing combined file
                    existing_content = github_handler('read', combined_path)
                    if existing_content and isinstance(existing_content, str):
                        existing_df = pd.read_csv(io.StringIO(existing_content))
                        
                        # Check if the combined file has data
                        if not existing_df.empty:
                            print(f"{get_timestamp()} Found existing combined file with {len(existing_df)} records")
                            
                            # Convert date columns for comparison
                            existing_df['Dato'] = pd.to_datetime(existing_df['Dato'])
                            
                            # Create a unique key for deduplication
                            existing_df['unique_key'] = existing_df['Dato'].dt.strftime('%Y-%m-%d') + '_' + \
                                                        existing_df['Kommunenummer'].astype(str) + '_' + \
                                                        existing_df['Kilde']
                            
                            # Remove any existing data for this year to avoid duplicates
                            print(f"{get_timestamp()} Removing existing data for year {year} from combined file")
                            existing_df = existing_df[existing_df['Dato'].dt.year != year]
                            
                            # Convert back to string format for consistency
                            existing_df['Dato'] = existing_df['Dato'].dt.strftime('%Y-%m-%d')
                            
                            # Drop the temporary key
                            existing_df = existing_df.drop('unique_key', axis=1)
                        else:
                            print(f"{get_timestamp()} Existing combined file is empty")
                        
                        # Combine with new data
                        combined_df = pd.concat([existing_df, save_df], ignore_index=True)
                    else:
                        print(f"{get_timestamp()} No valid existing combined file found, creating new one")
                        combined_df = save_df
                        
                except Exception as e:
                    print(f"{get_timestamp()} Error reading existing combined file: {str(e)}")
                    combined_df = save_df
                
                # Final deduplication check
                if not combined_df.empty:
                    # Create a unique key for final deduplication
                    combined_df['unique_key'] = combined_df['Dato'] + '_' + \
                                               combined_df['Kommunenummer'].astype(str) + '_' + \
                                               combined_df['Kilde']
                    
                    # Drop duplicates based on the unique key
                    original_len = len(combined_df)
                    combined_df = combined_df.drop_duplicates(subset=['unique_key'])
                    if len(combined_df) < original_len:
                        print(f"{get_timestamp()} Removed {original_len - len(combined_df)} duplicate records")
                    
                    # Drop the temporary key
                    combined_df = combined_df.drop('unique_key', axis=1)
                
                # Sort the combined data
                combined_df = combined_df.sort_values(['Dato', 'Kommunenummer'])
                
                # Save combined file
                combined_csv = combined_df.to_csv(index=False, line_terminator='\n')
                github_handler('write', combined_path, combined_csv)
                
                # Save combined file to temp folder if configured
                if temp_folder:
                    combined_temp_path = os.path.join(temp_folder, combined_filename)
                    with open(combined_temp_path, 'w', encoding='utf-8', newline='') as f:
                        f.write(combined_csv)
                    print(f"{get_timestamp()} Updated combined file with {len(combined_df)} total records")
                
            else:
                print(f"{get_timestamp()} Error: Invalid CSV content type {type(csv_content)}, skipping save")
                
        except Exception as e:
            print(f"{get_timestamp()} Error saving data for year {year}: {str(e)}")
            import traceback
            print(f"{get_timestamp()} Error details: {traceback.format_exc()}")

def main():
    print(f"{get_timestamp()} Starting solar installed capacity data update")
    
    # Get the latest date from existing data
    latest_date = get_latest_date()
    start_date = latest_date + timedelta(days=1)
    
    # End at yesterday
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if start_date > yesterday:
        print(f"{get_timestamp()} Data is already up to date (latest: {start_date.strftime('%Y-%m-%d')}, today: {yesterday.strftime('%Y-%m-%d')})") 
        return
    
    print(f"{get_timestamp()} Will collect data from {start_date.strftime('%Y-%m-%d')} to {yesterday.strftime('%Y-%m-%d')}")
    
    # Generate list of months to process
    months_to_process = []
    current_date = start_date
    
    while current_date <= yesterday:
        # Get the first day of the month
        first_day = current_date.replace(day=1)
        
        # Get the last day of the month
        if first_day.month == 12:
            last_day = first_day.replace(year=first_day.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            last_day = first_day.replace(month=first_day.month + 1, day=1) - timedelta(days=1)
        
        # Ensure we don't go beyond yesterday
        last_day = min(last_day, yesterday)
        
        months_to_process.append((first_day, last_day))
        
        # Move to the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)
    
    print(f"{get_timestamp()} Will process {len(months_to_process)} months × {len(TELEMARK_MUNICIPALITIES)} municipalities = {len(months_to_process) * len(TELEMARK_MUNICIPALITIES)} queries\n")
    
    # Process each month
    all_data = []
    query_count = 0
    
    # Track data availability
    data_availability = {}
    for year in range(start_date.year, yesterday.year + 1):
        data_availability[year] = {
            'total_queries': 0,
            'queries_with_data': 0,
            'municipalities_with_data': set(),
            'total_records': 0
        }
    
    try:
        for i, (month_start, month_end) in enumerate(months_to_process):
            month_str = month_start.strftime("%Y-%m")
            year = month_start.year
            print(f"\n{get_timestamp()} === Processing Month {i+1}/{len(months_to_process)}: {month_str} ====")
            print(f"{get_timestamp()} Period: {month_start.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')}")
            
            month_data = []
            municipalities_with_data_this_month = set()
            
            # Query each municipality
            for municipality_id in TELEMARK_MUNICIPALITIES:
                query_count += 1
                data_availability[year]['total_queries'] += 1
                print(f"{get_timestamp()} [{query_count}/{len(months_to_process) * len(TELEMARK_MUNICIPALITIES)}] Querying municipality {municipality_id}...")
                
                # Try up to 3 times with exponential backoff
                max_retries = 3
                retry_delay = 2  # seconds
                
                for attempt in range(max_retries):
                    try:
                        df = query_elhub_municipality(municipality_id, month_start, month_end)
                        if not df.empty:
                            month_data.append(df)
                            data_availability[year]['queries_with_data'] += 1
                            data_availability[year]['municipalities_with_data'].add(municipality_id)
                            municipalities_with_data_this_month.add(municipality_id)
                            data_availability[year]['total_records'] += len(df)
                            print(f"{get_timestamp()} Found {len(df)} records for municipality {municipality_id}")
                        break  # Success, exit retry loop
                    except Exception as e:
                        if attempt < max_retries - 1:  # Not the last attempt
                            wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                            print(f"{get_timestamp()} Error querying municipality {municipality_id}, retrying in {wait_time}s: {str(e)}")
                            time.sleep(wait_time)
                        else:  # Last attempt
                            print(f"{get_timestamp()} Failed to query municipality {municipality_id} after {max_retries} attempts: {str(e)}")
                
                # Rate limiting - be nice to the API
                time.sleep(1)
            
            # Process month data
            if month_data:
                month_df = pd.concat(month_data, ignore_index=True)
                all_data.append(month_df)
                print(f"\n{get_timestamp()} Collected {len(month_df)} records for {month_str}")
                print(f"{get_timestamp()} Municipalities with data: {', '.join(sorted(municipalities_with_data_this_month))}")
            else:
                print(f"\n{get_timestamp()} No data found for {month_str}")
        
        # Print data availability summary
        print(f"\n{get_timestamp()} === DATA AVAILABILITY SUMMARY ====")
        for year, stats in data_availability.items():
            if stats['total_queries'] > 0:
                data_rate = (stats['queries_with_data'] / stats['total_queries']) * 100
                print(f"{get_timestamp()} Year {year}: {data_rate:.1f}% of queries returned data")
                print(f"{get_timestamp()} - {len(stats['municipalities_with_data'])}/{len(TELEMARK_MUNICIPALITIES)} municipalities have data")
                print(f"{get_timestamp()} - {stats['total_records']} total records")
                if stats['municipalities_with_data']:
                    print(f"{get_timestamp()} - Municipalities with data: {', '.join(sorted(stats['municipalities_with_data']))}")
        
        # Process and save all collected data
        if all_data:
            all_df = pd.concat(all_data, ignore_index=True)
            print(f"\n{get_timestamp()} Total records collected: {len(all_df)}")
            
            # Process data before saving
            processed_df = process_data(all_df)
            
            # Save data by year
            save_data_by_year(processed_df)
            
            print(f"\n{get_timestamp()} Successfully updated solar installed capacity data")
        else:
            print(f"\n{get_timestamp()} No data available")
            
    except Exception as e:
        print(f"{get_timestamp()} Error updating solar installed capacity data: {str(e)}")
        raise
    
    print(f"{get_timestamp()} Completed solar installed capacity data update")

if __name__ == "__main__":
    main()