import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import io
import sys

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data, delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

# ENTSO-E API Key
API_KEY = "5cf92fdd-a882-4158-a5a9-9b0b8e202786"
BASE_URL = "https://web-api.tp.entsoe.eu/api"

print("Script starting...")

# Step 1: Set up initial parameters and paths
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e"
file_name = "strompriser.csv"
print(f"Working with file: {github_folder}/{file_name}")

# Step 2: Download and process existing data from GitHub
existing_df = download_github_file(f"{github_folder}/{file_name}")
print("Checking for existing data...")
print(f"Type of existing_df: {type(existing_df)}")

# Initialize default date
latest_date = datetime(2021, 12, 1)  # Default start date

if existing_df is not None and not existing_df.empty:
    try:
        # Convert time column to datetime if it's not already
        existing_df["time"] = pd.to_datetime(existing_df["time"])
        latest_date = existing_df["time"].max()
        print(f"Latest date in existing data: {latest_date}")
        print(f"Found {len(existing_df)} existing records")
    except Exception as e:
        print(f"Error processing dates: {e}")
        existing_df = pd.DataFrame()
        print(f"Using default start date: {latest_date}")
else:
    print("No existing data found")
    existing_df = pd.DataFrame()
    print(f"Using default start date: {latest_date}")

# Step 3: Set up date range for new data
start_date = latest_date + timedelta(days=1)
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

# Check if we already have data up to yesterday
is_up_to_date = start_date.date() > end_date.date()
if is_up_to_date:
    print("All prices are already up to date (up until yesterday), no new data to fetch.")
    daily_avg = existing_df.copy()
else:
    print(f"Will fetch data from {start_date} to {end_date} (yesterday)")

    # Step 4: Prepare parameters for ENTSO-E API
    domain = "10YNO-2--------T"  # NO2 bidding zone (Southern Norway)
    params = {
        "documentType": "A44",
        "in_Domain": domain,
        "out_Domain": domain,
        "periodStart": start_date.strftime("%Y%m%d") + "0000",
        "periodEnd": end_date.strftime("%Y%m%d") + "0000",
        "securityToken": API_KEY,
    }

    # Step 5: Fetch energy prices from ENTSO-E
    print("Fetching energy prices from ENTSO-E...")
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        print("Successfully received response from ENTSO-E")
    except requests.exceptions.RequestException as e:
        error_msg = f"Error connecting to ENTSO-E API: {str(e)}"
        print(error_msg)
        error_messages.append(error_msg)
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError("Failed to fetch data from ENTSO-E")

    # Step 6: Parse XML response
    print("Parsing XML response...")
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
    root = ET.fromstring(response.content)
    price_data = []

    timeseries = root.findall(".//ns:TimeSeries", ns)
    print(f"Found {len(timeseries)} TimeSeries elements")

    for ts in timeseries:
        periods = ts.findall(".//ns:Period", ns)
        for period in periods:
            start = period.find(".//ns:start", ns).text
            print(f"Processing period starting at {start}")
            
            points = period.findall(".//ns:Point", ns)
            print(f"Found {len(points)} Point elements")
            
            for point in points:
                position = int(point.find("ns:position", ns).text)
                price = float(point.find("ns:price.amount", ns).text)
                
                # Convert time (remove Z and parse)
                time = (datetime.fromisoformat(start[:-1]) + 
                       timedelta(hours=position-1))
                
                price_data.append({
                    "time": time,
                    "price_eur": price
                })

    if not price_data:
        error_msg = "No price data found in the API response"
        print(error_msg)
        error_messages.append(error_msg)
        notify_errors(error_messages, script_name=script_name)
        raise RuntimeError("No price data available")

    print(f"Total price data points collected: {len(price_data)}")
    df_prices = pd.DataFrame(price_data)

    # Convert time to datetime and get just the date part for filtering
    df_prices["date"] = pd.to_datetime(df_prices["time"]).dt.date

    # Filter out dates that are already in the existing dataset
    if not existing_df.empty:
        existing_dates = pd.to_datetime(existing_df["time"]).dt.date
        df_prices = df_prices[~df_prices["date"].isin(existing_dates)]
        print(f"After filtering out existing dates, {len(df_prices)} price records remain")

    if df_prices.empty:
        print("No new price data to process after filtering")
        daily_avg = existing_df.copy()
    else:
        # Drop the temporary date column used for filtering
        df_prices = df_prices.drop(columns=["date"])

        # Step 7: Fetch exchange rates
        print("Fetching exchange rates...")
        try:
            date_str = start_date.strftime("%Y-%m-%d")
            url = f"https://data.norges-bank.no/api/data/EXR/B.EUR.NOK.SP?format=csv&startPeriod={date_str}"
            df_rates = pd.read_csv(url, sep=";")
            
            # Clean and prepare exchange rates data
            df_rates["TIME_PERIOD"] = pd.to_datetime(df_rates["TIME_PERIOD"])
            df_rates = df_rates.rename(columns={
                "TIME_PERIOD": "time",
                "OBS_VALUE": "eur_nok_rate"
            })
            df_rates = df_rates[["time", "eur_nok_rate"]]
        except:
            print("Could not fetch new exchange rates, using latest available rate...")
            # Get the latest exchange rate from existing data
            if not existing_df.empty:
                latest_rate = existing_df.iloc[-1]["kurs"]
                # Create a dataframe with the latest rate for all new dates
                dates = pd.date_range(start=start_date, end=end_date, freq='D')
                df_rates = pd.DataFrame({
                    'time': dates,
                    'eur_nok_rate': [latest_rate] * len(dates)
                })
            else:
                error_msg = "No existing data to get latest exchange rate from"
                print(error_msg)
                error_messages.append(error_msg)
                notify_errors(error_messages, script_name=script_name)
                raise RuntimeError(error_msg)

        # Step 8: Merge prices with exchange rates
        print("Merging price and exchange rate data...")
        df = pd.merge(df_prices, df_rates, on="time", how="left")
        df["eur_nok_rate"] = df["eur_nok_rate"].fillna(method="ffill")
        df["price_nok"] = df["price_eur"] * df["eur_nok_rate"]

        # Step 9: Calculate daily averages
        print("Calculating daily averages...")
        df["date"] = pd.to_datetime(df["time"]).dt.date
        daily_avg_new = df.groupby("date").agg({
            "price_eur": "mean",
            "eur_nok_rate": "mean",
            "price_nok": "mean"
        }).reset_index()

        # Format the new data
        print(f"Columns before renaming: {daily_avg_new.columns.tolist()}")
        daily_avg_new = daily_avg_new.rename(columns={
            "date": "time",
            "price_eur": "EUR/MWh",
            "eur_nok_rate": "kurs",
            "price_nok": "NOK/MWh"
        })
        daily_avg_new["NOK/KWh"] = daily_avg_new["NOK/MWh"] / 1000
        daily_avg_new["time"] = pd.to_datetime(daily_avg_new["time"])

        # Combine with existing data
        print("Combining with existing data...")
        if not existing_df.empty:
            print(f"Columns in existing_df: {existing_df.columns.tolist()}")
            print(f"Columns in daily_avg_new: {daily_avg_new.columns.tolist()}")
            daily_avg = pd.concat([existing_df, daily_avg_new], ignore_index=True)
            print(f"Combined data now has {len(daily_avg)} records")
        else:
            daily_avg = daily_avg_new

        # Final sorting
        daily_avg = daily_avg.sort_values("time")

# Step 13: Save (and upload to GitHub if new data)

# Define parameters for handle_output_data
file_name = "strompriser.csv"
task_name = "Klima og energi - Strompriser"
github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e"
temp_folder = os.environ.get("TEMP_FOLDER")

# Call handle_output_data function
is_new_data = handle_output_data(daily_avg, file_name, github_folder, temp_folder, keepcsv=True)

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
print("Script completed successfully!")
