import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import fetch_data, delete_files_in_temp_folder
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# ENTSO-E API Key
API_KEY = "5cf92fdd-a882-4158-a5a9-9b0b8e202786"
BASE_URL = "https://web-api.tp.entsoe.eu/api"

def fetch_energy_prices(period_start, period_end):
    """
    Fetch energy prices from the ENTSO-E API.
    
    Args:
        period_start (str): Start date in YYYYMMDD format
        period_end (str): End date in YYYYMMDD format
        
    Returns:
        pandas.DataFrame: DataFrame with energy prices
    """
    # NO2 bidding zone (Southern Norway)
    domain = "10YNO-2--------T"
    
    params = {
        "documentType": "A44",
        "in_Domain": domain,
        "out_Domain": domain,
        "periodStart": period_start + "0000",
        "periodEnd": period_end + "0000",
        "securityToken": API_KEY,
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        
        # Parse XML response
        root = ET.fromstring(response.content)
        
        data = []
        for timeseries in root.findall(".//TimeSeries"):
            currency = timeseries.find(".//currency_Unit.name").text
            for period in timeseries.findall(".//Period"):
                start = period.find("timeInterval/start").text
                for point in period.findall("Point"):
                    position = int(point.find("position").text)
                    price = float(point.find("price.amount").text)
                    
                    # Convert time
                    time = (datetime.strptime(start, "%Y-%m-%dT%H:%MZ") + 
                           timedelta(hours=position-1))
                    
                    data.append({
                        "time": time,
                        f"price_{currency.lower()}": price
                    })
        
        return pd.DataFrame(data)
    
    except Exception as e:
        print(f"Error fetching energy prices: {str(e)}")
        notify_errors(script_name, str(e))
        return None

def fetch_exchange_rates(start_date):
    """
    Fetch EUR to NOK exchange rates.
    
    Args:
        start_date (datetime): Start date for exchange rates
        
    Returns:
        pandas.DataFrame: DataFrame with exchange rates
    """
    try:
        # Format date for Norges Bank API
        date_str = start_date.strftime("%Y-%m-%d")
        
        # Fetch exchange rates from Norges Bank
        url = f"https://data.norges-bank.no/api/data/EXR/B.EUR.NOK.SP?format=csv&startPeriod={date_str}"
        df_rates = pd.read_csv(url, sep=";")
        
        # Clean and prepare exchange rates data
        df_rates["TIME_PERIOD"] = pd.to_datetime(df_rates["TIME_PERIOD"])
        df_rates = df_rates.rename(columns={
            "TIME_PERIOD": "time",
            "OBS_VALUE": "eur_nok_rate"
        })
        
        return df_rates[["time", "eur_nok_rate"]]
    
    except Exception as e:
        print(f"Error fetching exchange rates: {str(e)}")
        notify_errors(script_name, str(e))
        return None

def get_latest_date_from_github(file_path):
    """
    Get the latest date from existing data in GitHub.
    
    Args:
        file_path (str): Path to the file in GitHub
        
    Returns:
        datetime: Latest date in the data, or None if no data exists
    """
    content = download_github_file(file_path)
    if content:
        df = pd.read_csv(io.StringIO(content))
        df["time"] = pd.to_datetime(df["time"])
        return df["time"].max()
    return None

# Main execution
try:
    # Get the latest date from GitHub data
    github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e"
    file_name = "strompriser.csv"
    latest_date = get_latest_date_from_github(f"{github_folder}/{file_name}")
    
    # Set date range for new data
    if latest_date is not None:
        start_date = latest_date + timedelta(days=1)
    else:
        start_date = datetime.now() - timedelta(days=30)  # Default to last 30 days
    
    end_date = datetime.now()
    
    # Fetch energy prices
    df_prices = fetch_energy_prices(
        start_date.strftime("%Y%m%d"),
        end_date.strftime("%Y%m%d")
    )
    
    if df_prices is not None and not df_prices.empty:
        # Fetch exchange rates
        df_rates = fetch_exchange_rates(start_date)
        
        if df_rates is not None and not df_rates.empty:
            # Merge prices with exchange rates
            df = pd.merge(df_prices, df_rates, on="time", how="left")
            
            # Fill missing exchange rates with the last known value
            df["eur_nok_rate"] = df["eur_nok_rate"].fillna(method="ffill")
            
            # Calculate NOK prices
            df["price_nok"] = df["price_eur"] * df["eur_nok_rate"]
            
            # Calculate daily averages
            df["date"] = pd.to_datetime(df["time"]).dt.date
            daily_avg = df.groupby("date").agg({
                "price_eur": "mean",
                "eur_nok_rate": "mean",
                "price_nok": "mean"
            }).reset_index()
            
            # Rename columns to match existing format
            daily_avg.columns = ["time", "EUR/MWh", "kurs", "NOK/MWh"]
            
            # Add NOK/KWh column
            daily_avg["NOK/KWh"] = (daily_avg["NOK/MWh"] / 1000).apply(lambda x: format(x, ".2f"))
            
            # Convert date to datetime string format - date only
            daily_avg["time"] = pd.to_datetime(daily_avg["time"]).dt.strftime("%Y-%m-%d")
            
            # Sort by date
            daily_avg = daily_avg.sort_values("time")
            
            ##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################
            
            file_name = "strompriser.csv"
            task_name = "Klima og energi - Strompriser"
            github_folder = "Data/04_Klima og ressursforvaltning/Kraft og energi/Kraftpriser/entso-e"
            temp_folder = os.environ.get("TEMP_FOLDER")
            
            # Call the function and get the "New Data" status
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

except Exception as e:
    print(f"Error in main execution: {str(e)}")
    notify_errors(script_name, str(e))
