import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import io

#### Ved første gangs kjøring lastes alle data fra ENTSO-E API ned i månedsvise loops. En engangsopersjon som kan kjøres manuelt.

# Define the base URL and common parameters
base_url = "https://web-api.tp.entsoe.eu/api"
security_token = (
    "5cf92fdd-a882-4158-a5a9-9b0b8e202786"  # Replace with your actual token
)

# Define the start and end dates
start_date = datetime(2021, 1, 1, 0, 0)  # Start at midnight
end_date = datetime(2024, 10, 31, 23, 59)  # End at the last minute of October 31, 2024

# Initialize an empty DataFrame to store all data
all_data = pd.DataFrame()

# Loop through each month from start_date to end_date
current_date = start_date
while current_date <= end_date:
    # Calculate the periodStart and periodEnd for the current month
    period_start = current_date.strftime("%Y%m%d%H%M")  # Start of the month
    next_month = current_date.replace(day=28) + timedelta(days=4)  # Jump to next month
    period_end_date = next_month.replace(day=1) - timedelta(
        seconds=1
    )  # End of the month
    period_end = period_end_date.strftime("%Y%m%d%H%M")

    # Update parameters for the request
    params = {
        "securityToken": security_token,
        "documentType": "A44",  # Price document
        "periodStart": period_start,
        "periodEnd": period_end,
        "out_Domain": "10YNO-2--------T",  # NO2
        "in_Domain": "10YNO-2--------T",
    }

    # Make the GET request
    response = requests.get(base_url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the XML response
        ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
        root = ET.fromstring(response.text)

        # Extract data into lists
        time_price_data = []
        for timeseries in root.findall("ns:TimeSeries", ns):
            for period in timeseries.findall("ns:Period", ns):
                # Get the start time and resolution
                start_time = datetime.fromisoformat(
                    period.find("ns:timeInterval/ns:start", ns).text[:-1]
                )

                # Parse points (hourly prices)
                for point in period.findall("ns:Point", ns):
                    position = int(point.find("ns:position", ns).text)
                    price = float(point.find("ns:price.amount", ns).text)

                    # Calculate the timestamp for this point
                    timestamp = start_time + timedelta(hours=position - 1)
                    time_price_data.append({"time": timestamp, "price": price})

        # Convert to DataFrame and append to all_data
        if time_price_data:  # Check if there is data
            month_df = pd.DataFrame(time_price_data)
            all_data = pd.concat([all_data, month_df], ignore_index=True)
        else:
            print(f"No data for period {period_start} to {period_end}.")
    else:
        print(
            f"Failed to fetch data for period {period_start} to {period_end}: {response.status_code}, {response.text}"
        )

    # Move to the next month
    current_date = next_month.replace(day=1)

# Save or display the final DataFrame
print(all_data)

# Calculate the average price per day
all_data["date"] = all_data["time"].dt.date
daily_avg_prices = all_data.groupby("date")["price"].mean()

# Covert to a dataframe with date
daily_avg_prices = daily_avg_prices.reset_index()

# Name the columns "time" and "EUR/MWh"
daily_avg_prices.columns = ["time", "EUR/MWh"]

daily_avg_prices.info()

### VALUTAKURSER EUR til NOK

base_url = "https://data.norges-bank.no/api/data/EXR/B.EUR.NOK.SP"
params = {
    "startPeriod": "2019-01-01",
    "endPeriod": "2024-10-31",
    "format": "csv",
    "bom": "include",
    "locale": "no",
}

# Make the GET request
response = requests.get(base_url, params=params)

# Check the response
if response.status_code == 200:
    # Use io.StringIO to handle the CSV response as a file-like object
    exchange_data = pd.read_csv(io.StringIO(response.text), encoding="utf-8", sep=";")
    print(exchange_data.head())  # Display the first few rows of the DataFrame
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
    print(response.text)

# Keep only columns "TIME_PERIOD" and "OBS_VALUE"
exchange_data = exchange_data[["TIME_PERIOD", "OBS_VALUE"]]

# Rename the columns to "time" and "exchange_rate"
exchange_data.columns = ["time", "kurs"]

### Merge "daily_avg_prices" and "exchange_data" on "time"

# Convert the "time" column to datetime format
daily_avg_prices["time"] = pd.to_datetime(daily_avg_prices["time"])
exchange_data["time"] = pd.to_datetime(exchange_data["time"])

# Perform a left join to keep all rows from daily_avg_prices
merged_data = pd.merge(
    daily_avg_prices,
    exchange_data,
    on="time",  # Merge on the 'time' column
    how="left",  # Left join to keep all rows from daily_avg_prices
)

# Fill NaN values by propagating the previous value
merged_data["kurs"] = merged_data["kurs"].fillna(method="ffill")

# Replace "," with "." and convert to float
merged_data["kurs"] = merged_data["kurs"].str.replace(",", ".").astype(float)

# Calculate the price in NOK
merged_data["NOK/MWh"] = merged_data["EUR/MWh"] * merged_data["kurs"]

# Create a column named "NOK/KWh" by dividing "NOK/MWh" by 1000
merged_data["NOK/KWh"] = merged_data["NOK/MWh"] / 1000

# Save the data to a CSV file
merged_data.to_csv("strompriser.csv", index=False)
