import requests
import pandas as pd
import json

## Henter ut forbruk per time for hhv. industri, private og næring (utenom industri) i alle kommuner i Telemark.
## Henter ut time for time, kan summeres for å få dagsforbruk.
## Se https://api.elhub.no/api/energy-data#/Municipality/municipalities

url = "https://api.elhub.no/energy-data/v0/municipalities?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR&&startDate=2024-06-27T20:00:00%2B02:00&endDate=2024-07-01T00:00:00%2B02:00&consumptionGroup=private"

response = requests.get(url)
data = response.json()  # A python dictionary


## Filtrerer ut bare kommunene i Telemark
# Access the list "data" containing the dictionaries to filter (starting with "40", dvs. Telemark)
if "data" in data:
    filtered_data = [
        entry for entry in data["data"] if entry.get("id", "").startswith("40")
    ]
    print(filtered_data)
else:
    print("No 'data' key found in the JSON response.")

## Henter ut det som skal være kolonner i en pandas DataFrame
# Initialize lists to store extracted data
names = []
start_times = []
end_times = []
consumption_groups = []
quantities = []

# Extract desired fields from filtered data
for entry in filtered_data:
    name = entry["attributes"]["name"]
    for hour_data in entry["attributes"]["consumptionPerGroupMunicipalityHour"]:
        start_time = hour_data["startTime"]
        end_time = hour_data["endTime"]
        consumption_group = hour_data["consumptionGroup"]
        quantity_kwh = hour_data["quantityKwh"]

        # Append to lists
        names.append(name)
        start_times.append(start_time)
        end_times.append(end_time)
        consumption_groups.append(consumption_group)
        quantities.append(quantity_kwh)

# Create a pandas DataFrame
df = pd.DataFrame(
    {
        "name": names,
        "startTime": start_times,
        "endTime": end_times,
        "consumptionGroup": consumption_groups,
        "quantityKwh": quantities,
    }
)

# Convert to datetime format
df["startTime"] = pd.to_datetime(df["startTime"])
df["endTime"] = pd.to_datetime(df["endTime"])

# Extract the date from the "startTime" column
df["date"] = df["startTime"].dt.date

# Based on the "date" column, sum the "quantityKwh" column to get one value per day. Keep the "name" and "consumptionGroup" columns.
total_consumption = (
    df.groupby(["date", "name", "consumptionGroup"])["quantityKwh"].sum().reset_index()
)

print(total_consumption)
