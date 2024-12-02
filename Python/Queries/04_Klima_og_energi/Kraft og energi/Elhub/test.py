import requests
import pandas as pd

url = f"https://api.elhub.no/energy-data/v0/municipalities?dataset=CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR&&startDate=2023-05-03T20:00:00%2B02:00&endDate=2023-05-04T00:00:00%2B02:00"
response = requests.get(url)
data = response.json()

df_data = pd.json_normalize(data["data"])
df_data_expanded = df_data.explode("attributes.consumptionPerGroupMunicipalityHour")
df_consumption = pd.json_normalize(
    df_data_expanded["attributes.consumptionPerGroupMunicipalityHour"]
)

df_data_expanded = df_data_expanded.reset_index(drop=True)
df_consumption = df_consumption.reset_index(drop=True)

df_combined = pd.concat(
    [
        df_data_expanded[["attributes.municipalityNumber", "attributes.name"]],
        df_consumption,
    ],
    axis=1,
)

# Show unique values in the "consumptionGroup" column
df_combined["consumptionGroup"].unique()

# Keep only rows where 'attributes.municipalityNumber' is numeric (first line may contain an asterisk)
df_combined = df_combined[df_combined["attributes.municipalityNumber"].str.isnumeric()]

# Show unique values in the "consumptionGroup" column
df_combined["attributes.municipalityNumber"].unique()

df_telemark = df_combined[
    (df_combined["attributes.municipalityNumber"].astype(int) > 4000)
    & (df_combined["attributes.municipalityNumber"].astype(int) < 4200)
]


url = f"https://api.elhub.no/energy-data/v0/municipalities"
response = requests.get(url)
data = response.json()

df_data = pd.json_normalize(data["data"])

# Show values in "attributes.municipalityNumber" column starting with "38"
df_data[df_data["attributes.municipalityNumber"].str.startswith("40")]
df_data["attributes.municipalityNumber"].unique()


df_telemark = df_combined[
    (df_combined["attributes.municipalityNumber"].astype(int) > 4000)
    & (df_combined["attributes.municipalityNumber"].astype(int) < 4200)
]
