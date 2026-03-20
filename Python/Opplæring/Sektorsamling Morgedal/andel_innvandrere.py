import requests
import pandas as pd
from pyjstat import pyjstat

# URL til SSB API (ny v2 GET-spørring)
GET_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/09817/data?lang=no"
    "&outputFormat=json-stat2"
    "&valuecodes[Tid]=2026"
    "&valuecodes[Region]=4001,4003,4005,4010,4012,4014,4016,4018,4020,4022,4024,4026,4028,4030,4032,4034,4036"
    "&codelist[Region]=agg_KommGjeldende"
    "&valuecodes[Landbakgrunn]=999"
    "&valuecodes[ContentsCode]=AndelBefolkning"
    "&valuecodes[InnvandrKat]=B"
    "&heading=Region,ContentsCode,Tid"
    "&stub=InnvandrKat,Landbakgrunn"
)

# Hent data fra SSB
response = requests.get(GET_URL)
response.raise_for_status()

# Parse json-stat2 til pandas DataFrame
dataset = pyjstat.Dataset.read(response.text)
df = dataset.write("dataframe")

print("Rådata fra SSB:")
print(df)

# %%

# Bearbeiding av dataframe
df_result = df.rename(columns={"value": "Andel innvandrere (%)"})

# Fjern unødvendige kolonner (beholder Region og verdi)
df_result = df_result[["region", "Andel innvandrere (%)"]].copy()
df_result = df_result.rename(columns={"region": "Kommune"})

# Sorter etter kommune
df_result = df_result.sort_values("Kommune").reset_index(drop=True)

print("\nFerdig tabell:")
print(df_result.to_string(index=False))

# %%

# Lagre som CSV i samme mappe som scriptet
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, "andel_innvandrere.csv")

df_result.to_csv(output_path, index=False, encoding="utf-8-sig", sep=";")
print(f"\nCSV lagret til: {output_path}")
