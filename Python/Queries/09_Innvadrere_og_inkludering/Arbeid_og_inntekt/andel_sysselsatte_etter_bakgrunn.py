import requests
import sys
import os
import glob
import pandas as pd
from pyjstat import pyjstat

# Alle kommuner, siste ti år (dvs. "top 10")


## MANGLER TALL FOR KUN TELEMARK I ÅRENE 2020-2023!
## SJEKKER PÅ NYTT NÅR 2024-TALLENE ER KLARE


""" # Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/11607/"

payload = {
    "query": [
        {
            "code": "Region",
            "selection": {"filter": "vs:FylkerAlle", "values": ["40", "38", "08"]},
        },
        {"code": "Alder", "selection": {"filter": "item", "values": ["15-74"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {
            "code": "Landbakgrunn",
            "selection": {"filter": "item", "values": ["abc", "ddd", "eee"]},
        },
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Sysselsatte2"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["10"]}},
    ],
    "response": {"format": "json-stat2"},
}


resultat = requests.post(POST_URL, json=payload)

if resultat.status_code == 200:
    print("Spørring ok")
else:
    print(f"Spørring feilet. Statuskode: {resultat.status_code}")

dataset = pyjstat.Dataset.read(resultat.text)
df = dataset.write("dataframe")
df.head()
df.info()

# Convert "år" to datetime
df["år"] = pd.to_datetime(df["år"])

df["kjønn"].unique()
df["landbakgrunn"].unique()

# Rename values in column "landbakgrunn"
df["landbakgrunn"] = df["landbakgrunn"].replace(
    {
        "Norden utenom Norge, EU/EFTA,  Storbritannia, USA, Canada, Australia, New Zealand": "Gruppe 1-land (EU, Storbritannia, USA ++)",
        "Europa utenom EU/EFTA og Storbritannia, Afrika, Asia, Amerika utenom USA og Canada, Oseania utenom Australia og NZ, polare områder": "Gruppe 2-land (Asia, Afrika, Latin-Amerika ++)",
    }
)

# Remove columns "alder", "kjønn" and "statistikkvariabel"
df = df.drop(columns=["alder", "kjønn", "statistikkvariabel"])

# Remove rows with NaN values
df = df.dropna()


# Rename columns
df_fylker = df_fylker.rename(columns={"Antall": "Andel"})

# Reset index
df_fylker = df_fylker.reset_index(drop=True)
 """
