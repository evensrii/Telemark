#### Dette er et testscript for å spørre etter og behandle data fra SSBs API.
#### Marker kode og trykk "Shift + Enter" for å kjøre den valgte koden.

######## Laste nødvendige (standard-)pakker

import requests  # For å kjøre spørringer mot alle mulige APIer ++
import pandas as pd  # For å håndtere data i tabellform. Standard i "Data science"
from pyjstat import pyjstat  # Anbefalt av SSB for å håndtere JSON-stat2 formatet

######## Hente data fra SSB API

# Endepunkt for SSB API
url = "https://data.ssb.no/api/v0/no/table/11607/"

# Spørring fra SSB API (kjent fra før..)
tfk_query = {
    "query": [
        {
            "code": "Region",
            "selection": {
                "filter": "agg_single:KommGjeldende",
                "values": [
                    "4001",
                    "4003",
                    "4005",
                    "4010",
                    "4012",
                    "4014",
                    "4016",
                    "4018",
                    "4020",
                    "4022",
                    "4024",
                    "4026",
                    "4028",
                    "4030",
                    "4032",
                    "4034",
                    "4036",
                ],
            },
        },
        {"code": "Alder", "selection": {"filter": "item", "values": ["15-74"]}},
        {"code": "Kjonn", "selection": {"filter": "item", "values": ["0"]}},
        {"code": "Landbakgrunn", "selection": {"filter": "item", "values": ["zzz"]}},
        {
            "code": "ContentsCode",
            "selection": {"filter": "item", "values": ["Sysselsatte2"]},
        },
        {"code": "Tid", "selection": {"filter": "top", "values": ["1"]}},
    ],
    "response": {"format": "json-stat2"},
}

######## Spørring vha. "requests"-modulen (Promt til ChatGPT: "Gi meg Python-kode for å hente data fra SSBs API ved hjelp av følgende kode: [limte inn alt over denne linjen]")

# Send POST-forespørselen
response = requests.post(url, json=tfk_query)

# Sjekk om forespørselen var vellykket
if response.status_code == 200:
    print("Forespørsel vellykket!")

    # Last JSON-stat2-data direkte til Dataset-objektet
    dataset = pyjstat.Dataset(response.json())

    # Konverter dataset til pandas DataFrame
    df = dataset.write("dataframe")

    # Skriv ut DataFrame for å verifisere data
    print(df.head())
else:
    print(f"Feil ved henting av data. Statuskode: {response.status_code}")
    print(response.text)


######## Leke seg med datasett (kalles en "dataframe" i pandas)

## Skriv # etterfulgt av en enkel beskrivelse av hva du ønsker å gjøre med datasettet.
## Trykk "Enter", vent på forslag fra Github Copilot, og trykk "Tab" for å godta forslaget.

# Vis grunnleggende info om datasettet
df.info()

# Vis de første 5 radene i datasettet
df.head()

## TIPS: Dobbeltklikke på en enkeltvariabel (f. eks. "df") og trykk "Shift + Enter" for å se innholdet i variabelen.
# Sammenlikne f. eks. "print(df)" med å dobbeltklikke på "df" og trykke "Shift + Enter"


######## Til slutt: Skrive endelig dataframe (df) til .csv-fil
df.to_csv("ssb_data.csv", index=False)
