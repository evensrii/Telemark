#### Dette er et script for å spørre etter og behandle data fra Statens vegvesen sitt API.
#### Marker kode og trykk "Shift + Enter" for å kjøre den valgte koden.

######## Laste nødvendige (standard-)pakker

import requests  # For å kjøre spørringer mot alle mulige APIer ++
import pandas as pd  # For å håndtere data i tabellform. Standard i "Data science"
import json
from pyjstat import pyjstat  # Anbefalt av SSB for å håndtere JSON-stat2 formatet

######## Hente data fra Entur API

# Endepunkt for API
url = "https://trafikkdata-api.atlas.vegvesen.no"

# STestspørring fra IDE API (kjent fra før..)
tfk_query = """
{
  trafficRegistrationPoints(searchQuery: {roadCategoryIds: [E]}) {
    id
    name
    location {
      coordinates {
        latLon {
          lat
          lon
        }
      }
    }
  }
}
"""

######## Spørring vha. "requests"-modulen (Promt til ChatGPT: "Gi meg Python-kode for å hente data fra SSBs API ved hjelp av følgende kode: [limte inn alt over denne linjen]")

# HTTP-headers (statens vegvesen kan kreve spesifikke autoriseringsnøkler)
headers = {
    "Content-Type": "application/json",
    "API-Key": "your-api-key-here"  # Bytt ut med en gyldig API-nøkkel
}

# Send POST-forespørselen
response = requests.post(url, json={"query": tfk_query}, headers=headers)

# Sjekk om forespørselen var vellykket
if response.status_code == 200:
    # Parse JSON-responsen
    data = response.json()

    # Konverter dataset til pandas DataFrame
    #df = pyjstat.from_json_stat(data).to_pandas()
    
    # Skriv ut data for å bekrefte resultatene
    print(json.dumps(data, indent=2))

    # Håndter og formatter dataene videre, hvis ønskelig
    registration_points = data.get("data", {}).get("trafficRegistrationPoints", [])
    for point in registration_points:
        point_id = point.get("id")
        name = point.get("name")
        location = point.get("location", {}).get("coordinates", {}).get("latLon", {})
        lat = location.get("lat")
        lon = location.get("lon")
        print(f"Registreringspunkt ID: {point_id}, Navn: {name}, Latitude: {lat}, Longitude: {lon}")
        
else:
    print(f"Feil ved henting av data. Statuskode: {response.status_code}")
    print(response.text)


######## Leke seg med datasett (kalles en "dataframe" i pandas)

## Skriv # etterfulgt av en enkel beskrivelse av hva du ønsker å gjøre med datasettet.
## Trykk "Enter", vent på forslag fra Github Copilot, og trykk "Tab" for å godta forslaget.

# Håndter og formatter dataene videre
registration_points = data.get("data", {}).get("trafficRegistrationPoints", [])

# Lag en liste med dataene som kan konverteres til DataFrame
registration_data = []
for point in registration_points:
    point_id = point.get("id")
    name = point.get("name")
    location = point.get("location", {}).get("coordinates", {}).get("latLon", {})
    lat = location.get("lat")
    lon = location.get("lon")
        
    registration_data.append({
        "id": point_id,
        "name": name,
        "latitude": lat,
        "longitude": lon
    })
    
# Konverter listen til en pandas DataFrame
df = pd.DataFrame(registration_data)

# Skriv ut DataFrame
#print(df)


## TIPS: Dobbeltklikke på en enkeltvariabel (f. eks. "df") og trykk "Shift + Enter" for å se innholdet i variabelen.
# Sammenlikne f. eks. "print(df)" med å dobbeltklikke på "df" og trykke "Shift + Enter"


######## Til slutt: Skrive endelig dataframe (df) til .csv-fil
df.to_csv("ssb_data.csv", index=False)
