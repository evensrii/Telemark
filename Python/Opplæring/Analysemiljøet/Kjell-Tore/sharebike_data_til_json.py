import requests
import jwt
import datetime
import json
import os

# Definer tjenestekontoinfo (bruker JSON-nøkkelfil)
with open('prod-farte.json') as f:
    service_account_info = json.load(f)

private_key = service_account_info["private_key"]
client_email = service_account_info["client_email"]

# Generer JWT-token
now = datetime.datetime.utcnow()
payload = {
    "iss": client_email,   # Hvem som utsteder tokenet
    "sub": client_email,   # Hvem tokenet er for
    "aud": "sharebike-prod",  # Målgruppen for tokenet (basert på API-URL)
    "iat": now,            # Utstedt tidspunkt
    "exp": now + datetime.timedelta(minutes=60),  # Utløpstid (60 min)
    "email": client_email, # Epostadresse
}

# 🔹 Signer JWT-tokenet med privat nøkkel
jwt_token = jwt.encode(payload, private_key, algorithm="RS256")

# Definer API-URL-er og relaterte data
api_data = [
    {"url": "https://api.sharebike.com/station-service/dashboard/vehicles?size=500&page=0&cityId=36", "type": "vehicles"},
    {"url": "https://api.sharebike.com/journey-service/dashboard/journeys?size=10000&page=0&cityId=36", "type": "journeys"},
    {"url": "https://api.sharebike.com/station-service/dashboard/stations?size=100&page=0&cityId=36", "type": "stations"},
]

# Definerer stien for data fra de ulike endepunktene
base_path = r"\sharebike_data"
catalogs = {
    "journeys": os.path.join(base_path, "journeys"),
    "vehicles": os.path.join(base_path, "vehicles"),
    "stations": os.path.join(base_path, "stations"),
}

# Sørg for at katalogene finnes
for path in catalogs.values():
    os.makedirs(path, exist_ok=True)

# Funksjon for å hente data fra API og lagre det i en fil
def fetch_and_save_data(api_url, filename):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
    }
    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        with open(filename, 'w') as json_file:
            json.dump(response.json(), json_file, indent=4)
        print(f"Data lagret i {filename}")
    else:
        print(f"Feil ved API-kall: {response.status_code}, {response.text}")

# Hent data fra alle API-er og lagre i separate filer med datotag
date_tag = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Definerer stien for data fra de ulike endepunktene
base_path = r"C:\Users\kje0303\OneDrive - Telemark fylkeskommune\GitHub\Telemark\Data\Mobilitet_i_Telemark\Sharebike"
catalogs = {
    "journeys": os.path.join(base_path, "journeys"),
    "vehicles": os.path.join(base_path, "vehicles"),
    "stations": os.path.join(base_path, "stations"),
}

# Sørg for at katalogene finnes
for path in catalogs.values():
    os.makedirs(path, exist_ok=True)

# Iterer gjennom API-ene og hent data
for api in api_data:
    url = api["url"]
    data_type = api["type"]

    # Velg riktig katalog, bruk en fallback hvis type ikke er definert
    catalog = catalogs.get(data_type, base_path)

#    filename = f"sharebike_data_{data_type}.json"  # Filnavn med type uten datotag
#    filename = f"{catalog_Journeys}sharebike_data_{data_type}_{date_tag}.json"  # Filnavn med type og datotag
    # Lag filnavn med riktig katalog
    filename = os.path.join(catalog, f"sharebike_data_{data_type}_{date_tag}.json")

    print("Henter data fra:", api["url"])  # Debugging
    fetch_and_save_data(url, filename)