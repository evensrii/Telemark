# NB!!!!!!!!!!!
# Scriptet stopper ikke når dataene er slutt. Dette må fikses før det kjøres en gang til.

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
    "iss": client_email,   
    "sub": client_email,   
    "aud": "sharebike-prod",  
    "iat": now,            
    "exp": now + datetime.timedelta(minutes=60),  
    "email": client_email, 
}

# 🔹 Signer JWT-tokenet med privat nøkkel
jwt_token = jwt.encode(payload, private_key, algorithm="RS256")

# API-endepunkt for journeys
api_url = "https://api.sharebike.com/journey-service/dashboard/journeys?cityId=36"

# Definer lagringssti
base_path = r"C:\Github\Telemark\Python\Opplæring\Analysemiljøet\Kjell-Tore\sharebike_data"
journeys_path = os.path.join(base_path, "journeys")

# Sørg for at katalogen finnes
os.makedirs(journeys_path, exist_ok=True)

# Funksjon for å hente data i sider på 10 000 rader
def fetch_and_save_journeys(api_url):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
    }

    page = 0
    total_records = 0
    date_tag = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    while True:
        paged_url = f"{api_url}&size=10000&page={page}"
        response = requests.get(paged_url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            if not data or len(data) == 0:
                print(f"Ingen flere data funnet etter {total_records} rader.")
                break  # Slutt hvis ingen flere data

            # Lagre hver side i en egen fil
            filename = os.path.join(journeys_path, f"sharebike_data_journeys_{date_tag}_page_{page}.json")
            with open(filename, 'w') as json_file:
                json.dump(data, json_file, indent=4)

            total_records += len(data)
            print(f"Side {page} lagret: {filename} ({len(data)} rader)")

            page += 1  # Neste side
        else:
            print(f"Feil ved API-kall: {response.status_code}, {response.text}")
            break  # Stopp ved feil

# Kjør funksjonen
fetch_and_save_journeys(api_url)