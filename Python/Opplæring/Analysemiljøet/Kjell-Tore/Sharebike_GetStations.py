import json
import jwt  # PyJWT-biblioteket
import requests
import datetime

# 🔹 Erstatt med den faktiske stien til din JSON-nøkkelfil
SERVICE_ACCOUNT_FILE = "C:\Github\Telemark\Python\Opplæring\Analysemiljøet\Kjell-Tore\prod-farte.json"

# 🔹 Les inn tjenestekontoinformasjonen fra JSON-filen
with open(SERVICE_ACCOUNT_FILE) as f:
    service_account_info = json.load(f)

# 🔹 Hent nødvendig informasjon fra JSON-filen
private_key = service_account_info["private_key"]
client_email = service_account_info["client_email"]

# 🔹 Definer JWT-payload
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

# 🔹 Skriv ut tokenet
#print("Generated JWT Token:")
#print(jwt_token)

# 🔹 API-endepunktet
api_url = "https://api.sharebike.com/station-service/dashboard/stations?size=20&page=0&cityId=36"

# 🔹 Legg JWT-tokenet i HTTP-headeren
headers = {
    "Authorization": f"Bearer {jwt_token}",
    "Content-Type": "application/json",
}

# 🔹 Send forespørsel til API-et
response = requests.get(api_url, headers=headers)

if response.status_code == 200:
# Lagre responsen som JSON-fil
    with open('sharebike_stations.json', 'w') as json_file:
        json.dump(response.json(), json_file, indent=4)
    print("Data lagret i sharebike_stations.json")
else:
    print(f"Feil ved API-kall: {response.status_code}, {response.text}")