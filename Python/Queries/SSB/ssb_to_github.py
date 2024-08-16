import sys
import requests
from pyjstat import pyjstat

# Legger til mappen "custom_functions" i sys.path (en liste med mapper hvor interpreter leter etter moduler)
sys.path.append(".." + "\\custom_functions")

# from github_functions import upload_to_github (peker direkte til funksjon, så da må i tilfelle github importeres i dette scriptet)
import github_functions

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/07459/"

# Spørring for å hente ut data fra SSB
payload = {
    "query": [
        {
            "code": "Region",
            "selection": {
                "filter": "agg:KommSummer",
                "values": [
                    "K-4001",
                    "K-4003",
                    "K-4005",
                    "K-4010",
                    "K-4012",
                    "K-4014",
                    "K-4016",
                    "K-4018",
                    "K-4020",
                    "K-4022",
                    "K-4024",
                    "K-4026",
                    "K-4028",
                    "K-4030",
                    "K-4032",
                    "K-4034",
                    "K-4036",
                ],
            },
        },
        {"code": "Alder", "selection": {"filter": "vs:AlleAldre00B", "values": []}},
        {
            "code": "Tid",
            "selection": {
                "filter": "item",
                "values": [
                    "2001",
                    "2002",
                    "2003",
                    "2004",
                    "2005",
                    "2006",
                    "2007",
                    "2008",
                    "2009",
                    "2010",
                    "2011",
                    "2012",
                    "2013",
                    "2014",
                    "2015",
                    "2016",
                    "2017",
                    "2018",
                    "2019",
                    "2020",
                    "2021",
                    "2022",
                    "2023",
                    "2024",
                ],
            },
        },
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
df.tail()
df.info()

####### Lagre datasett som fil

df.to_csv(
    "folketall_kommuner_telemark.csv", index=False
)  # Relativt til dette scriptet.

##################### Opplasting til Github #####################

# Hvis eksisterer, oppdater filen. Hvis ikke, opprett filen.

source_file = "folketall_kommuner_telemark.csv"  # Relativt til dette scriptet.
destination_folder = (
    "test_folder"  # Mapper som ikke eksisterer vil opprettes automatisk.
)
github_repo = "evensrii/python_testing"
git_branch = "main"

github_functions.upload_to_github(
    source_file, destination_folder, github_repo, git_branch
)
