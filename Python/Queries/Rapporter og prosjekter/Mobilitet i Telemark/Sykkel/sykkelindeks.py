import os
from datetime import date

import pandas as pd

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Script metadata
script_name = os.path.basename(__file__)
error_messages = []

# %% Hente sykkelindeks fra Trafikkdata sitt GraphQL-API

# API-et til Trafikkdataportalen (https://trafikkdata.atlas.vegvesen.no/om-api) er et
# GraphQL-API uten autentisering på https://trafikkdata-api.atlas.vegvesen.no.
# INDEX_ID er en ferdig definert egendefinert ("CUSTOM") trafikkindeks hos Statens vegvesen
# for sykkeltrafikk i Grenland (samme indeks-id som tidligere ble brukt i Power BI/Power Query).
# Indeksen publiseres én gang per år (beregningsmåned desember = rullerende 12-månedersindeks
# for hele året), så vi henter ett år av gangen de siste YEARS_BACK årene.
API_URL = "https://trafikkdata-api.atlas.vegvesen.no"
INDEX_ID = "4953"
YEARS_BACK = 7

GRAPHQL_QUERY = """
{{ publishedAreaTrafficVolumeIndex(id: {index_id}, year: {year}, month: 12) {{
    name
    period {{
      calculationMonth {{
        year
      }}
    }}
    aggregatedTrafficVolumeIndex(areaTypes: [CUSTOM]) {{
      area {{ name }}
      byRoadCategoryCombination {{
        roadCategoryCombination
        last12MonthsIndicesByDayType {{
          dayType
          byLengthRange {{
            lengthRange {{ representation }}
            volumeIndexNumber {{ percentageChange }}
          }}
        }}
      }}
    }}
  }}
}}
"""

current_year = date.today().year
responses = []

for years_back in range(1, YEARS_BACK + 1):
    query_year = current_year - years_back
    query = GRAPHQL_QUERY.format(index_id=INDEX_ID, year=query_year)
    try:
        data = fetch_data(
            API_URL,
            payload={"query": query},
            error_messages=error_messages,
            query_name=f"Sykkelindeks {query_year}",
            response_type="json",
        )
        responses.append(data)
    except Exception as e:
        error_messages.append(f"Klarte ikke å hente sykkelindeks for {query_year}: {e}")

# Notify yourself of errors, if any
if error_messages:
    print(f"Completed with {len(error_messages)} error(s).")
else:
    print("All tasks completed successfully.")

# %% Manuell inspeksjon (kjøres cellevis for å se på responsene)

print(f"Antall årsresponser hentet: {len(responses)}")
print(responses[0] if responses else "Ingen responser")

# %% Plukk ut ønsket kombinasjon (tilsvarer filtreringen som tidligere ble gjort i Power Query)

# Power Query-oppsettet unnestet alle kombinasjoner av vegkategori/døgn/kjøretøylengde og
# filtrerte etterpå ned til én rad per år: Vegkategori = "Europaveg_Riksveg_Fylkesveg_Kommunalveg",
# Døgn = "Alle", Kjøretøylengde = "Alle". Siden det er den eneste kombinasjonen vi trenger,
# plukker vi den direkte ut av responsen i stedet for å bygge opp og filtrere en lang tabell.
TARGET_VEGKATEGORI = "Europaveg_Riksveg_Fylkesveg_Kommunalveg"
TARGET_DAYTYPE = "ALL"
TARGET_LENGTH_RANGE = "[..,..)"

rows = []

for data in responses:
    indexes = data.get("data", {}).get("publishedAreaTrafficVolumeIndex") or []
    if not indexes:
        continue
    index_entry = indexes[0]

    indeksnavn = index_entry["name"]
    aar = index_entry["period"]["calculationMonth"]["year"]

    areas = index_entry["aggregatedTrafficVolumeIndex"] or []
    if not areas:
        print(f"Ingen områdedata for {aar}, hopper over.")
        continue
    area_entry = areas[0]
    omraade = area_entry["area"]["name"]

    indeks = None
    for combination in area_entry["byRoadCategoryCombination"]:
        if combination["roadCategoryCombination"].title() != TARGET_VEGKATEGORI:
            continue
        for by_day_type in combination["last12MonthsIndicesByDayType"]:
            if by_day_type["dayType"] != TARGET_DAYTYPE:
                continue
            for by_length_range in by_day_type["byLengthRange"]:
                if by_length_range["lengthRange"]["representation"] == TARGET_LENGTH_RANGE:
                    indeks = by_length_range["volumeIndexNumber"]["percentageChange"]
                    break

    if indeks is None:
        print(f"Fant ikke ønsket kombinasjon for {aar}, hopper over.")
        continue

    rows.append(
        {
            "Indeksnavn": indeksnavn,
            "År": aar,
            "Område": omraade,
            "Vegkategori": TARGET_VEGKATEGORI,
            "Døgn": "Alle",
            "Kjøretøylengde": "Alle",
            "Indeks": float(indeks),
        }
    )

df = pd.DataFrame(rows).sort_values("År", ascending=False).reset_index(drop=True)

# %% Beregnede kolonner (tilsvarer siste steg i Power Query)

df["Maksverdi"] = df["Indeks"].abs().apply(lambda x: 15 if x > 5.0 else 5).astype("Int64")
df["Minverdi"] = -df["Maksverdi"]
df["Målverdi"] = pd.array([0] * len(df), dtype="Int64")
df["Dato"] = df["År"].apply(lambda aar: pd.Timestamp(year=aar, month=12, day=31))

print(f"Antall rader: {len(df)}")
print(df.head(10))

# %% Lagre til csv, sammenlikne og eventuell opplasting til Github

file_name = "sykkelindeks.csv"
task_name = "Mobilitet i Telemark - Sykkelindeks"
github_folder = "Data/Mobilitet_i_Telemark/Sykkel"
temp_folder = os.environ.get("TEMP_FOLDER")

is_new_data = handle_output_data(
    df,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
    value_columns=["Indeks", "Maksverdi", "Minverdi"],
)

# Write a status file indicating if new data was found (used by master_script.py's email summary)
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

if is_new_data:
    print(f"New data detected in {file_name} and pushed to GitHub.")
else:
    print(f"No new data detected in {file_name}.")

print(f"New data status log written to {new_data_status_file}")
