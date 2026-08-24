import os
import pandas as pd
from pyproj import Transformer

from Helper_scripts.utility_functions import fetch_data
from Helper_scripts.github_functions import handle_output_data

# Script metadata
script_name = os.path.basename(__file__)
error_messages = []

# %% Hente trafikkulykker fra NVDB sitt eksport-API

# Vegobjekttype 570 = "Trafikkulykke" i NVDB sin datakatalog.
# Eksport-APIet (https://nvdb-eksport-x.atlas.vegvesen.no/swagger) returnerer samme flate
# CSV-format som den manuelle nedlastingen fra nvdb-eksport, men lar oss filtrere direkte på
# fylkesnummer i stedet for å måtte kombinere en egenskap-filter med et kartutsnitt (bounding box).
# fylke=40 er Telemark sitt fylkesnummer (gjeldende fylkesinndeling fra 2024).
# inkluder=alle gir alle kolonnegruppene fra eksporten: OBJ. (objekt-id/versjon/datoer),
# MET. (metadata), LOK. (kommune/fylke/vegsystemreferanse), VSR. (vegsystemreferanse-detaljer),
# REL. (relasjoner til andre vegobjekter, f.eks. involverte kjøretøy), GEO. (geometri), og de
# dynamiske EGS.<NAVN>.<EGENSKAPSID>-kolonnene med selve ulykkesdataene
# (f.eks. EGS.ULYKKESDATO.5055, EGS.ULYKKESKODE.5066, EGS.VÆRFORHOLD.5079 osv.).
url = (
    "https://nvdb-eksport-x.atlas.vegvesen.no/vegobjekter/570.csv"
    "?fylke=40&inkluder=alle&srid=5973&segmentering=true"
)

try:
    df = fetch_data(
        url,
        error_messages=error_messages,
        query_name="Trafikkulykker (NVDB)",
        response_type="csv",
        delimiter=";",
        encoding="utf-8-sig",  # Eksporten leveres som UTF-8 med BOM
    )
except Exception as e:
    error_messages.append(f"Klarte ikke å hente trafikkulykker fra NVDB: {e}")
    raise

# Notify yourself of errors, if any
if error_messages:
    print(f"Completed with {len(error_messages)} error(s).")
else:
    print("All tasks completed successfully.")

# %% Manuell inspeksjon (kjøres cellevis for å se på df.head() etc.)

print(f"Antall rader (rå eksport): {len(df)}")
print(f"Antall kolonner (rå eksport): {len(df.columns)}")
print(df.head())

# %% Datavask (tilsvarer oppsettet som tidligere ble brukt i Power BI/Power Query)

# Behold bare vegobjekt-id, vegsystemreferanse og de EGS-kolonnene som faktisk beskriver
# ulykken (dato/tid/type/alvorlighet/forhold/involverte enheter), og gi dem korte,
# lesbare navn i stedet for "EGS.<NAVN>.<ID>"/"VSR.<NAVN>".
RENAME_MAP = {
    "OBJ.VEGOBJEKT-ID": "VEGOBJEKT_ID",
    "EGS.ALVORLIGHETSGRAD.5074": "ALVORLIGHETSGRAD",
    "EGS.ULYKKESDATO.5055": "ULYKKESDATO",
    "EGS.ULYKKESKLOKKESLETT.5056": "ULYKKESKLOKKESLETT",
    "EGS.UKEDAG.5054": "UKEDAG",
    "EGS.MÅNED (NY).11898": "MÅNED",
    "EGS.ULYKKESKODE.5066": "ULYKKESKODE",
    "EGS.ANTALL ENHETER (STK).5069": "ANTALL ENHETER",
    "EGS.ANTALL DREPTE I ULYKKEN (STK).5070": "ANTALL DREPTE",
    "EGS.ANTALL MEGET ALVORLIG SKADET (STK).5071": "ANTALL MEGET ALVORLIG SKADET",
    "EGS.ANTALL ALVORLIG SKADET (STK).5072": "ANTALL ALVORLIG SKADET",
    "EGS.ANTALL LETTERE SKADET (STK).5073": "ANTALL LETTERE SKADET",
    "EGS.VEGTYPE.5075": "VEGTYPE",
    "EGS.STEDSFORHOLD.5076": "STEDSFORHOLD",
    "EGS.DEKKETYPE.5077": "DEKKETYPE",
    "EGS.FØREFORHOLD.5078": "FØREFORHOLD",
    "EGS.VÆRFORHOLD.5079": "VÆRFORHOLD",
    "EGS.LYSFORHOLD.5080": "LYSFORHOLD",
    "EGS.KJØREFELTTYPE.5081": "KJØREFELTTYPE",
    "EGS.ANTALL KJØREFELT (STK).5082": "ANTALL KJØREFELT",
    "EGS.TETTSTED.5083": "TETTSTED",
    "EGS.FARTSGRENSE (KM/H).5085": "FARTSGRENSE",
    "EGS.HISTORISK VEGKATEGORI.5115": "HISTORISK VEGKATEGORI",
    "EGS.HISTORISK VEGNUMMER.5116": "HISTORISK VEGNUMMER",
    "EGS.GEOMETRI, PUNKT.5123": "GEOMETRI, PUNKT",
    "EGS.UID (NY).11906": "UID",
    "EGS.UKEDAGSTYPE (NY).11907": "UKEDAGSTYPE",
    "EGS.ULYKKESTYPE (NY).11908": "ULYKKESTYPE",
    "EGS.ULYKKESTYPE UNDERKATEGORI (NY).11909": "ULYKKESTYPE UNDERKATEGORI",
    "EGS.ÅR (NY).11910": "ÅR",
    "EGS.ANTALL ANDRE ENHETER (NY) (STK).11911": "ANTALL ANDRE ENHETER",
    "EGS.ANTALL BUSS (NY) (STK).11912": "ANTALL BUSS",
    "EGS.ANTALL FOTGJENGERE (NY) (STK).11913": "ANTALL FOTGJENGERE",
    "EGS.ANTALL LASTEBIL (NY) (STK).11914": "ANTALL LASTEBIL",
    "EGS.ANTALL LETT MC (NY) (STK).11915": "ANTALL LETT MC",
    "EGS.ANTALL MC (NY) (STK).11916": "ANTALL MC",
    "EGS.ANTALL MOPED (NY) (STK).11917": "ANTALL MOPED",
    "EGS.ANTALL PERSONBIL (NY) (STK).11918": "ANTALL PERSONBIL",
    "EGS.ANTALL SKINNEGÅENDE MATERIELL (NY) (STK).11919": "ANTALL SKINNEGÅENDE MATERIELL",
    "EGS.ANTALL LITEN ELEKTRISK MOTORVOGN (STK).12625": "ANTALL LITEN ELEKTRISK MOTORVOGN",
    "EGS.ANTALL SYKKEL (NY) (STK).11920": "ANTALL SYKKEL",
    "EGS.ANTALL TRAKTOR (NY) (STK).11921": "ANTALL TRAKTOR",
    "EGS.ANTALL UKJENTE ENHETER (NY) (STK).11922": "ANTALL UKJENTE ENHETER",
    "EGS.ANTALL VAREBIL (NY) (STK).11923": "ANTALL VAREBIL",
    "EGS.ÅDT (NY).11924": "ÅRSDØGNTRAFIKK",
    "EGS.VEGBELYSNING (NY).11928": "VEGBELYSNING",
    "EGS.FYLKENAVN (NY).11900": "FYLKENAVN",
    "EGS.FYLKENUMMER (NY).11901": "FYLKENUMMER",
    "EGS.KOMMUNENAVN (NY).11902": "KOMMUNENAVN",
    "EGS.KOMMUNENUMMER (NY).11903": "KOMMUNENUMMER",
    "VSR.VEGSYSTEMREFERANSE": "VEGSYSTEMREFERANSE",
    "VSR.VEGKATEGORI": "VEGKATEGORI",
    "VSR.VEGNUMMER": "VEGNUMMER",
}

df = df[list(RENAME_MAP.keys())].rename(columns=RENAME_MAP)

# EGS.GEOMETRI, PUNKT.5123 kommer som WKT, f.eks. "POINT(192335 6575820)", i EUREF89/UTM sone 33
# (horisontalkomponenten av srid 5973). Noen eldre ulykker mangler punktgeometri (kun stedfestet
# via vegsystemreferanse/HP/meterverdi) og filtreres bort, siden de ikke kan plottes i kart.
coords = df["GEOMETRI, PUNKT"].str.extract(r"\(([\-\d.]+)\s+([\-\d.]+)\)")
df["UTM_ØST"] = pd.to_numeric(coords[0])
df["UTM_NORD"] = pd.to_numeric(coords[1])
df = df[df["UTM_ØST"].notna()].copy()

transformer = Transformer.from_crs("EPSG:25833", "EPSG:4326", always_xy=True)
lon, lat = transformer.transform(df["UTM_ØST"].to_numpy(), df["UTM_NORD"].to_numpy())
df["LONGITUDE"] = lon
df["LATITUDE"] = lat

df = df.drop(columns=["GEOMETRI, PUNKT", "UTM_ØST", "UTM_NORD"])

antall_kolonner = [col for col in df.columns if col.startswith("ANTALL ")]
for col in antall_kolonner:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

# Fjern kolonner uten noen verdier i det hele tatt (f.eks. skadegrad-feltene, som NVDB
# ikke lenger eksponerer i det åpne API-et av personvernhensyn).
df = df.dropna(axis=1, how="all")

# Kolonnenavn på formen "Stor forbokstav, resten små bokstaver"
df.columns = [col.capitalize() for col in df.columns]

print(f"Antall rader (etter datavask): {len(df)}")
print(f"Antall kolonner (etter datavask): {len(df.columns)}")
print(df.head())

# %% Lagre til csv, sammenlikne og eventuell opplasting til Github

file_name = "trafikkulykker_telemark.csv"
task_name = "Mobilitet i Telemark - Trafikkulykker"
github_folder = "Data/Mobilitet_i_Telemark/Trafikkulykker"
temp_folder = os.environ.get("TEMP_FOLDER")

is_new_data = handle_output_data(
    df,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
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
