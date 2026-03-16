import os
import pandas as pd
from pyjstat import pyjstat

# URL for SSB API - Andel innvandrere i befolkningen per kommune i Telemark (2026)
url = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/09817/data?"
    "lang=no&outputFormat=json-stat2"
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
data = pyjstat.Dataset.read(url)
df = data.write("dataframe")

print(df.head(20))
print(f"\nAntall rader: {len(df)}")
print(f"Kolonner: {list(df.columns)}")

# Rydd opp i datasettet
df = df.rename(columns={"value": "Andel innvandrere (%)"})

# Fjern unødvendige kolonner (kun én kategori)
df = df.drop(columns=["innvandringskategori", "landbakgrunn", "statistikkvariabel", "år"])

# Sorter etter andel (høyest først)
df["Andel innvandrere (%)"] = pd.to_numeric(df["Andel innvandrere (%)"], errors="coerce")
df = df.sort_values("Andel innvandrere (%)", ascending=False).reset_index(drop=True)

print("\n--- Andel innvandrere i kommunene i Telemark (2026) ---\n")
print(df.to_string(index=False))

# Lagre til CSV i samme mappe som scriptet
script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, "andel_innvandrere.csv")
df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
print(f"\nData lagret til: {output_path}")
