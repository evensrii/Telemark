"""
Sammenligner kjøretid og avstand for én strekning (grunnkrets -> grunnkrets) på flere
avreisetidspunkter via Google Routes API (Compute Route Matrix, 1 x 1 per tidspunkt).

SKU: Compute Route Matrix Pro (TRAFFIC_AWARE_OPTIMAL) | DRIVE | BEST_GUESS
Punktene kan angis som grunnkretsnummer (bruker sentroiden fra origins_gk_befolkning.csv)
eller som koordinater (lat, lon).

Utdata: Utdata/Tidspunkter/<origin>_<destinasjon>_<tidsstempel>.csv
Krever GOOGLE_ROUTES_API_KEY i token.env (Python-mappen).
"""

import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.environ["PYTHONPATH"], "token.env"), override=True)
API_KEY = os.getenv("GOOGLE_ROUTES_API_KEY")
if not API_KEY:
    raise RuntimeError("GOOGLE_ROUTES_API_KEY mangler i token.env")

BASE_DIR = Path(os.environ["PYTHONPATH"]) / "Queries" / "Fagseksjoner" / "Tannhelse" / "Reisetidsanalyse"
GRUNNKRETS_FILE = BASE_DIR / "Inndata" / "Befolkning" / "origins_gk_befolkning.csv"
OUTPUT_DIR = BASE_DIR / "Utdata" / "Tidspunkter"

# Grunnkretsnummer (int) eller koordinater (lat, lon).
# Tidligere kjøring: ORIGIN = 40010502 (Klevstrand), DESTINATION = 40120306 (Grasmyr)
ORIGIN = (59.11893734393534, 9.707539724727964)
DESTINATION = (59.01610248098124, 9.652019363777399)

# Norsk lokal tid; sommer-/vintertid håndteres automatisk
DATES = ["2026-10-06", "2026-12-08", "2027-01-05", "2027-04-06"]
TIMES = ["10:00", "15:30"]
TZ = ZoneInfo("Europe/Oslo")

URL = "https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix"
FIELD_MASK = "originIndex,destinationIndex,status,condition,distanceMeters,duration,staticDuration"


def waypoint(lat, lon):
    return {"waypoint": {"location": {"latLng": {"latitude": lat, "longitude": lon}}}}


def compute_route(origin, destination, departure_time):
    body = {
        "origins": [waypoint(*origin)],
        "destinations": [waypoint(*destination)],
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE_OPTIMAL",
        "trafficModel": "BEST_GUESS",
        "departureTime": departure_time,
    }
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    response = requests.post(URL, json=body, headers=headers, timeout=60)
    if not response.ok:
        raise RuntimeError(f"Routes API feilet ({response.status_code}): {response.text}")
    return response.json()[0]


def parse_seconds(value):
    # Varighet returneres som streng, f.eks. "1234s"
    return int(value.rstrip("s")) if isinstance(value, str) else None


def resolve_point(point, gk):
    # Returnerer (lat, lon, visningsnavn, filnavn-del)
    if isinstance(point, tuple):
        lat, lon = point
        return lat, lon, f"({lat:.6f}, {lon:.6f})", f"{lat:.4f}_{lon:.4f}"
    row = gk.loc[point]
    return row["Latitude"], row["Longitude"], f"{row['grunnkretsnavn']} ({point})", row["grunnkretsnavn"].lower()


# %% Finn koordinater for punktene
gk = pd.read_csv(GRUNNKRETS_FILE, sep=";", decimal=",", encoding="utf-8-sig").set_index("grunnkretsnummer")
o_lat, o_lon, o_label, o_file = resolve_point(ORIGIN, gk)
d_lat, d_lon, d_label, d_file = resolve_point(DESTINATION, gk)
stretch = f"{o_label} -> {d_label}"
print(f"Strekning: {stretch}")

# %% Hent rute for hvert tidspunkt (1 element per tidspunkt)
rows = []
for date in DATES:
    for time in TIMES:
        dt = datetime.fromisoformat(f"{date}T{time}").replace(tzinfo=TZ)
        row = {"dato": date, "ukedag": dt.strftime("%A"), "klokkeslett": time, "departure_time": dt.isoformat()}
        try:
            el = compute_route(
                (o_lat, o_lon),
                (d_lat, d_lon),
                dt.isoformat(),
            )
            row.update(
                {
                    "condition": el.get("condition"),
                    "distance_km": el.get("distanceMeters", 0) / 1000,
                    "duration_min": round(parse_seconds(el.get("duration")) / 60, 2),
                    "staticDuration_min": round(parse_seconds(el.get("staticDuration")) / 60, 2),
                }
            )
        except Exception as e:
            row["feil"] = str(e)
            print(f"Feil for {dt.isoformat()}: {e}")
        rows.append(row)

df = pd.DataFrame(rows)

# %% Differanser
# Trafikkforsinkelse = reell kjøretid (med trafikk) - statisk kjøretid (uten trafikk).
# Avvik = differanse mot første tidspunkt (referanse) og mot strekningens laveste verdi.
df["trafikkforsinkelse_min"] = (df["duration_min"] - df["staticDuration_min"]).round(2)
ref = df.iloc[0]
for col in ["distance_km", "duration_min", "staticDuration_min"]:
    df[f"{col}_diff_mot_ref"] = (df[col] - ref[col]).round(3)
df["duration_min_diff_mot_laveste"] = (df["duration_min"] - df["duration_min"].min()).round(2)
df["samme_rute_som_ref"] = df["distance_km"].sub(ref["distance_km"]).abs() < 0.05

df.insert(0, "strekning", stretch)
df

# %% Oversikt
pd.set_option("display.width", 200)
print(f"\n{stretch}")
print(f"Referanse: {ref['dato']} kl. {ref['klokkeslett']}\n")
print(
    df[
        ["dato", "klokkeslett", "distance_km", "duration_min", "staticDuration_min", "trafikkforsinkelse_min",
         "duration_min_diff_mot_ref", "samme_rute_som_ref"]
    ].to_string(index=False)
)

print("\nReell kjøretid (min) per dato og klokkeslett:")
print(df.pivot(index="dato", columns="klokkeslett", values="duration_min").to_string())
print("\nStatisk kjøretid (min) per dato og klokkeslett:")
print(df.pivot(index="dato", columns="klokkeslett", values="staticDuration_min").to_string())

print(
    f"\nReell kjøretid: {df['duration_min'].min():.1f}–{df['duration_min'].max():.1f} min "
    f"(spenn {df['duration_min'].max() - df['duration_min'].min():.1f} min)"
)
print(f"Statisk kjøretid: {df['staticDuration_min'].min():.1f}–{df['staticDuration_min'].max():.1f} min")
print(f"Avstand: {df['distance_km'].min():.2f}–{df['distance_km'].max():.2f} km")

# %% Lagre
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
output_file = OUTPUT_DIR / f"{o_file}_{d_file}_{datetime.now():%Y%m%d_%H%M%S}.csv"
df.to_csv(output_file, index=False, sep=";", decimal=",", encoding="utf-8-sig")
print(f"\nLagret {len(df)} rader til {output_file}")
