"""
Reisetidsanalyse tannhelse: kjøretid og avstand fra grunnkretser (origins) til
tannklinikker (destinasjoner) via Google Routes API (Compute Route Matrix).

SKU: Compute Route Matrix Pro (utløses av routingPreference = TRAFFIC_AWARE_OPTIMAL)
Travel mode: DRIVE | Traffic model: BEST_GUESS
Output: distanceMeters, duration, staticDuration

Inndata:
- Inndata/origins_grunnkretser_m_befolkning.csv        (OriginID = OBJECTID_KOPI)
- Inndata/destinations_tannklinikker.csv  (DestinationID = OBJECTID_KOPI)

Utdata: Utdata/Matriser/reisetid[_test]_<tidsstempel>.csv, samme struktur som
Utdata/output_example_fra_eirik.csv (Total_*-kolonnene erstattet av Google-feltene, uten Shape_Length).

Krever GOOGLE_ROUTES_API_KEY i token.env (Python-mappen).
Dok: https://developers.google.com/maps/documentation/routes/compute_route_matrix

NB: Det faktureres per element (origin x destinasjon). Forespørslene grupperes per
destinasjon (N origins x 1 destinasjon), slik at kun parene i OD-tabellen faktureres.
"""

import os
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

# Last API-nøkkel fra token.env (ligger i PYTHONPATH-mappen, dvs. Python/)
load_dotenv(os.path.join(os.environ["PYTHONPATH"], "token.env"), override=True)
API_KEY = os.getenv("GOOGLE_ROUTES_API_KEY")
if not API_KEY:
    raise RuntimeError("GOOGLE_ROUTES_API_KEY mangler i token.env")

BASE_DIR = Path(os.environ["PYTHONPATH"]) / "Queries" / "Fagseksjoner" / "Tannhelse" / "Reisetidsanalyse"
ORIGINS_FILE = BASE_DIR / "Inndata" / "origins_grunnkretser_m_befolkning.csv"
DESTINATIONS_FILE = BASE_DIR / "Inndata" / "destinations_tannklinikker.csv"
OUTPUT_DIR = BASE_DIR / "Utdata" / "Matriser"

# Test: kjør kun de N første origins i origins-filen mot alle destinasjoner (N x 13 elementer).
# None = full matrise, alle grunnkretser x alle klinikker (612 x 13 = 7 956 elementer).
# Ved test merkes utdata med "_test", siden det ikke er en full matrise.
MAX_ORIGINS = None
IS_TEST = MAX_ORIGINS is not None

URL = "https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix"
FIELD_MASK = "originIndex,destinationIndex,status,condition,distanceMeters,duration,staticDuration"

# Avreisetidspunkt (RFC3339, må være i fremtiden). None = nå. Trafikkmodellen beregnes ut fra dette tidspunktet.
# Tirsdag 8. desember 2026 kl. 09:00 norsk vintertid (CET, UTC+1)
DEPARTURE_TIME = "2026-12-08T09:00:00+01:00"

# Maks elementer per forespørsel med TRAFFIC_AWARE_OPTIMAL er 100
MAX_ELEMENTS = 100

# Kvote: 3 000 elementer per minutt (ingen dagsgrense). Holder oss godt under for å ha margin.
ELEMENTS_PER_MINUTE = 2500
# Ved 429 (kvote) eller 5xx: vent og prøv igjen, med økende ventetid (sekunder)
RETRY_WAITS_S = [30, 60, 120, 240]

# Mellomlagring: hver vellykket forespørsel lagres straks til en cache-fil per avreisetidspunkt.
# Ved ny kjøring hoppes allerede hentede par over, slik at et avbrudd ikke koster nye spørringer.
# Slett cache-filen for å tvinge ny henting av alle par.
CACHE_DIR = OUTPUT_DIR / "cache"
CACHE_FILE = CACHE_DIR / f"routes_{(DEPARTURE_TIME or 'naa').replace(':', '').replace('+', 'p')}.csv"

# Scenario: klinikker som legges ned, og hvilken klinikk deres opptaksområde flyttes til.
# Klinikknavn = kortnavn som i kolonnen "Klinikk" i origins-filen (dvs. "<navn> Tannklinikk" uten suffiks).
SCENARIO_NAME = "nedleggelse_stathelle_fyresdal_treungen_siljan"
REDIRECT = {
    "Stathelle": "Porsgrunn",
    "Fyresdal": "Vinje",
    "Treungen": "Drangedal",
    "Siljan": "Skien",
}
CLOSED = list(REDIRECT)

TIME_COL = "duration_min"  # eller "staticDuration_min" (uten trafikk)
WEIGHT_COL = "totalbefolkning"
THRESHOLDS_MIN = [30, 45, 60]
PERCENTILES = [50, 75, 90, 95]  # befolkningsvektede persentiler
MIN_GAIN_MIN = 2  # minste tidsgevinst (min) for at en annen åpen klinikk regnes som bedre alternativ
SCENARIO_DIR = BASE_DIR / "Utdata" / "Scenarier"

OUTPUT_COLUMNS = [
    "Name",
    "OriginID",
    "DestinationID",
    "distance_km",
    "duration_min",
    "staticDuration_min",
    "grunnkretsnummer_Origin",
    "grunnkretsnavn_Origin",
    "kommunenavn_Origin",
    "kommunenummer_Origin",
    "totalbefolkning",
    "antallmenn",
    "antallkvinner",
    "Klinikk_Idag",
    "Klinikk_destinasjon",
    "Grunnkrets_destinasjon",
    "departure_time",
]


def waypoint(lat, lon):
    return {"waypoint": {"location": {"latLng": {"latitude": lat, "longitude": lon}}}}


def compute_route_matrix(origins, destinations):
    body = {
        "origins": [waypoint(lat, lon) for lat, lon in origins],
        "destinations": [waypoint(lat, lon) for lat, lon in destinations],
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE_OPTIMAL",
        "trafficModel": "BEST_GUESS",
    }
    if DEPARTURE_TIME:
        body["departureTime"] = DEPARTURE_TIME

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    for attempt, wait_s in enumerate([*RETRY_WAITS_S, None], start=1):
        try:
            response = requests.post(URL, json=body, headers=headers, timeout=60)
        except requests.RequestException as e:  # nettverksfeil, tidsavbrudd o.l.
            response, error = None, str(e)
        else:
            if response.ok:
                return response.json()
            error = f"{response.status_code}: {response.text}"
            if response.status_code != 429 and response.status_code < 500:
                break  # feil i forespørselen (f.eks. 400), nytt forsøk hjelper ikke
        if wait_s is None:
            break
        print(f"  Forsøk {attempt} feilet ({error.splitlines()[0]}). Venter {wait_s} s ...")
        time.sleep(wait_s)
    raise RuntimeError(f"Routes API feilet ({error})")


def parse_seconds(value):
    # Varighet returneres som streng, f.eks. "1234s"
    return int(value.rstrip("s")) if isinstance(value, str) else None


# %% Les origins og destinasjoner
read_opts = dict(sep=";", decimal=",", encoding="utf-8-sig")
origins = pd.read_csv(ORIGINS_FILE, **read_opts)
destinations = pd.read_csv(DESTINATIONS_FILE, **read_opts)

origins["ID"] = origins["OBJECTID_KOPI"].astype(int)
destinations["ID"] = destinations["OBJECTID_KOPI"].astype(int)
for col in ["totalbefolkning", "antallmenn", "antallkvinner"]:
    origins[col] = origins[col].astype("Int64")

print(f"{len(origins)} origins, {len(destinations)} destinasjoner")

# %% Bygg OD-tabell (OriginID, DestinationID): origins x alle destinasjoner
od_origins = origins[["ID"]] if MAX_ORIGINS is None else origins[["ID"]].head(MAX_ORIGINS)
od = od_origins.merge(destinations[["ID"]], how="cross", suffixes=("_o", "_d"))
od.columns = ["OriginID", "DestinationID"]

# Par som allerede er hentet (cache fra tidligere, avbrutt kjøring) hoppes over
CACHE_DIR.mkdir(parents=True, exist_ok=True)
cached = (
    pd.read_csv(CACHE_FILE)
    if CACHE_FILE.exists()
    else pd.DataFrame({"OriginID": pd.Series(dtype=int), "DestinationID": pd.Series(dtype=int)})
)
todo = od.merge(cached[["OriginID", "DestinationID"]], how="left", indicator=True)
todo = todo[todo["_merge"] == "left_only"].drop(columns="_merge")

print(f"{len(od)} elementer totalt, {len(od) - len(todo)} allerede i cache ({CACHE_FILE.name})")
print(f"{len(todo)} elementer vil bli fakturert, ca. {len(todo) / ELEMENTS_PER_MINUTE:.1f} min")

# %% Hent ruter (gruppert per destinasjon, N origins x 1 destinasjon per forespørsel)
# Hver forespørsel lagres straks i cache-filen. Feiler skriptet, kjør denne cellen på nytt for å fortsette.
o_coords = origins.set_index("ID")[["Latitude", "Longitude"]]
d_coords = destinations.set_index("ID")[["Latitude", "Longitude"]]

done = 0
for dest_id, group in todo.groupby("DestinationID"):
    dest = tuple(d_coords.loc[dest_id])
    for start in range(0, len(group), MAX_ELEMENTS):
        origin_ids = group["OriginID"].iloc[start : start + MAX_ELEMENTS].tolist()
        t0 = time.monotonic()
        elements = compute_route_matrix([tuple(o_coords.loc[i]) for i in origin_ids], [dest])

        batch = pd.DataFrame(
            [
                {
                    "OriginID": origin_ids[el.get("originIndex", 0)],
                    "DestinationID": dest_id,
                    "condition": el.get("condition"),
                    "status": el.get("status", {}).get("message"),
                    "distanceMeters": el.get("distanceMeters"),
                    "duration_s": parse_seconds(el.get("duration")),
                    "staticDuration_s": parse_seconds(el.get("staticDuration")),
                }
                for el in elements
                # Elementer med feilstatus (f.eks. midlertidig feil) lagres ikke, og hentes på nytt neste gang
                if not el.get("status", {}).get("code")
            ]
        )
        if not batch.empty:
            batch.to_csv(CACHE_FILE, mode="a", header=not CACHE_FILE.exists(), index=False)
        done += len(origin_ids)
        print(f"  Destinasjon {dest_id}: {done}/{len(todo)} elementer hentet")

        # Strup tempoet slik at vi holder oss under ELEMENTS_PER_MINUTE
        time.sleep(max(0, len(origin_ids) * 60 / ELEMENTS_PER_MINUTE - (time.monotonic() - t0)))

routes = pd.read_csv(CACHE_FILE).merge(od, on=["OriginID", "DestinationID"])
missing_pairs = len(od) - len(routes)
if missing_pairs:
    print(f"Advarsel: {missing_pairs} par mangler fortsatt (feilstatus fra API). Kjør cellen over på nytt.")

not_found = routes[routes["condition"] != "ROUTE_EXISTS"]
if not not_found.empty:
    print(f"Advarsel: {len(not_found)} par uten rute:")
    print(not_found[["OriginID", "DestinationID", "condition", "status"]])

routes.head()

# %% Koble på attributter og bygg utdata-struktur
origin_attrs = origins.rename(
    columns={
        "ID": "OriginID",
        "grunnkretsnummer": "grunnkretsnummer_Origin",
        "grunnkretsnavn": "grunnkretsnavn_Origin",
        "kommunenavn": "kommunenavn_Origin",
        "kommunenummer": "kommunenummer_Origin",
        "Klinikk": "Klinikk_Idag",
    }
)
dest_attrs = destinations.rename(
    columns={
        "ID": "DestinationID",
        "firfirmanavn1": "Klinikk_destinasjon",
        "grunnkretsnr": "Grunnkrets_destinasjon",
    }
)

df = routes.merge(origin_attrs, on="OriginID", how="left").merge(dest_attrs, on="DestinationID", how="left")
df["Name"] = "Location " + df["OriginID"].astype(str) + " - Location " + df["DestinationID"].astype(str)
df["distance_km"] = (df["distanceMeters"] / 1000).round(3)
df["duration_min"] = (df["duration_s"] / 60).round(2)
df["staticDuration_min"] = (df["staticDuration_s"] / 60).round(2)
df["departure_time"] = DEPARTURE_TIME or datetime.now().astimezone().isoformat(timespec="seconds")

df = df.sort_values(["OriginID", "DestinationID"])[OUTPUT_COLUMNS].reset_index(drop=True)
df.head(20)

# %% Lagre (samme format som output_example_fra_eirik.csv: semikolon, desimalkomma)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
prefix = "reisetid_test" if IS_TEST else "reisetid"
output_file = OUTPUT_DIR / f"{prefix}_{datetime.now():%Y%m%d_%H%M%S}.csv"
df.to_csv(output_file, index=False, sep=";", decimal=",", encoding="utf-8-sig")
print(f"Lagret {len(df)} rader til {output_file}")


# %% ---------------------------------------------------------------------------
# SCENARIOANALYSE
# Bruker matrisen i df. For å analysere en tidligere kjørt matrise uten ny spørring:
# kjør første celle (innlesing av origins/destinasjoner), deretter
#   df = pd.read_csv(OUTPUT_DIR / "<fil>.csv", **read_opts)
# og så cellene herfra og ned.
# ------------------------------------------------------------------------------


def weighted_quantile(values, weights, q):
    # Befolkningsvektet persentil: verdien der andel q av innbyggerne har lik eller lavere verdi.
    # Hver grunnkrets teller like mye som antall innbyggere (3 innbyggere = 3, 300 innbyggere = 300).
    v, w = np.asarray(values, dtype=float), np.asarray(weights, dtype=float)
    keep = ~np.isnan(v) & (w > 0)
    v, w = v[keep], w[keep]
    if w.sum() == 0:
        return np.nan
    order = np.argsort(v)
    v, w = v[order], w[order]
    return v[np.searchsorted(np.cumsum(w), q * w.sum())]


def weighted_stats(g, value_col):
    g = g.dropna(subset=[value_col])
    w = g[WEIGHT_COL].fillna(0)
    stats = {
        "grunnkretser": len(g),
        "innbyggere": int(w.sum()),
        "snitt_min": round(np.average(g[value_col], weights=w), 2) if w.sum() > 0 else np.nan,
    }
    for p in PERCENTILES:
        stats[f"p{p}_min"] = weighted_quantile(g[value_col], w, p / 100)
    stats["maks_min"] = g.loc[w > 0, value_col].max()  # høyeste verdi blant bebodde grunnkretser
    return pd.Series(stats)


def save_scenario_table(table, name):
    table.to_csv(scenario_out / f"{name}.csv", index=False, sep=";", decimal=",", encoding="utf-8-sig")


# %% Valider scenario og bygg tabell per grunnkrets
clinics = destinations["firfirmanavn1"].str.removesuffix(" Tannklinikk")
unknown = (set(REDIRECT) | set(REDIRECT.values())) - set(clinics)
if unknown:
    raise ValueError(f"Ukjente klinikknavn i REDIRECT: {unknown}")
if set(REDIRECT.values()) & set(CLOSED):
    raise ValueError("Et opptaksområde flyttes til en klinikk som også legges ned")
open_clinics = [c for c in clinics if c not in CLOSED]

# Reisetid per grunnkrets x klinikk (bred tabell, kolonner = kortnavn)
m = df.assign(Klinikk=df["Klinikk_destinasjon"].str.removesuffix(" Tannklinikk"))
T = m.pivot_table(index="OriginID", columns="Klinikk", values=TIME_COL)
T_long = T.stack()

gk = origins[
    ["ID", "grunnkretsnummer", "grunnkretsnavn", "kommunenummer", "kommunenavn", WEIGHT_COL, "Klinikk"]
].rename(columns={"ID": "OriginID", "Klinikk": "Klinikk_Idag"})
gk["Klinikk_scenario"] = gk["Klinikk_Idag"].replace(REDIRECT)
gk["berort"] = gk["Klinikk_Idag"].isin(CLOSED)
gk["tid_idag"] = T_long.reindex(list(zip(gk["OriginID"], gk["Klinikk_Idag"]))).to_numpy()
gk["tid_scenario"] = T_long.reindex(list(zip(gk["OriginID"], gk["Klinikk_scenario"]))).to_numpy()
gk["endring_min"] = gk["tid_scenario"] - gk["tid_idag"]

# Raskeste åpne klinikk i scenarioet, og raskeste klinikk i dag (blant klinikkene i matrisen)
for label, cols in [("naermeste_aapen", [c for c in open_clinics if c in T.columns]), ("naermeste_idag", list(T.columns))]:
    sub = T[cols].dropna(how="all")
    gk = gk.merge(
        pd.DataFrame({f"Klinikk_{label}": sub.idxmin(axis=1), f"tid_{label}": sub.min(axis=1)}),
        left_on="OriginID",
        right_index=True,
        how="left",
    )
gk["gevinst_naermeste_aapen_min"] = gk["tid_scenario"] - gk["tid_naermeste_aapen"]

missing = gk["tid_idag"].isna() | gk["tid_scenario"].isna()
if missing.any():
    print(
        f"NB: {missing.sum()} av {len(gk)} grunnkretser mangler reisetid i dag eller i scenarioet "
        f"(ingen rute, eller ikke med i matrisen ved MAX_ORIGINS). "
        f"Innbyggere i disse: {int(gk.loc[missing, WEIGHT_COL].fillna(0).sum())}"
    )

scenario_out = SCENARIO_DIR / (SCENARIO_NAME + ("_test" if IS_TEST else ""))
scenario_out.mkdir(parents=True, exist_ok=True)
save_scenario_table(gk.sort_values("OriginID"), "grunnkretser")
gk[gk["berort"]].head()

# %% Steg 1: Hvor mange berøres, og hvor mye lengre reiser de?
affected = gk[gk["berort"]]
per_clinic = affected.groupby("Klinikk_Idag").apply(weighted_stats, value_col="endring_min", include_groups=False)
per_clinic.loc["Alle berørte"] = weighted_stats(affected, "endring_min")
per_clinic = per_clinic.reset_index().rename(columns={"Klinikk_Idag": "Nedlagt klinikk"})
per_clinic[["grunnkretser", "innbyggere"]] = per_clinic[["grunnkretser", "innbyggere"]].astype(int)
per_clinic.insert(1, "Ny klinikk", per_clinic["Nedlagt klinikk"].map(REDIRECT))

per_kommune = (
    affected.groupby("kommunenavn")
    .apply(weighted_stats, value_col="endring_min", include_groups=False)
    .astype({"grunnkretser": int, "innbyggere": int})
    .reset_index()
)

save_scenario_table(per_clinic, "steg1_endring_per_klinikk")
save_scenario_table(per_kommune, "steg1_endring_per_kommune")
print("Steg 1 – endring i reisetid (min, befolkningsvektet) for berørte grunnkretser:")
print(per_clinic.to_string(index=False))

# %% Steg 2: Hvem får uakseptabelt lang reisetid?
valid = gk.dropna(subset=["tid_idag", "tid_scenario"])
w = valid[WEIGHT_COL].fillna(0)
thresholds = pd.DataFrame(
    [
        {
            "terskel_min": t,
            "innbyggere_over_idag": int(w[valid["tid_idag"] > t].sum()),
            "innbyggere_over_scenario": int(w[valid["tid_scenario"] > t].sum()),
        }
        for t in THRESHOLDS_MIN
    ]
)
thresholds["endring"] = thresholds["innbyggere_over_scenario"] - thresholds["innbyggere_over_idag"]
thresholds["andel_over_scenario_pct"] = (100 * thresholds["innbyggere_over_scenario"] / w.sum()).round(1)

# Befolkningsvektet fordeling av reisetid i dag vs. i scenarioet, for hele fylket og for berørte grunnkretser
percentiles = pd.DataFrame(
    [
        {"utvalg": utvalg, "situasjon": situasjon, **weighted_stats(data, col)}
        for utvalg, data in [("Hele fylket", valid), ("Berørte grunnkretser", valid[valid["berort"]])]
        for situasjon, col in [("I dag", "tid_idag"), ("Scenario", "tid_scenario")]
    ]
).astype({"grunnkretser": int, "innbyggere": int})

worst = (
    affected[affected[WEIGHT_COL] > 0]
    .sort_values("tid_scenario", ascending=False)
    .head(25)[["grunnkretsnummer", "grunnkretsnavn", "kommunenavn", WEIGHT_COL, "Klinikk_Idag", "Klinikk_scenario",
               "tid_idag", "tid_scenario", "endring_min"]]
)

save_scenario_table(thresholds, "steg2_terskler")
save_scenario_table(percentiles, "steg2_persentiler_reisetid")
save_scenario_table(worst, "steg2_lengst_reisetid")
print(f"Steg 2 – innbyggere over terskel (av {int(w.sum())} med reisetid i matrisen):")
print(thresholds.to_string(index=False))
print("\nBefolkningsvektet reisetid (min):")
print(percentiles.to_string(index=False))

# %% Steg 3: Er den foreslåtte omfordelingen den beste?
better = affected[
    (affected["Klinikk_naermeste_aapen"] != affected["Klinikk_scenario"])
    & (affected["gevinst_naermeste_aapen_min"] >= MIN_GAIN_MIN)
].sort_values("gevinst_naermeste_aapen_min", ascending=False)[
    ["grunnkretsnummer", "grunnkretsnavn", "kommunenavn", WEIGHT_COL, "Klinikk_Idag", "Klinikk_scenario",
     "tid_scenario", "Klinikk_naermeste_aapen", "tid_naermeste_aapen", "gevinst_naermeste_aapen_min"]
]
better_summary = (
    better.groupby(["Klinikk_Idag", "Klinikk_scenario", "Klinikk_naermeste_aapen"])
    .apply(weighted_stats, value_col="gevinst_naermeste_aapen_min", include_groups=False)
    .astype({"grunnkretser": int, "innbyggere": int})
    .reset_index()
    if not better.empty
    else pd.DataFrame()
)

save_scenario_table(better, "steg3_bedre_alternativ_grunnkretser")
save_scenario_table(better_summary, "steg3_bedre_alternativ_oppsummering")
print(f"Steg 3 – berørte grunnkretser der en annen åpen klinikk er minst {MIN_GAIN_MIN} min raskere:")
print(better_summary.to_string(index=False) if not better_summary.empty else "Ingen")

# %% Steg 4: Hvor mye økt belastning får mottakerklinikkene?
# Basert på opptaksområder (alle grunnkretser), uavhengig av matrisen. Alternativ = raskeste åpne klinikk.
load = pd.DataFrame(
    {
        "innbyggere_idag": gk.groupby("Klinikk_Idag")[WEIGHT_COL].sum(),
        "innbyggere_scenario": gk.groupby("Klinikk_scenario")[WEIGHT_COL].sum(),
        "innbyggere_naermeste_aapen": gk.groupby("Klinikk_naermeste_aapen")[WEIGHT_COL].sum(),
    }
).reindex(clinics).fillna(0).astype(int)
load["endring_scenario"] = load["innbyggere_scenario"] - load["innbyggere_idag"]
load["endring_scenario_pct"] = (100 * load["endring_scenario"] / load["innbyggere_idag"]).round(1)
load["status"] = np.where(load.index.isin(CLOSED), "nedlagt", "åpen")
load = load.rename_axis("Klinikk").reset_index().sort_values("endring_scenario", ascending=False)

save_scenario_table(load, "steg4_belastning")
print("Steg 4 – innbyggere per klinikk:")
print(load.to_string(index=False))
print(f"\nScenarioresultater lagret i {scenario_out}")
