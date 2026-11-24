import os

import pandas as pd

from Helper_scripts.utility_functions import fetch_data

## Antall felte elg og hjort i Telemark per jaktår (SSB tabell 03432 og 03434).
## Jaktåret ("2025-2026") vises som enkeltår (startåret, 2025), slik at x-aksen i Datawrapper viser enkeltår.
## Resultatet skrives til CSV i denne mappen.

URLS = {
    "Elg": "https://data.ssb.no/api/pxwebapi/v2/tables/03432/data?lang=no&valueCodes[ContentsCode]=Feltedyr&valueCodes[Tid]=*&valueCodes[Region]=F-40&valueCodes[Dyr]=00a&valueCodes[Kjonn]=0&codelist[Region]=agg_KommFylker&outputValues[Region]=aggregated",
    "Hjort": "https://data.ssb.no/api/pxwebapi/v2/tables/03434/data?lang=no&valueCodes[ContentsCode]=Feltedyr&valueCodes[Tid]=*&valueCodes[Region]=F-40&valueCodes[Dyr]=00a&valueCodes[Kjonn]=0&codelist[Region]=agg_KommFylker&outputValues[Region]=aggregated",
}

OUTPUT_FOLDER = os.path.join(os.environ["PYTHONPATH"], "Opplæring", "PANDA", "2026 Oslo", "Datawrapper")
FILE_NAME = "felt_elg_og_hjort_telemark.csv"

error_messages = []
frames = {}

for animal, url in URLS.items():
    try:
        frames[animal] = fetch_data(
            url=url,
            payload=None,
            error_messages=error_messages,
            query_name=f"Felte {animal.lower()}",
            response_type="json",
        )
    except Exception as e:
        print(f"Error occurred: {e}")
        raise


######### Manuell bearbeiding #########

for animal, df in frames.items():
    print(animal, df.columns.tolist())
    print(df.head())

## Tidskolonnen er jaktår ("2025-2026"); behold startåret som enkeltår
series = []
for animal, df in frames.items():
    time_col = next(c for c in df.columns if df[c].astype(str).str.match(r"^\d{4}-\d{4}$").all())
    s = df[[time_col, "value"]].copy()
    s["År"] = s[time_col].str[:4].astype(int)
    series.append(s.set_index("År")["value"].rename(animal))

result = pd.concat(series, axis=1).sort_index().reset_index()
result[["Elg", "Hjort"]] = result[["Elg", "Hjort"]].astype("Int64")

## SSB oppgir 0 felte hjort i jaktåret 2008-2009 (350 året før, 485 året etter), uten statusflagg.
## Tolkes som manglende data og settes tomt, så linjen får et brudd i stedet for et fall til 0.
result.loc[(result["År"] == 2008) & (result["Hjort"] == 0), "Hjort"] = pd.NA

print(result.to_string())

result.to_csv(os.path.join(OUTPUT_FOLDER, FILE_NAME), index=False, encoding="utf-8")
print(f"Skrevet til {os.path.join(OUTPUT_FOLDER, FILE_NAME)}")
