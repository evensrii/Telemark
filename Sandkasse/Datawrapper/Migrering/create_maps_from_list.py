import glob
import os
import shutil
import tempfile
from urllib.parse import quote

import pandas as pd

from datawrapper_generelt import API_BASE, get_session, get_team_id
from datawrapper_map import create_map

## Migrering: create the maps listed in "Kart med titler og steps.csv" (one map per row), formatted according to
## Formatering/formatering_map.txt, each in the Datawrapper folder that mirrors its query script's folder.
## Created maps are recorded in kart_opprettet.csv; a rerun updates those maps instead of creating new ones.

HERE = os.path.dirname(os.path.abspath(__file__))
MAP_LIST = os.path.join(HERE, "Kart med titler og steps.csv")  # Script;When two output files;Title;Description;Steps
RESULTS = os.path.join(HERE, "kart_opprettet.csv")
PUBLISH = True  # live data is only refreshed automatically after the first publish
DRY_RUN = False  # True: only check each row (script, dw_ file online, folder), create nothing
DEFAULT_STEPS = 5

QUERIES = os.path.join(os.environ["PYTHONPATH"], "Queries")
RAW_BASE = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/"

## dw_ file (path in the repo) per map script. Scripts with two maps are listed per file in the map list
## ("When two output files").
DW_FILES = {
    "forsørgerevne.py": "Data/01_Befolkning/Alderssammensetning/dw_forsørgerevne.csv",
    "andel_67_pluss.py": "Data/01_Befolkning/Befolkningsframskrivinger/dw_andel_67_pluss.csv",
    "befolkningsvekst.py": "Data/01_Befolkning/Befolkningsutvikling/dw_befolkningsvekst.csv",
    "aleneboende.py": "Data/01_Befolkning/Husholdninger/dw_aleneboende.csv",
    "andel_unge_utenfor_kommuner.py": "Data/02_Opplæring og kompetanse/Utenforskap/dw_andel_unge_utenfor_kommuner.csv",
    "sysselsetting_i_kommunene.py": "Data/03_Arbeid og næringsliv/01_Arbeidsliv/Sysselsetting/dw_sysselsetting_i_kommunene.csv",
    "muskel_og_skjelettplager_lidelser_kontakt_med_primaerhelsetjenesten_ettaarige_tall.py":
        "Data/08_Folkehelse og levekår/Helsetilstand/Sykdomsgrupper/dw_muskel_og_skjelettplager_lidelser_kontakt_med_primaerhelsetjenesten_ettaarige_tall.csv",
    "influensavaksinerte_over_65_aar.py": "Data/08_Folkehelse og levekår/Helsetilstand/Vaksinasjon/dw_influensavaksinerte_over_65_aar.csv",
    "luftforurensning.py": "Data/08_Folkehelse og levekår/Miljø/dw_luftforurensning.csv",
    "valgdeltakelse_kommunevalg.py": "Data/08_Folkehelse og levekår/Miljø/dw_valgdeltakelse_kommunevalg.csv",
    "valgdeltakelse_stortingsvalg.py": "Data/08_Folkehelse og levekår/Miljø/dw_valgdeltakelse_stortingsvalg.csv",
    "inntektsulikhet.py": "Data/08_Folkehelse og levekår/Oppvekst og levekår/Levekår/Inntekt og gjeld/dw_inntektsulikhet.csv",
    "vedvarende_lavinntekt_etter_innvandringskategori_kommunegrense.py":
        "Data/08_Folkehelse og levekår/Oppvekst og levekår/Levekår/Inntekt og gjeld/",  # + file from the list
    "bor_trangt.py": "Data/08_Folkehelse og levekår/Oppvekst og levekår/Levekår/Trangboddhet/dw_bor_trangt.csv",
    "pedagognorm.py": "Data/08_Folkehelse og levekår/Oppvekst og levekår/dw_pedagognorm_barnehager.csv",
    "andel_sysselsatte_innvandrere.py": "Data/09_Innvandrere og inkludering/Arbeid og inntekt/dw_andel_sysselsatte_innvandrere.csv",
    "andel_flyktninger_og_arbeidsinnvandrere.py": "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen/",  # + file
    "innvandrere_bosatt.py": "Data/09_Innvandrere og inkludering/Innvandrerbefolkningen/dw_andel_innvandrere_bosatt.csv",
}


def find_script(name):
    """Full path of a query script; names shortened with "…" match on the start and end."""
    name = name.strip()
    candidates = [p for p in glob.glob(os.path.join(QUERIES, "**", "*.py"), recursive=True)
                  if "Backup" not in p and not os.path.basename(p).startswith("bckp")]
    if "…" in name:
        start, end = name.split("…", 1)
        found = [p for p in candidates if os.path.basename(p).startswith(start) and os.path.basename(p).endswith(end)]
    else:
        found = [p for p in candidates if os.path.basename(p) == name]
    if len(found) > 1:  # same name in several folders (e.g. FHI/fylkessammenligning): the one writing a dw_ file
        found = [p for p in found if "handle_dw_map_output(" in open(p, encoding="utf-8", errors="ignore").read()]
    if len(found) != 1:
        raise ValueError(f"Script '{name}': {len(found)} matches")
    return found[0]


def find_folder(session, script_path):
    """Datawrapper folder mirroring the script's folder under Queries: topic folder by number (01_ ...),
    then the subfolders by name, skipping "scripts" (removed in Datawrapper)."""
    parts = os.path.relpath(os.path.dirname(script_path), QUERIES).split(os.sep)
    team = get_team_id(session)
    entry = next(e for e in session.get(f"{API_BASE}/folders").json()["list"]
                 if e.get("type") == "team" and e["id"] == team)
    folder = next(f for f in entry["folders"] if f["name"].split("_")[0] == parts[0].split("_")[0])
    path = [folder["name"]]
    for name in parts[1:]:
        if name == "scripts":
            continue
        folder = next((c for c in folder.get("folders", []) if c["name"] == name), None)
        if folder is None:
            raise ValueError(f"Datawrapper folder missing: {' / '.join(path + [name])}")
        path.append(name)
    return folder["id"], " / ".join(path)


## Read the map list (copied first, as Excel locks the file while it is open)
with tempfile.TemporaryDirectory() as tmp:
    copy = os.path.join(tmp, "list.csv")
    shutil.copy(MAP_LIST, copy)
    try:
        maps = pd.read_csv(copy, sep=";", dtype=str, encoding="utf-8-sig").fillna("")
    except UnicodeDecodeError:
        maps = pd.read_csv(copy, sep=";", dtype=str, encoding="cp1252").fillna("")
maps.columns = [c.strip() for c in maps.columns]

done = pd.read_csv(RESULTS, dtype=str).fillna("") if os.path.exists(RESULTS) else pd.DataFrame(
    columns=["dw_file", "title", "chart_id", "folder", "published"])

session = get_session()
for _, row in maps.iterrows():
    script_path = find_script(row["Script"])
    script_name = os.path.basename(script_path)
    dw_file = DW_FILES[script_name]
    if dw_file.endswith("/"):
        dw_file += row["When two output files"].strip()
    title = " ".join(row["Title"].split())
    steps = int(row["Steps"]) if row["Steps"].strip() else DEFAULT_STEPS
    existing = done.loc[done["dw_file"] == dw_file, "chart_id"]
    chart_id = existing.iloc[0] if len(existing) else None
    if chart_id:
        # Existing map: keep it in whatever folder it is in now (folders may have been reorganised in Datawrapper)
        folder_id, folder_path = None, done.loc[done["dw_file"] == dw_file, "folder"].iloc[0] + " (unchanged)"
    else:
        folder_id, folder_path = find_folder(session, script_path)

    print(f"\n=== {title}\n    {dw_file} -> {folder_path} ({steps} steps){' [update ' + chart_id + ']' if chart_id else ''}")
    if DRY_RUN:
        header = pd.read_csv(RAW_BASE + quote(dw_file), nrows=1).columns.tolist()
        print(f"    OK: {script_name} | columns {header} | description: {row['Description'].strip()[:60]!r}")
        continue
    try:
        chart_id = create_map(
            session,
            csv_url=RAW_BASE + quote(dw_file),
            title=title,
            chart_id=chart_id,
            query_script=os.path.relpath(script_path, os.environ["PYTHONPATH"]),
            num_steps=steps,
            description=row["Description"].strip(),
            folder_id=folder_id,
            publish=PUBLISH,
        )
    except Exception as e:
        print(f"ERROR: {e}")
        continue
    done = pd.concat([done[done["dw_file"] != dw_file], pd.DataFrame([{
        "dw_file": dw_file, "title": title, "chart_id": chart_id, "folder": folder_path, "published": PUBLISH}])])
    done.to_csv(RESULTS, index=False, encoding="utf-8")

print(f"\n{len(done)} maps recorded in {RESULTS}")
