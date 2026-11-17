import io
import json
import os
import re
import shutil
import sys
import tempfile
from urllib.parse import quote

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.append(os.environ["PYTHONPATH"])
import datawrapper_generelt as dwg  # noqa: E402
from datawrapper_generelt import API_BASE, get_session  # noqa: E402
from Helper_scripts.github_functions import upload_github_file  # noqa: E402
from comment_extras import extras_from_comment  # noqa: E402

## Migrering: create the graphs whose data was typed directly into Everviz (Status "Data lagt inn direkte i Everviz
## (ingen kobling)" in the figure overview), using the create_<type>.py script for each graph type. Not published.
## - Data: read from the Everviz embed script (https://app.everviz.com/inject/<id>/), saved as a comma CSV named
##   "statiske_data_<title>.csv" and uploaded to GitHub (Data/..., folder from data_folder()), so the chart can link
##   to it as live data like the other graphs.
## - Title from "Tittel (forslag)", type from "Type (forslag)", optional extras from "Comment" (same as
##   create_graphs_from_list.py).
## - Folder: the Datawrapper folder "Statiske".
## Created graphs are recorded in statiske_grafer_opprettet.csv. A rerun skips them (UPDATE_EXISTING = False), so
## manual styling in Datawrapper is kept.

OVERVIEW = os.path.join(HERE, "Figuroversikt Kunnskap om Telemark (utkast).xlsx")
RESULTS = os.path.join(HERE, "statiske_grafer_opprettet.csv")
STATUS = "Data lagt inn direkte i Everviz (ingen kobling)"
DW_FOLDER_NAME = "Statiske"
FILE_PREFIX = "statiske_data_"
PUBLISH = False
UPDATE_EXISTING = False
ONLY = ["3mK7jIK1Y", "hTqkGuADu"]  # set to a list of Everviz ids to run only those rows, or None for all

RAW_BASE = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/"
TEMP_FOLDER = os.environ.get("TEMP_FOLDER") or os.path.join(os.environ["PYTHONPATH"], "Temp")

TYPE_SCRIPTS = {
    "Line graph": "create_line.py",
    "Column chart": "create_column.py",
    "Grouped column chart": "create_column_grouped.py",
    "Stacked column chart": "create_column_stacked.py",
    "Stacked column chart (100 %)": "create_column_stacked.py",  # stack percentages are chosen from the data
    "Bar chart": "create_bar.py",
    "Grouped bar chart": "create_bar_grouped.py",
    "Stacked bar chart": "create_bar_stacked.py",
    "Stacked bar chart (100 %)": "create_bar_stacked.py",
    "Pie chart": "create_donut.py",
    "Donut chart": "create_donut.py",
}

## Folkehelseundersøkelsen 2025 (pages under rapporter/folkehelseundersokelsen2025/): Data subfolder per page,
## source "Folkehelseundersøkelsen 2025" (link in sources.csv), all values are shares in whole percent
FHUS_URL_PART = "folkehelseundersokelsen2025"
FHUS_DATA_FOLDER = "Data/Folkehelseundersøkelsen/2025"
FHUS_SUBFOLDERS = {"kjennetegn": "Bakgrunnsvariabler", "om-undersokelsen": "Bakgrunnsvariabler"}
FHUS_SOURCE = "Folkehelseundersøkelsen 2025"

## Theme pages (tema/<slug>/...): numbered Data topic folder; the subfolder is the "Side" column
TEMA_FOLDERS = {
    "befolkning": "01_Befolkning",
    "opplaring-og-kompetanse": "02_Opplæring og kompetanse",
    "arbeid-og-naeringsliv": "03_Arbeid og næringsliv",
    "klima-og-ressursforvaltning": "04_Klima og ressursforvaltning",
    "mobilitet-og-infrastruktur": "05_Mobilitet og infrastruktur",
    "kultur-og-kulturarv": "06_Kultur og kulturarv",
    "idrett-og-friluftsliv": "07_Idrett_friluftsliv_og_frivillighet",
    "folkehelse-og-levekar": "08_Folkehelse og levekår",
    "innvandrere-og-inkludering": "09_Innvandrere og inkludering",
    "areal-og-stedsutvikling": "10_Areal- og stedsutvikling",
}


def slug(text):
    text = text.lower()
    for a, b in {"æ": "ae", "ø": "oe", "å": "aa"}.items():
        text = text.replace(a, b)
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def is_fhus(row):
    return FHUS_URL_PART in row["URL"]


def data_folder(row):
    """GitHub folder for the CSV, deduced from the page URL (and "Side")."""
    url = row["URL"]
    if is_fhus(row):
        page = url.rstrip("/").rsplit("/", 1)[-1]
        if page not in FHUS_SUBFOLDERS:
            raise ValueError(f"No Data subfolder set for Folkehelseundersøkelsen page '{page}' (FHUS_SUBFOLDERS)")
        return f"{FHUS_DATA_FOLDER}/{FHUS_SUBFOLDERS[page]}"
    m = re.search(r"/tema/([^/]+)/", url)
    if not m or m.group(1) not in TEMA_FOLDERS:
        raise ValueError(f"No Data folder known for page {url}")
    return f"Data/{TEMA_FOLDERS[m.group(1)]}/{row['Side'].strip()}"


def everviz_csv(everviz_id):
    """The data typed into the Everviz chart, read from its embed script, as a DataFrame."""
    response = requests.get(f"https://app.everviz.com/inject/{everviz_id}/?v=6", timeout=30)
    response.raise_for_status()
    js = response.content.decode("utf-8")
    start = js.find('"csv":')
    if start < 0:
        raise ValueError(f"No CSV data in Everviz chart {everviz_id} (linked to another source?)")
    text, _ = json.JSONDecoder().raw_decode(js[start + 6:])
    first_line = text.splitlines()[0]
    sep = max([";", "\t", ","], key=first_line.count)
    df = pd.read_csv(io.StringIO(text), sep=sep, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        values = df[c].str.strip()
        number = r"-?\d+(\.\d+)?" if sep == "," else r"-?\d+(,\d+)?"
        if values.dropna().str.fullmatch(number).all() and values.notna().any():
            df[c] = pd.to_numeric(values.str.replace(",", "."))
        else:
            df[c] = values
    return df


def find_dw_folder(session, name):
    """Id of the (first) team folder with this name, at any depth."""
    def search(folders):
        for f in folders:
            if f["name"] == name:
                return f["id"]
            found = search(f.get("folders", []))
            if found:
                return found
        return None
    for entry in session.get(f"{API_BASE}/folders").json()["list"]:
        found = search(entry.get("folders", []))
        if found:
            return found
    raise ValueError(f"Datawrapper folder '{name}' not found")


def run_type_script(script, settings):
    """Run a create_<type>.py script with its settings replaced; returns its variables (chart_id ...)."""
    src = open(os.path.join(HERE, script), encoding="utf-8").read()
    for key, value in settings.items():
        src, n = re.subn(rf"^{key} = .*$", f"{key} = {value!r}", src, count=1, flags=re.M)
        if n != 1:
            raise ValueError(f"{script}: setting {key} not found")
    namespace = {"__file__": os.path.join(HERE, script), "__name__": "__main__"}
    exec(compile(src, script, "exec"), namespace)
    return namespace


## Source and number formats per row (the type scripts call these from datawrapper_generelt)
_number_formats = dwg.number_formats


def set_row_rules(row):
    if is_fhus(row):
        dwg.find_data_source = lambda query_script: (FHUS_SOURCE, dwg.SOURCES_CSV_LINKS()[FHUS_SOURCE])
        dwg.number_formats = lambda values, title: _number_formats(values, "Andel " + title)
    else:
        def no_source(query_script):
            print("Warning: source unknown for data typed into Everviz, source left empty")
            return "", ""
        dwg.find_data_source = no_source
        dwg.number_formats = _number_formats


## Read the overview (copied first, as Excel locks the file while it is open)
with tempfile.TemporaryDirectory() as tmp:
    copy = os.path.join(tmp, "overview.xlsx")
    shutil.copy(OVERVIEW, copy)
    overview = pd.read_excel(copy, sheet_name="Figurer", dtype=str).fillna("")
rows = overview[overview["Status"].str.strip() == STATUS]
if ONLY:
    rows = rows[rows["Everviz-id"].isin(ONLY)]
print(f"{len(rows)} graphs to create")

done = pd.read_csv(RESULTS, dtype=str).fillna("") if os.path.exists(RESULTS) else pd.DataFrame(
    columns=["everviz_id", "title", "type", "chart_id", "csv"])



session = get_session()
dw_folder_id = find_dw_folder(session, DW_FOLDER_NAME)
print(f"Datawrapper folder '{DW_FOLDER_NAME}': {dw_folder_id}")

for _, row in rows.iterrows():
    everviz_id = row["Everviz-id"].strip()
    title = " ".join(row["Tittel (forslag)"].split())
    ctype = row["Type (forslag)"].strip()
    print(f"\n=== {title} ({ctype}, Everviz {everviz_id})")
    if row["Comment"].strip():
        print(f"Comment: {row['Comment'].strip()}")
    existing = done.loc[done["everviz_id"] == everviz_id, "chart_id"]
    chart_id = existing.iloc[0] if len(existing) else None
    if chart_id and not UPDATE_EXISTING:
        print(f"Skipped: already created as {chart_id}")
        continue
    try:
        if ctype not in TYPE_SCRIPTS:
            raise ValueError(f"Unknown graph type '{ctype}'")
        ## 1) Data from Everviz -> CSV on GitHub
        df = everviz_csv(everviz_id)
        print(df.to_string(max_rows=10))
        file_name = f"{FILE_PREFIX}{slug(title)}.csv"
        github_path = f"{data_folder(row)}/{file_name}"
        os.makedirs(TEMP_FOLDER, exist_ok=True)
        local_path = os.path.join(TEMP_FOLDER, file_name)
        df.to_csv(local_path, index=False, encoding="utf-8")
        upload_github_file(local_path, github_path, message=f"Added {file_name} - static data from Everviz")
        print(f"CSV: {github_path}")

        ## 2) Datawrapper chart
        set_row_rules(row)
        ns = run_type_script(TYPE_SCRIPTS[ctype], {
            "CSV_URL": RAW_BASE + quote(github_path), "CHART_TITLE": title, "CHART_ID": chart_id,
            "QUERY_SCRIPT": None})
        chart_id = ns["chart_id"]
        patch = {"metadata": {"describe": {"intro": row["Description"].strip()}}}
        extras = extras_from_comment(row["Comment"], ns)
        if extras:
            patch["metadata"]["visualize"] = extras
        if not len(existing):
            patch["folderId"] = dw_folder_id  # only when created; later moves in Datawrapper are kept
        session.patch(f"{API_BASE}/charts/{chart_id}", json=patch).raise_for_status()
        if PUBLISH:
            session.post(f"{API_BASE}/charts/{chart_id}/publish").raise_for_status()
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        continue
    done = pd.concat([done[done["everviz_id"] != everviz_id], pd.DataFrame([{
        "everviz_id": everviz_id, "title": title, "type": ctype, "chart_id": chart_id, "csv": github_path}])])
    done.to_csv(RESULTS, index=False, encoding="utf-8")

print(f"\n{len(done)} graphs recorded in {RESULTS}")
