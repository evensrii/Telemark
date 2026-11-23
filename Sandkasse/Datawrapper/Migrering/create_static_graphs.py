import io
import json
import os
import re
import shutil
import sys
import tempfile

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import datawrapper_generelt as dwg  # noqa: E402
from datawrapper_generelt import API_BASE, get_session  # noqa: E402
from comment_extras import extras_from_comment  # noqa: E402

## Migrering (one-time): create the graphs whose data was typed directly into Everviz (Status "Data lagt inn direkte
## i Everviz (ingen kobling)" in the figure overview), using the create_<type>.py script for each graph type, so the
## formatting rules in Formatering/ apply. Not published.
## - Data: read from the Everviz embed script (https://app.everviz.com/inject/<id>/), saved locally as a comma CSV
##   "statiske_data_<title>.csv" under Data/ (folder from data_folder(); pushed to GitHub by hand), and uploaded
##   directly into the chart (static copy, no live link).
## - Title from "Tittel (forslag)", type from "Type (forslag)", source from "Kilde navn"/"Kilde url",
##   % formatting if "Percentage?" is "Yes" (empty: absolute numbers), extras from "Comment".
## - Folder: the Datawrapper folder "Statiske".
## Created graphs are recorded in statiske_grafer_opprettet.csv. A rerun skips them (UPDATE_EXISTING = False), so
## manual styling in Datawrapper is kept.

OVERVIEW = os.path.join(HERE, "Figuroversikt Kunnskap om Telemark (utkast).xlsx")
RESULTS = os.path.join(HERE, "statiske_grafer_opprettet.csv")
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
STATUS = "Data lagt inn direkte i Everviz (ingen kobling)"
DW_FOLDER_NAME = "Statiske"
FILE_PREFIX = "statiske_data_"
PUBLISH = False
UPDATE_EXISTING = False
ONLY = None  # set to a list of Everviz ids to run only those rows, or None for all

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

## Per-graph changes from the overview, decided after the first run (key: Everviz id):
## "type" replaces "Type (forslag)", "drop" removes data columns, "percent" replaces "Percentage?",
## "description" replaces "Description"
OVERRIDES = {
    "vQCcf_lY2": {"type": "Grouped column chart", "drop": ["2020"]},  # Andel barn i kulturskoler
    "AhGbAbRLP": {"type": "Grouped column chart", "drop": ["2020"]},  # Andel elevplasser i kommunenes kulturskoler
    "fOdJucw3Z": {"percent": False, "description": "Indeks (2012 = 100)"},  # Verdiskaping over tid
    "XJ4iQ0qtl": {"percent": False, "description": "Indeks (2012 = 100)"},  # Sysselsetting over tid
    "M2-AK1ktN": {"percent": False},  # Detaljhandel på kommunenivå (index, base year unknown)
}

## Report pages: fixed Data folders
REPORT_FOLDERS = {
    "rapporter/folkehelseundersokelsen2025/": "Data/Folkehelseundersøkelsen/2025/Bakgrunnsvariabler/2025",
    "rapporter/ungdataundersokelsen/": "Data/Ungdataundersøkelsen/2024/Psykiske plager",
}

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


def data_folder(row):
    """Data folder (relative to the repo) for the CSV, deduced from the page URL and "Side"."""
    url = row["URL"]
    for part, folder in REPORT_FOLDERS.items():
        if part in url:
            return folder
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
    df = df.dropna(how="all").dropna(axis=1, how="all")
    number = r"-?\d+(\.\d+)?" if sep == "," else r"-?\d+(,\d+)?"
    for c in df.columns:
        values = df[c].str.strip()
        # numbers may have "%" or (non-breaking) spaces as thousands separators
        cleaned = values.str.replace("%", "").str.replace(r"(?<=\d)[\s ](?=\d{3})", "", regex=True).str.strip()
        if values.notna().any() and cleaned.dropna().str.fullmatch(number).all():
            df[c] = pd.to_numeric(cleaned.str.replace(",", ".") if sep != "," else cleaned)
        else:
            df[c] = values
    return fix_first_column(df)


YEAR_LIKE = r"\d{4}([-/]\d{2,4})?(-kullet)?"


def first_column_name(values):
    values = values.astype(str).str.strip()
    if values.str.fullmatch(YEAR_LIKE).all():
        return "År"
    if values.str.contains(r"\d+\+?\s*år", regex=True).all():
        return "Alder"
    if values.isin(dwg.COUNTRY_NAMES + dwg.COUNTY_NAMES).any() or values.str.contains("Telemark").any():
        return "Region"
    return "Kategori"


def fix_first_column(df):
    """Data with one row and the categories as columns (e.g. years) is turned so the categories are rows; an empty
    or placeholder first header (Everviz: "", "Unnamed: 0", "null", "Category") is named after its content."""
    first = df.columns[0]
    if len(df) == 1 and len(df.columns) > 2:
        value_name = str(df.iloc[0, 0])
        df = df.set_index(first).T.reset_index()
        df.columns = [first_column_name(df.iloc[:, 0]), value_name]
        print(f"Turned one-row data: categories as rows, values in '{value_name}'")
        return df
    if first == "" or first.startswith("Unnamed:") or first.lower() in ("null", "category"):
        name = first_column_name(df[first])
        df = df.rename(columns={first: name})
        print(f"First column '{first}' named '{name}'")
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
        line = f"{key} = {value!r}"  # a function as replacement, so backslashes in paths are kept as they are
        src, n = re.subn(rf"^{key} = .*$", lambda m: line, src, count=1, flags=re.M)
        if n != 1:
            raise ValueError(f"{script}: setting {key} not found")
    namespace = {"__file__": os.path.join(HERE, script), "__name__": "__main__"}
    exec(compile(src, script, "exec"), namespace)
    return namespace


## Overrides of datawrapper_generelt for these graphs (the type scripts import the functions when they run):
## - data uploaded into the chart instead of linked to an external CSV
## - source from the overview, % from "Percentage?"
_general_metadata = dwg.general_metadata
current_csv_text = ""


def general_metadata_static(csv_url, source_name, source_url):
    metadata = _general_metadata(csv_url, source_name, source_url)
    metadata["data"] = {"upload-method": "copy"}
    return metadata


def apply_and_upload(session, chart_id, title, chart_type, metadata):
    session.patch(f"{API_BASE}/charts/{chart_id}", json={
        "title": title, "type": chart_type, "language": "nb-NO", "theme": "datawrapper", "metadata": metadata,
    }).raise_for_status()
    print("Applied metadata")
    session.put(f"{API_BASE}/charts/{chart_id}/data", data=current_csv_text.encode("utf-8"),
                headers={"Content-Type": "text/csv"}).raise_for_status()
    print("Uploaded data into the chart")
    print(f"Edit: https://app.datawrapper.de/chart/{chart_id}/visualize")


dwg.general_metadata = general_metadata_static
dwg.apply_and_refresh = apply_and_upload


def set_row_rules(row):
    percent = OVERRIDES.get(row["Everviz-id"].strip(), {}).get(
        "percent", row["Percentage?"].strip().lower() in ("yes", "ja"))
    source = (row["Kilde navn"].strip(), row["Kilde url"].strip())
    dwg.is_share = lambda title: percent  # number_formats() and the line chart's y-axis cap use this
    dwg.find_data_source = lambda query_script: source
    return percent


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
    override = OVERRIDES.get(everviz_id, {})
    ctype = override.get("type", row["Type (forslag)"].strip())
    description = override.get("description", row["Description"].strip())
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
        ## 1) Data from Everviz -> local CSV under Data/
        df = everviz_csv(everviz_id).drop(columns=override.get("drop", []))
        print(df.to_string(max_rows=8))
        csv_path = f"{data_folder(row)}/{FILE_PREFIX}{slug(title)}.csv"
        local_path = os.path.join(REPO, *csv_path.split("/"))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        df.to_csv(local_path, index=False, encoding="utf-8")
        current_csv_text = open(local_path, encoding="utf-8").read()
        print(f"CSV: {csv_path}")

        ## 2) Datawrapper chart, data uploaded into it
        percent = set_row_rules(row)
        values = df.iloc[:, 1:].select_dtypes("number").stack()
        if percent and len(values) and values.abs().max() <= 1:
            print("WARNING: 'Percentage?' is Yes, but all values are between 0 and 1 (fractions, not percent?)")
        ns = run_type_script(TYPE_SCRIPTS[ctype], {
            "CSV_URL": local_path, "CHART_TITLE": title, "CHART_ID": chart_id, "QUERY_SCRIPT": None})
        chart_id = ns["chart_id"]
        intro = " ".join(t for t in [description, ns.get("description_note", "")] if t)
        patch = {"metadata": {"describe": {"intro": intro}}}
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
        "everviz_id": everviz_id, "title": title, "type": ctype, "chart_id": chart_id, "csv": csv_path}])])
    done.to_csv(RESULTS, index=False, encoding="utf-8")

print(f"\n{len(done)} graphs recorded in {RESULTS}")
