import os
import re
import shutil
import sys
import tempfile
from urllib.parse import quote, unquote

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import datawrapper_generelt as dwg  # noqa: E402
from datawrapper_generelt import API_BASE, get_session  # noqa: E402

## Migrering: create the Folkehelseundersøkelsen 2025 graphs (rows marked "FHUS-grafer - Ja" in "Klar til produksjon"
## in the figure overview), using the create_<type>.py script for each graph type, and publish them.
## - Data: static CSVs on GitHub (no query script).
## - All values are shares (whole percentages, e.g. 27 = 27 %), even though the titles don't contain "Andel".
## - Source: "Folkehelseundersøkelsen 2025" with the link in sources.csv.
## - Folder: the subfolder of 00_Rapporter og prosjekter/Folkehelseundersøkelsen/2025 named as the "Side" column.
## Created graphs are recorded in fhus_grafer_opprettet.csv. A rerun skips them (UPDATE_EXISTING = False), so manual
## styling in Datawrapper is kept.

OVERVIEW = os.path.join(HERE, "Figuroversikt Kunnskap om Telemark (utkast).xlsx")
RESULTS = os.path.join(HERE, "fhus_grafer_opprettet.csv")
READY_VALUE = "fhus-grafer - ja"
PARENT_FOLDER_ID = 459896  # 00_Rapporter og prosjekter / Folkehelseundersøkelsen / 2025
SOURCE_NAME = "Folkehelseundersøkelsen 2025"
PUBLISH = True
UPDATE_EXISTING = False
ONLY = None  # set to a list of Everviz ids to run only those rows

RAW_BASE = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/"

TYPE_SCRIPTS = {
    "Column chart": "create_column.py",
    "Grouped column chart": "create_column_grouped.py",
    "Bar chart": "create_bar.py",
    "Grouped bar chart": "create_bar_grouped.py",
    "Stacked column chart": "create_column_stacked.py",
    "Stacked bar chart": "create_bar_stacked.py",
    "Pie chart": "create_donut.py",
    "Donut chart": "create_donut.py",
}


## Overrides of the general rules for these graphs: always shares, fixed source
_number_formats = dwg.number_formats
dwg.number_formats = lambda values, title: _number_formats(values, "Andel " + title)
dwg.find_data_source = lambda query_script: (SOURCE_NAME, dwg.SOURCES_CSV_LINKS()[SOURCE_NAME])


## The CSVs have some quirks: empty headers (pandas would call them "Unnamed: 0"), and stray spaces in headers and
## values (" 45-59 år", "31 "). Empty headers are named after their position (X.1, X.2 ...), spaces are ignored.
_read_csv = pd.read_csv


def read_csv_clean(*args, **kwargs):
    df = _read_csv(*args, **kwargs)
    df.columns = [f"X.{i + 1}" if str(c).startswith("Unnamed:") else str(c).strip() for i, c in enumerate(df.columns)]
    for i, c in enumerate(df.columns):
        if i == 0 or pd.api.types.is_numeric_dtype(df.iloc[:, i]):
            continue
        text = df.iloc[:, i].astype(str).str.strip()
        if text[df.iloc[:, i].notna()].str.fullmatch(r"-?\d+([.,]\d+)?").all():
            df[c] = pd.to_numeric(text.str.replace(",", "."), errors="coerce")
    return df


pd.read_csv = read_csv_clean


def csv_url(value):
    value = str(value).strip()
    path = value.split("/main/", 1)[1] if value.startswith("http") else value
    return RAW_BASE + quote(unquote(path))


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


## Read the overview (copied first, as Excel locks the file while it is open)
with tempfile.TemporaryDirectory() as tmp:
    copy = os.path.join(tmp, "overview.xlsx")
    shutil.copy(OVERVIEW, copy)
    overview = pd.read_excel(copy, sheet_name="Figurer", dtype=str).fillna("")
ready = overview[overview["Klar til produksjon"].str.strip().str.lower() == READY_VALUE]
if ONLY:
    ready = ready[ready["Everviz-id"].isin(ONLY)]
print(f"{len(ready)} graphs in the overview")

done = _read_csv(RESULTS, dtype=str).fillna("") if os.path.exists(RESULTS) else pd.DataFrame(
    columns=["everviz_id", "title", "type", "chart_id", "folder", "csv"])

session = get_session()
parent = session.get(f"{API_BASE}/folders/{PARENT_FOLDER_ID}").json()
children = [c["id"] if isinstance(c, dict) else c for c in parent.get("children", [])]
folders = {f["name"]: f["id"] for f in (session.get(f"{API_BASE}/folders/{i}").json() for i in children)}

for _, row in ready.iterrows():
    title = " ".join(row["Tittel (forslag)"].split())
    ctype = row["Type (forslag)"].strip()
    folder = row["Side"].strip()
    print(f"\n=== {title} ({ctype}) -> {folder}")
    existing = done.loc[done["everviz_id"] == row["Everviz-id"], "chart_id"]
    chart_id = existing.iloc[0] if len(existing) else None
    if chart_id and not UPDATE_EXISTING:
        print(f"Skipped: already created as {chart_id}")
        continue
    try:
        if folder not in folders:
            raise ValueError(f"Folder '{folder}' not found under {PARENT_FOLDER_ID}")
        url = csv_url(row["CSV"])
        ns = run_type_script(TYPE_SCRIPTS[ctype], {
            "CSV_URL": url, "CHART_TITLE": title, "CHART_ID": chart_id, "QUERY_SCRIPT": None})
        chart_id = ns["chart_id"]
        patch = {"metadata": {"describe": {"intro": row["Description"].strip()}}}
        if not len(existing):
            patch["folderId"] = folders[folder]  # only when created; later moves in Datawrapper are kept
        session.patch(f"{API_BASE}/charts/{chart_id}", json=patch).raise_for_status()
        if PUBLISH:
            session.post(f"{API_BASE}/charts/{chart_id}/publish").raise_for_status()
            print(f"Published: https://datawrapper.dwcdn.net/{chart_id}/")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        continue
    done = pd.concat([done[done["everviz_id"] != row["Everviz-id"]], pd.DataFrame([{
        "everviz_id": row["Everviz-id"], "title": title, "type": ctype, "chart_id": chart_id, "folder": folder,
        "csv": row["CSV"]}])])
    done.to_csv(RESULTS, index=False, encoding="utf-8")

print(f"\n{len(done)} graphs recorded in {RESULTS}")
