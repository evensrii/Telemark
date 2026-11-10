import glob
import os
import re
import shutil
import sys
import tempfile
import uuid
from urllib.parse import quote, unquote

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from datawrapper_generelt import API_BASE, get_session  # noqa: E402

## Migrering: create the graphs marked "Ja" in "Klar til produksjon" in the figure overview (Excel), using the
## create_<type>.py script for each graph type, with title, description and source from the overview.
## Comments ask for optional extras from the formatting rules (highlighted period, area fill).
## Created graphs are recorded in grafer_opprettet.csv; a rerun skips them (UPDATE_EXISTING = False), so manual
## styling in Datawrapper is kept. With UPDATE_EXISTING = True they are overwritten instead of created again.

OVERVIEW = os.path.join(HERE, "Figuroversikt Kunnskap om Telemark (utkast).xlsx")
RESULTS = os.path.join(HERE, "grafer_opprettet.csv")
FOLDER_ID = 459833  # "Til kontroll"
PUBLISH = False
UPDATE_EXISTING = False  # graphs in grafer_opprettet.csv are skipped: they may have been styled manually
ONLY = None  # set to a list of Everviz ids to run only those rows

QUERIES = os.path.join(os.environ["PYTHONPATH"], "Queries")
RAW_BASE = "https://raw.githubusercontent.com/evensrii/Telemark/refs/heads/main/"

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
    "Pie chart": "create_donut.py",  # pies are made as donuts
    "Donut chart": "create_donut.py",
}


## Some CSVs store numbers as text with a decimal comma ("57,1"); read those columns as numbers in the scripts
_read_csv = pd.read_csv


def read_csv_decimal_comma(*args, **kwargs):
    df = _read_csv(*args, **kwargs)
    for c in df.columns[1:]:
        if not pd.api.types.is_numeric_dtype(df[c]) and df[c].dropna().astype(str).str.fullmatch(r"-?\d+(,\d+)?").all():
            df[c] = df[c].astype(str).str.replace(",", ".").astype(float)
    return df


pd.read_csv = read_csv_decimal_comma


def csv_url(value):
    value = str(value).strip()
    path = value.split("/main/", 1)[1] if value.startswith("http") else value
    return RAW_BASE + quote(unquote(path))


def find_script(names, csv_path):
    """Full path of the query script; with several candidates, the one that writes this CSV."""
    csv_name = unquote(csv_path.rsplit("/", 1)[-1])
    found = []
    for name in {n.strip() for n in str(names).split(";") if n.strip()}:
        found += [p for p in glob.glob(os.path.join(QUERIES, "**", name), recursive=True)
                  if "Backup" not in p and not os.path.basename(p).startswith("bckp")]
    if len(found) > 1:
        writing = [p for p in found if csv_name in open(p, encoding="utf-8", errors="ignore").read()]
        found = writing or found
    if len(found) > 1:
        master = open(os.path.join(os.environ["PYTHONPATH"], "Automatisering", "Task scheduler", "master_script.py"),
                      encoding="utf-8").read()
        found = [p for p in found if os.path.relpath(p, os.environ["PYTHONPATH"]).replace("\\", "/") in master] or found
    if not found:
        raise ValueError(f"Script not found: {names}")
    return os.path.relpath(found[0], os.environ["PYTHONPATH"]).replace("\\", "/")


def run_type_script(script, settings):
    """Run a create_<type>.py script with its settings replaced; returns its variables (chart_id, metadata ...)."""
    src = open(os.path.join(HERE, script), encoding="utf-8").read()
    for key, value in settings.items():
        src, n = re.subn(rf"^{key} = .*$", f"{key} = {value!r}", src, count=1, flags=re.M)
        if n != 1:
            raise ValueError(f"{script}: setting {key} not found")
    namespace = {"__file__": os.path.join(HERE, script), "__name__": "__main__"}
    exec(compile(src, script, "exec"), namespace)
    return namespace


def extras_from_comment(comment, ns):
    """Optional extras from the line chart rules, asked for in the comment."""
    visualize = {}
    m = re.search(r"[Hh]ighlight between (\d{4}) and (\d{4}), named \"([^\"]+)\"", comment)
    if m:
        start, end, label = m.groups()
        y_top = ns.get("y_max", "")
        visualize["range-annotations"] = [{
            "id": uuid.uuid4().hex[:10], "type": "x", "display": "range", "color": "#888", "opacity": 10,
            "strokeType": "solid", "strokeWidth": 1, "pattern": {"gap": 3, "type": "solid", "width": 3},
            "position": {"x0": f"{start}/01/01 00:00", "x1": f"{end}/01/01 00:00", "y0": "0", "y1": "0", "yAxis": "y"},
            "showMobile": True, "showDesktop": True,
        }]
        visualize["text-annotations"] = [{
            "id": uuid.uuid4().hex[:10], "text": label, "align": "tl", "size": 14, "bold": False, "italic": False,
            "underline": False, "bg": False, "dx": 0, "dy": 0,
            "position": {"x": f"{start}/01/01 14:00", "y": str(y_top)},
            "connectorLine": {"enabled": False}, "showMobile": True, "showDesktop": True, "mobileFallback": True,
        }]
        print(f"Highlight {start}-{end} '{label}' (text at y={y_top})")
    if re.search(r"[Cc]olou?ring below each line", comment):
        df, color_map = ns["df"], dict(ns.get("color_map", {}))
        series = [s for s in df.columns[1:]]
        # Series with only scattered single values (no two values in a row) are shown as dots, not lines
        point_series = [s for s in series
                        if not (df[s].notna() & df[s].shift(1).notna()).any() and df[s].notna().any()]
        line_series = [s for s in series if s not in point_series]
        for p in point_series:  # dots take the color of the line they belong to (a shared word, e.g. "Mdir")
            words = set(re.findall(r"\w+", p))
            owner = next((s for s in line_series if words & set(re.findall(r"\w+", s))), None)
            if owner:
                color_map[p] = color_map[owner]
        visualize["color-category"] = {"map": color_map}
        visualize["custom-area-fills"] = [{
            "id": uuid.uuid4().hex[:10], "from": name, "to": "--zero-baseline--", "color": color_map[name],
            "opacity": 0.3, "colorNegative": "#F76454", "interpolation": "linear", "useMixedColors": False,
        } for name in line_series]
        # Area look: no dots or value labels on the lines, series names in a legend; dot series only as dots
        visualize["lines"] = {
            name: {"symbols": {"enabled": name in point_series, "on": "every", "size": 6},
                   "valueLabels": {"enabled": False},
                   "colorKey": name in line_series,
                   "directLabel": False}
            for name in series
        }
        print(f"Area fill below {line_series}; dots only: {point_series}")
    return visualize


## Read the overview (copied first, as Excel locks the file while it is open)
with tempfile.TemporaryDirectory() as tmp:
    copy = os.path.join(tmp, "overview.xlsx")
    shutil.copy(OVERVIEW, copy)
    overview = pd.read_excel(copy, sheet_name="Figurer", dtype=str).fillna("")
ready = overview[overview["Klar til produksjon"].str.strip().str.lower() == "ja"]
if ONLY:
    ready = ready[ready["Everviz-id"].isin(ONLY)]

done = _read_csv(RESULTS, dtype=str).fillna("") if os.path.exists(RESULTS) else pd.DataFrame(
    columns=["everviz_id", "title", "type", "chart_id", "script"])

session = get_session()
for _, row in ready.iterrows():
    title = " ".join(row["Tittel (forslag)"].split())
    ctype = row["Type (forslag)"].strip()
    print(f"\n=== {title} ({ctype})")
    existing = done.loc[done["everviz_id"] == row["Everviz-id"], "chart_id"]
    chart_id = existing.iloc[0] if len(existing) else None
    if chart_id and not UPDATE_EXISTING:
        print(f"Skipped: already created as {chart_id} (may be styled manually; set UPDATE_EXISTING = True to overwrite)")
        continue
    try:
        query_script = find_script(row["Script"], row["CSV"])
        ns = run_type_script(TYPE_SCRIPTS[ctype], {
            "CSV_URL": csv_url(row["CSV"]), "CHART_TITLE": title, "CHART_ID": chart_id, "QUERY_SCRIPT": query_script})
        chart_id = ns["chart_id"]
        patch = {"metadata": {"describe": {"intro": row["Description"].strip()}}}
        extras = extras_from_comment(row["Comment"], ns)
        if extras:
            patch["metadata"]["visualize"] = extras
        if not len(existing):
            patch["folderId"] = FOLDER_ID  # only when created; later moves in Datawrapper are kept
        session.patch(f"{API_BASE}/charts/{chart_id}", json=patch).raise_for_status()
        if PUBLISH:
            session.post(f"{API_BASE}/charts/{chart_id}/publish").raise_for_status()
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        continue
    done = pd.concat([done[done["everviz_id"] != row["Everviz-id"]], pd.DataFrame([{
        "everviz_id": row["Everviz-id"], "title": title, "type": ctype, "chart_id": chart_id, "script": query_script}])])
    done.to_csv(RESULTS, index=False, encoding="utf-8")

print(f"\n{len(done)} graphs recorded in {RESULTS}")
