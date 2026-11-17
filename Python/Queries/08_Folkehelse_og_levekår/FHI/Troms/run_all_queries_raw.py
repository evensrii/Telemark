"""
FHI-spørringer for Troms - råuttrekk
====================================

Kjører alle spørringene (*.txt) i Troms/queries og undermapper mot FHIs API, og lagrer
svaret uten transformasjoner som CSV i Troms/resultater_raw.

Nye spørringer plukkes opp automatisk: legg en .txt-fil (første linje URL, resten JSON-spørring)
hvor som helst under queries/. Filnavnet lages fra spørringens filnavn, f.eks.
"Kreft, nye tilfeller, tiårige tall.txt" -> "kreft_nye_tilfeller_tiaarige_tall.csv".

Ingenting lastes opp til GitHub.
"""

import json
import os
import re
import sys
import traceback
from pathlib import Path

pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    current = Path(__file__).resolve()
    while current.name != "Python" and current != current.parent:
        current = current.parent
    pythonpath = str(current)
    os.environ["PYTHONPATH"] = pythonpath
sys.path.append(pythonpath)

from Helper_scripts.utility_functions import fetch_data

BASE_DIR = Path(pythonpath) / "Queries" / "08_Folkehelse_og_levekår" / "FHI" / "Troms"
QUERIES_DIR = BASE_DIR / "queries"
RESULTS_DIR = BASE_DIR / "resultater_raw"


def load_query_file(file_path):
    """Første linje er URL, resten er JSON-spørringen."""
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    url = lines[0].strip()
    query = json.loads("".join(line.strip() for line in lines[1:] if line.strip()))
    return url, query


def slugify(name):
    """'Hjerte- og karsykdom - primærhelsetjenesten, treårige tall' -> 'hjerte_og_karsykdom_primaerhelsetjenesten_treaarige_tall'"""
    name = name.lower().replace("æ", "ae").replace("ø", "oe").replace("å", "aa")
    return re.sub(r"[^a-z0-9]+", "_", name).strip("_")


def output_names(query_files):
    """Filnavn per spørring. Hvis to spørringer i ulike mapper har samme navn,
    får de mappestien som prefiks for å unngå at de overskriver hverandre."""
    names = {p: slugify(p.stem) for p in query_files}
    counts = {}
    for n in names.values():
        counts[n] = counts.get(n, 0) + 1
    for p, n in names.items():
        if counts[n] > 1:
            names[p] = slugify(p.relative_to(QUERIES_DIR).with_suffix("").as_posix())
    return {p: f"{n}.csv" for p, n in names.items()}


# %%
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

query_files = sorted(QUERIES_DIR.rglob("*.txt"))
names = output_names(query_files)
print(f"Fant {len(query_files)} spørringer i {QUERIES_DIR}\n")

failures = []
for i, query_file in enumerate(query_files, start=1):
    rel_path = query_file.relative_to(QUERIES_DIR).as_posix()
    print(f"[{i}/{len(query_files)}] {rel_path}")
    try:
        url, query = load_query_file(query_file)
        error_messages = []
        df = fetch_data(url=url, payload=query, error_messages=error_messages,
                        query_name=rel_path, response_type="json")
        if df is None or df.empty:
            raise RuntimeError("Ingen data fra API-et. " + " | ".join(error_messages))
        df.to_csv(RESULTS_DIR / names[query_file], index=False, encoding="utf-8")
        print(f"    ✓ {names[query_file]} ({len(df)} rader)")
    except Exception as e:
        print(f"    ✗ FEILET: {e}")
        traceback.print_exc()
        failures.append((rel_path, str(e)))

print(f"\n{len(query_files) - len(failures)}/{len(query_files)} spørringer OK. Resultater i: {RESULTS_DIR}")
if failures:
    print("\nFeilet:")
    for rel_path, err in failures:
        print(f"  - {rel_path}: {err}")
    sys.exit(1)
