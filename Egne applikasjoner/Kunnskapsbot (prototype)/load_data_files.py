"""
Load structured data from this repo's own Data/ CSVs for the topics that
match kunnskap-om-telemark's thematic subpages. Numbers shown on the site
as charts/graphs aren't visible to crawl.py (it only reads rendered page
text), but the same numbers already exist as CSVs in this repo - so we
feed those in directly instead of trying to "read" the charts.

Only Data/ folders with a confirmed, unambiguous match to a real
kunnskap-om-telemark subpage are included below (see TOPIC_MAP). A few
other Data/ folders (Boligbehovsanalyse_2026, Bystrategi_Grenland,
Folkehelseundersøkelsen, Klima_energi_og_gronn_omstilling, Klima_og_energi,
Mobilitet_i_Telemark, Sirkulare_Telemark, 0_Annet) were not mapped because
their corresponding subpage URL wasn't confirmed - extend TOPIC_MAP below
if you want those included too.

Run: python load_data_files.py
"""

import json
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "Data"
GITHUB_BLOB_BASE = "https://github.com/evensrii/Telemark/blob/main/Data"

OUTPUT_FILE = "data_pages.json"
MAX_FULL_ROWS = 60  # embed the whole table if it has at most this many rows
SAMPLE_ROWS = 30  # otherwise, take this many of the most recent rows

TOPIC_MAP = {
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/befolkning/": "01_Befolkning",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/opplaring-og-kompetanse/": "02_Opplæring og kompetanse",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/arbeid-og-naeringsliv/": "03_Arbeid og næringsliv",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/klima-og-energi/": "04_Klima og ressursforvaltning",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/mobilitet-og-infrastruktur/": "05_Mobilitet og infrastruktur",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/kultur-og-kulturarv/": "06_Kultur og kulturarv",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/idrett-og-friluftsliv/": "07_Idrett_friluftsliv_og_frivillighet",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/folkehelse-og-levekar/": "08_Folkehelse og levekår",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/innvandrere-og-integrering/": "09_Innvandrere og inkludering",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/tema/areal-og-stedsutvikling/": "10_Areal- og stedsutvikling",
    "https://www.telemarkfylke.no/no/kunnskap-om-telemark/rapporter/arealregnskap-for-telemark/": "Arealregnskap",
}


def csv_to_text(csv_path):
    df = None
    last_error = None
    for encoding in ("utf-8-sig", "cp1252", "ISO-8859-1"):
        try:
            df = pd.read_csv(csv_path, sep=None, engine="python", encoding=encoding)
            break
        except (UnicodeDecodeError, UnicodeError) as e:
            last_error = e
        except Exception as e:
            print(f"  skipping {csv_path.name}: {e}")
            return None

    if df is None:
        print(f"  skipping {csv_path.name}: {last_error}")
        return None

    if len(df) > MAX_FULL_ROWS:
        preview = df.tail(SAMPLE_ROWS)
        note = f"(viser de {SAMPLE_ROWS} nyeste av {len(df)} rader)"
    else:
        preview = df
        note = f"({len(df)} rader)"

    return f"Kolonner: {', '.join(df.columns)}\n{note}\n\n{preview.to_string(index=False)}"


def load_data_files():
    pages = []
    for url, folder_name in TOPIC_MAP.items():
        folder = DATA_ROOT / folder_name
        if not folder.exists():
            print(f"Missing Data folder, skipping: {folder}")
            continue

        for csv_path in folder.rglob("*.csv"):
            text = csv_to_text(csv_path)
            if not text:
                continue
            relative = csv_path.relative_to(DATA_ROOT).as_posix()
            pages.append(
                {
                    "url": f"{GITHUB_BLOB_BASE}/{relative}",
                    "title": f"Datasett: {csv_path.stem} ({folder_name})",
                    "text": text,
                }
            )
            print(f"Loaded {relative} ({len(text)} chars)")

    return pages


if __name__ == "__main__":
    pages = load_data_files()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(pages, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(pages)} datasets to {OUTPUT_FILE}")
