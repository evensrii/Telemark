"""
Fylkessammenligning Aggregation
================================
Reads every CSV in data/ (one per FHI indicator, county-level, latest year
only) and computes, for each indicator x breakdown-variant (e.g. per Kjønn,
per education-level category), how Telemark's value compares to the
distribution of the other 14 Norwegian counties: mean/median/min/max,
z-score, and rank.

Writes results/fylkessammenligning_summary.csv, sorted by |z-score|
descending, so the indicators where Telemark deviates most from its peers
surface first.

Usage:
    python aggregate_comparison.py
"""

import json
import re
import statistics
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RESULTS_DIR = BASE_DIR / "results"

GEO_COL = "Fylke"
NON_BREAKDOWN_COLS = {
    GEO_COL, "År", "År (intervall)", "Skoleår", "Skoleår_slutt", "Måltall",
}
SIGNIFICANT_Z_THRESHOLD = 1.5

# Friendlier display label for the "verdi_type" shown in the dashboard, for
# indicators where the generic Andel/Antall column name is misleading. The
# underlying CSV column stays named "Andel" (its value is a real average, not
# a fraction, despite FHI's Måltall text for this table saying "andel") -
# only the human-readable label used in the dashboard is overridden here.
VALUE_TYPE_LABEL_OVERRIDE = {
    "grunnskolepoeng_etter_foreldrenes_utdanningsnivaa": "Gjennomsnittlige grunnskolepoeng",
}

# Collapse gender breakdowns to the combined category everywhere - a
# Menn/Kvinner split multiplies every indicator's variant count 2-3x without
# adding a distinct comparison (the combined figure already reflects both).
COMBINED_KJONN_LABELS = {"Kjønn samlet", "Begge kjønn"}

# Dødsårsaker tiårig only makes sense compared as "Alle aldre" for this
# analysis - its per-age-group breakdown isn't of interest here.
FORCE_ALLE_ALDRE_FOR = {"doedsaarsaker_tiaarig"}

# Diagnosis/ICD-code suffixes like " (S72.0-72.2)" or " (T36-T65)" clutter the
# breakdown label without adding meaning for a lay reader - strip them.
ICD_SUFFIX_RE = re.compile(r"\s*\([^()]*\d[^()]*\)\s*$")


def detect_value_col(df):
    if "Andel" in df.columns:
        return "Andel"
    if "Antall" in df.columns:
        return "Antall"
    return None


def detect_breakdown_cols(df, value_col):
    excluded = NON_BREAKDOWN_COLS | {value_col}
    candidates = [c for c in df.columns if c not in excluded and not c.startswith("Sort")]
    return [c for c in candidates if df[c].nunique(dropna=False) > 1]


def clean_breakdown_value(val):
    return ICD_SUFFIX_RE.sub("", str(val)).strip()


def format_breakdown_label(breakdown_cols, values):
    if not breakdown_cols:
        return "Totalt"
    return "; ".join(f"{col} = {clean_breakdown_value(val)}" for col, val in zip(breakdown_cols, values))


def compute_row(indikator, år, breakdown_label, value_col, fylke_values):
    telemark_value = fylke_values.get("Telemark")
    landet_value = fylke_values.get("Hele landet")
    other_values = {f: v for f, v in fylke_values.items() if f not in ("Telemark", "Hele landet") and pd.notna(v)}

    if telemark_value is None or pd.isna(telemark_value) or len(other_values) < 5:
        return None

    fylke_values_clean = {f: (None if pd.isna(v) else v) for f, v in fylke_values.items()}

    other_vals = list(other_values.values())
    mean_other = statistics.mean(other_vals)
    median_other = statistics.median(other_vals)
    std_other = statistics.stdev(other_vals) if len(other_vals) > 1 else 0.0
    min_other = min(other_vals)
    max_other = max(other_vals)

    z_score = (telemark_value - mean_other) / std_other if std_other > 0 else 0.0

    # Rank 1 = highest value, rank n = lowest value (leaderboard convention -
    # matches how "rank" reads intuitively; whether a high or low value is
    # clinically "good" is conveyed separately via the higher/lower badge).
    # Computed as n minus the count of strictly-lower values (not by sorting
    # and indexing) so ties land on the shared, less-flattering rank number
    # instead of an arbitrary one - e.g. two counties tied for "2nd lowest"
    # of 15 both get rank 14, not one of them getting 13 by chance of order.
    all_vals = other_vals + [telemark_value]
    n = len(all_vals)
    rank = n - sum(1 for v in all_vals if v < telemark_value)

    # "Betydelig forskjell" (not a formal significance test): either the
    # z-score clears the threshold, or Telemark is among the two highest or
    # two lowest of all n counties.
    significant = abs(z_score) >= SIGNIFICANT_Z_THRESHOLD or rank <= 2 or rank >= (n - 1)

    return {
        "indikator": indikator,
        "breakdown": breakdown_label,
        "år": år,
        "verdi_type": value_col,
        "telemark_verdi": telemark_value,
        "landet_verdi": landet_value,
        "andre_fylker_snitt": mean_other,
        "andre_fylker_median": median_other,
        "andre_fylker_min": min_other,
        "andre_fylker_max": max_other,
        "andre_fylker_antall": len(other_vals),
        "z_score": z_score,
        "rangering": rank,
        "rangering_av": n,
        "significant": significant,
        "fylke_verdier": json.dumps(fylke_values_clean, ensure_ascii=False),
    }


def process_file(csv_path):
    df = pd.read_csv(csv_path, encoding="utf-8")
    indikator = csv_path.stem

    if GEO_COL not in df.columns:
        print(f"  ! Skipping {indikator}: no '{GEO_COL}' column")
        return []

    if "Kjønn" in df.columns and df["Kjønn"].isin(COMBINED_KJONN_LABELS).any():
        df = df[df["Kjønn"].isin(COMBINED_KJONN_LABELS)]

    if indikator in FORCE_ALLE_ALDRE_FOR and "Alder" in df.columns and (df["Alder"] == "Alle aldre").any():
        df = df[df["Alder"] == "Alle aldre"]

    value_col = detect_value_col(df)
    if value_col is None:
        print(f"  ! Skipping {indikator}: no Andel/Antall column")
        return []

    år = df["År"].iloc[0] if "År" in df.columns and not df.empty else None
    breakdown_cols = detect_breakdown_cols(df, value_col)
    verdi_type_label = VALUE_TYPE_LABEL_OVERRIDE.get(indikator, value_col)

    rows = []
    if breakdown_cols:
        for values, group in df.groupby(breakdown_cols, dropna=False):
            values = values if isinstance(values, tuple) else (values,)
            breakdown_label = format_breakdown_label(breakdown_cols, values)
            fylke_values = group.groupby(GEO_COL)[value_col].mean().to_dict()
            row = compute_row(indikator, år, breakdown_label, verdi_type_label, fylke_values)
            if row:
                rows.append(row)
    else:
        fylke_values = df.groupby(GEO_COL)[value_col].mean().to_dict()
        row = compute_row(indikator, år, "Totalt", verdi_type_label, fylke_values)
        if row:
            rows.append(row)

    return rows


def main():
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    print(f"Found {len(csv_files)} data files in {DATA_DIR}\n")

    all_rows = []
    for csv_path in csv_files:
        rows = process_file(csv_path)
        all_rows.extend(rows)
        print(f"  {csv_path.stem}: {len(rows)} indicator-variant(s)")

    if not all_rows:
        print("\nNo rows produced - check that fetch scripts have been run (see run_all.py).")
        return

    summary = pd.DataFrame(all_rows)
    summary["abs_z"] = summary["z_score"].abs()
    summary = summary.sort_values("abs_z", ascending=False).drop(columns="abs_z")

    RESULTS_DIR.mkdir(exist_ok=True)
    output_path = RESULTS_DIR / "fylkessammenligning_summary.csv"
    summary.to_csv(output_path, index=False, encoding="utf-8")

    print(f"\n{'=' * 70}")
    print(f"Wrote {len(summary)} rows to {output_path}")
    print(f"{'=' * 70}\n")

    print("Top 15 by |z-score| (Telemark vs. other counties):\n")
    top = summary.head(15)
    for _, r in top.iterrows():
        direction = "høyere" if r["z_score"] > 0 else "lavere"
        print(f"  z={r['z_score']:+.2f}  {r['indikator']} [{r['breakdown']}]: "
              f"Telemark {r['telemark_verdi']:.3f} vs. andre fylker snitt {r['andre_fylker_snitt']:.3f} "
              f"({direction}, rangert {r['rangering']}/{r['rangering_av']})")


if __name__ == "__main__":
    main()
