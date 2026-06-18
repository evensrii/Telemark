import os
import re
import requests
import pandas as pd
import matplotlib.pyplot as plt

from Helper_scripts.github_functions import download_github_file, handle_output_data, GITHUB_TOKEN

script_name = os.path.basename(__file__)
error_messages = []

# ============================================================
# Configuration
# ============================================================

GITHUB_FOLDER = "Data/01_Befolkning/Befolkningsframskrivinger"

# Dynamically find all prediction files from 2016 onward on GitHub
def get_prediction_files_from_github(github_folder, min_year=2016):
    """List all Framskrevet_folkemengde_*.csv files on GitHub from min_year onward."""
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{github_folder}?ref=main"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    files = []
    for item in response.json():
        name = item["name"]
        match = re.match(r"Framskrevet_folkemengde_(\d{4})-(\d{4})\.csv", name)
        if match:
            start_year = int(match.group(1))
            if start_year >= min_year:
                files.append(name)
    
    return sorted(files)


PREDICTION_FILES = get_prediction_files_from_github(GITHUB_FOLDER, min_year=2016)
print(f"Found {len(PREDICTION_FILES)} prediction files (2016+): {PREDICTION_FILES}")

HISTORICAL_FILE = "befolkning_historisk_og_framskrevet.csv"

# Regions that represent the Telemark fylke total (not individual kommuner)
FYLKE_NAMES = ["Telemark", "Telemark (-2019)"]

# All Telemark kommuner (current + historical names, after cleaning)
# Used to filter prediction files which contain ALL Norwegian kommuner
TELEMARK_KOMMUNER = [
    "Porsgrunn", "Skien", "Notodden", "Siljan", "Bamble", "Kragerø",
    "Drangedal", "Nome", "Midt-Telemark", "Seljord", "Hjartdal", "Tinn",
    "Kviteseid", "Nissedal", "Fyresdal", "Tokke", "Vinje",
    # Pre-2020 names (before mergers) — after clean_region_name()
    "Bø (Telemark)", "Sauherad",
]

# ============================================================
# Step 1: Read historical data from GitHub
# ============================================================

print("=" * 60)
print("Step 1: Reading historical data")
print("=" * 60)

df_hist = download_github_file(f"{GITHUB_FOLDER}/{HISTORICAL_FILE}")

if df_hist is None:
    raise RuntimeError(f"Could not download {HISTORICAL_FILE} from GitHub")

# Convert numeric columns
df_hist["Personer"] = pd.to_numeric(df_hist["Personer"], errors="coerce")

# Filter to historical data only, exclude "Hele landet" and fylke-level "Telemark" row
df_hist = df_hist[df_hist["Type"] == "Historisk"].copy()
df_hist = df_hist[~df_hist["Kommune"].isin(["Hele landet", "Telemark"])].copy()

# Extract year from "YYYY-01-01" format
df_hist["År"] = df_hist["År"].str[:4].astype(int)

# Filter from 2000 onward (all available historical data)
df_hist = df_hist[df_hist["År"] >= 2000].copy()

# Clean kommune names (strip any suffixes for consistency)
df_hist["Kommune_clean"] = df_hist["Kommune"]

print(f"Historical data: {len(df_hist)} rows, years {df_hist['År'].min()}-{df_hist['År'].max()}")
print(f"Kommuner: {sorted(df_hist['Kommune_clean'].unique())}")


# ============================================================
# Step 2: Read and process prediction files from GitHub
# ============================================================

print("\n" + "=" * 60)
print("Step 2: Reading prediction files")
print("=" * 60)


def clean_region_name(name):
    """Remove year suffixes like (-2019), (2020-2023), (1964-2019), (1913-2019) from region names."""
    # Remove patterns like "(-2019)", "(2020-2023)", "(1964-2019)"
    cleaned = re.sub(r"\s*\(-?\d{4}(?:-\d{4})?\)\s*$", "", name).strip()
    # Also handle "Bø (Telemark) (-2019)" -> first remove the year suffix, then handle "(Telemark)"
    # After first regex: "Bø (Telemark)" remains
    # We keep "(Telemark)" as part of the name to distinguish from Bø in Nordland
    return cleaned


def get_telemark_kommuner_from_prediction(df, framskriving_year):
    """
    Filter prediction dataframe to Telemark kommuner only (MMMM alternative).
    Returns a dataframe with cleaned kommune names and aggregated Bø+Sauherad -> Midt-Telemark.
    Also returns the fylke-row total if available.
    """
    # Filter to MMMM only
    df_mmmm = df[df["statistikkvariabel"].str.contains("MMMM", na=False)].copy()

    # Convert value to numeric
    df_mmmm["value"] = pd.to_numeric(df_mmmm["value"], errors="coerce")
    df_mmmm["år"] = pd.to_numeric(df_mmmm["år"], errors="coerce")

    # Clean region names
    df_mmmm["region_clean"] = df_mmmm["region"].apply(clean_region_name)

    # Separate fylke row from kommune rows, and filter to Telemark only
    fylke_mask = df_mmmm["region_clean"].isin(FYLKE_NAMES)
    df_fylke = df_mmmm[fylke_mask].copy()
    telemark_mask = df_mmmm["region_clean"].isin(TELEMARK_KOMMUNER)
    df_kommuner = df_mmmm[telemark_mask].copy()

    # Handle Bø (Telemark) + Sauherad -> Midt-Telemark aggregation
    # In 2016/2018 files: "Bø (Telemark)" and "Sauherad" are separate
    # In 2020+ files: "Midt-Telemark" already exists
    bo_mask = df_kommuner["region_clean"] == "Bø (Telemark)"
    sauherad_mask = df_kommuner["region_clean"] == "Sauherad"

    if bo_mask.any() and sauherad_mask.any():
        # Aggregate Bø + Sauherad into Midt-Telemark
        df_bo_sauherad = df_kommuner[bo_mask | sauherad_mask].copy()
        df_midt_telemark = (
            df_bo_sauherad.groupby(["år", "alder"], as_index=False)["value"]
            .sum()
        )
        df_midt_telemark["region_clean"] = "Midt-Telemark"

        # Remove Bø and Sauherad from kommuner, add Midt-Telemark
        df_kommuner = df_kommuner[~(bo_mask | sauherad_mask)].copy()
        df_kommuner = pd.concat(
            [df_kommuner[["region_clean", "år", "alder", "value"]], df_midt_telemark],
            ignore_index=True,
        )
    else:
        df_kommuner = df_kommuner[["region_clean", "år", "alder", "value"]].copy()

    # Add framskriving identifier
    df_kommuner["Framskriving"] = framskriving_year
    df_fylke_out = None
    if not df_fylke.empty:
        df_fylke_out = df_fylke[["region_clean", "år", "alder", "value"]].copy()
        df_fylke_out["Framskriving"] = framskriving_year

    return df_kommuner, df_fylke_out


# Process all prediction files
all_prediction_kommuner = []
all_prediction_fylke = []

for file_name in PREDICTION_FILES:
    print(f"\nProcessing: {file_name}")
    df_pred = download_github_file(f"{GITHUB_FOLDER}/{file_name}")

    if df_pred is None:
        print(f"  WARNING: Could not download {file_name}, skipping.")
        continue

    # Extract framskriving year from filename
    match = re.search(r"(\d{4})-\d{4}", file_name)
    framskriving_year = int(match.group(1))

    df_kommuner, df_fylke = get_telemark_kommuner_from_prediction(df_pred, framskriving_year)

    print(f"  Kommuner rows: {len(df_kommuner)}, unique kommuner: {sorted(df_kommuner['region_clean'].unique())}")
    if df_fylke is not None:
        print(f"  Fylke row available: YES")
    else:
        print(f"  Fylke row available: NO")

    all_prediction_kommuner.append(df_kommuner)
    if df_fylke is not None:
        all_prediction_fylke.append(df_fylke)

# Combine all predictions
df_all_pred_kommuner = pd.concat(all_prediction_kommuner, ignore_index=True)
df_all_pred_fylke = pd.concat(all_prediction_fylke, ignore_index=True) if all_prediction_fylke else pd.DataFrame()

print(f"\nTotal prediction kommune rows: {len(df_all_pred_kommuner)}")
print(f"Framskrivinger: {sorted(df_all_pred_kommuner['Framskriving'].unique())}")


# ============================================================
# Step 3: Build the output CSV (per-kommune, per-age detail)
# ============================================================

print("\n" + "=" * 60)
print("Step 3: Building output CSV")
print("=" * 60)

# Historical data: reshape to match prediction format
df_hist_output = df_hist[["Kommune_clean", "Alder", "År", "Personer"]].copy()
df_hist_output.columns = ["Kommune", "Alder", "År", "Personer"]
df_hist_output["Kilde"] = "Historisk"

# Prediction data: reshape
df_pred_output = df_all_pred_kommuner.rename(columns={
    "region_clean": "Kommune",
    "alder": "Alder",
    "år": "År",
    "value": "Personer",
}).copy()
df_pred_output["Kilde"] = "Framskriving " + df_pred_output["Framskriving"].astype(str)
df_pred_output = df_pred_output[["Kommune", "Alder", "År", "Personer", "Kilde"]]

# Combine
df_output = pd.concat([df_hist_output, df_pred_output], ignore_index=True)
df_output["År"] = df_output["År"].astype(int)
df_output["Personer"] = pd.to_numeric(df_output["Personer"], errors="coerce")

print(f"Output dataset: {len(df_output)} rows")
print(f"Kilder: {sorted(df_output['Kilde'].unique())}")
print(f"Years: {df_output['År'].min()} - {df_output['År'].max()}")

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "befolkning_sammenlikning_historisk_og_framskrivinger.csv"
task_name = "Befolkning - Sammenlikning historisk og framskrivinger"
github_folder = GITHUB_FOLDER
temp_folder = os.environ.get("TEMP_FOLDER")

is_new_data = handle_output_data(
    df_output,
    file_name,
    github_folder,
    temp_folder,
    keepcsv=True,
)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
task_name_safe = task_name.replace(".", "_").replace(" ", "_")
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")


# ============================================================
# Step 4: Debug table — Total Telemark population per year
# ============================================================

print("\n" + "=" * 60)
print("Step 4: Debug table — Total Telemark per year per source")
print("=" * 60)

# Sum all kommuner per year per kilde (= total Telemark from kommune aggregation)
df_total_kommuner = (
    df_output.groupby(["År", "Kilde"], as_index=False)["Personer"]
    .sum()
    .rename(columns={"Personer": "Sum_kommuner"})
)

# Get fylke-row totals (where available)
if not df_all_pred_fylke.empty:
    df_fylke_total = (
        df_all_pred_fylke.groupby(["år", "Framskriving"], as_index=False)["value"]
        .sum()
    )
    df_fylke_total["Kilde"] = "Framskriving " + df_fylke_total["Framskriving"].astype(str)
    df_fylke_total = df_fylke_total.rename(columns={"år": "År", "value": "Fylke_rad"})
    df_fylke_total["År"] = df_fylke_total["År"].astype(int)
    df_fylke_total = df_fylke_total[["År", "Kilde", "Fylke_rad"]]
else:
    df_fylke_total = pd.DataFrame(columns=["År", "Kilde", "Fylke_rad"])

# Merge kommune sum with fylke row
df_debug = df_total_kommuner.merge(df_fylke_total, on=["År", "Kilde"], how="left")

# Get the start year of each framskriving (dynamically)
df_pred_debug = df_debug[df_debug["Kilde"] != "Historisk"]
framskriving_start_years = sorted(
    df_pred_debug.groupby("Kilde")["År"].min().unique()
)

# Show historical population for each framskriving start year
df_hist_debug = df_debug[df_debug["Kilde"] == "Historisk"].sort_values("År")

print("\nHistorical Telemark population at framskriving start years:")
print("-" * 40)
print(f"{'År':<6} {'Historisk':>14}")
print("-" * 40)
for year in framskriving_start_years:
    row = df_hist_debug[df_hist_debug["År"] == year]
    if not row.empty:
        print(f"{year:<6} {int(row.iloc[0]['Sum_kommuner']):>14,}")
    else:
        print(f"{year:<6} {'N/A':>14}")
print()

# Show each framskriving with first, middle, last year
print("\nTotal Telemark population per framskriving (selected years):")
print("-" * 70)
print(f"{'Kilde':<20} {'År':<6} {'Sum kommuner':>14} {'Fylke-rad':>12} {'Diff':>8}")
print("-" * 70)

for kilde in sorted(df_debug["Kilde"].unique()):
    if kilde == "Historisk":
        continue
    df_k = df_debug[df_debug["Kilde"] == kilde].sort_values("År")
    # Show first, middle, last year
    years_to_show = []
    if len(df_k) > 0:
        years_to_show.append(df_k["År"].min())
    if len(df_k) > 2:
        mid_year = df_k["År"].iloc[len(df_k) // 2]
        years_to_show.append(mid_year)
    if len(df_k) > 1:
        years_to_show.append(df_k["År"].max())

    for year in years_to_show:
        row = df_k[df_k["År"] == year].iloc[0]
        sum_k = int(row["Sum_kommuner"])
        fylke = int(row["Fylke_rad"]) if pd.notna(row["Fylke_rad"]) else None
        diff = sum_k - fylke if fylke is not None else None
        fylke_str = f"{fylke:>12,}" if fylke else f"{'N/A':>12}"
        diff_str = f"{diff:>8}" if diff is not None else f"{'':>8}"
        print(f"{kilde:<20} {year:<6} {sum_k:>14,} {fylke_str} {diff_str}")
    print()


# ============================================================
# Step 5: Plot — Total Telemark population over time
# (only when running interactively, skipped in master_script)
# ============================================================

def is_interactive():
    """Check if running in an interactive environment (Jupyter/IPython)."""
    try:
        from IPython import get_ipython
        return get_ipython() is not None
    except ImportError:
        return False

if is_interactive():
    print("\n" + "=" * 60)
    print("Step 5: Plotting total Telemark population")
    print("=" * 60)

    # Pivot for plotting: sum all ages per year per kilde
    df_plot = (
        df_output.groupby(["År", "Kilde"], as_index=False)["Personer"]
        .sum()
    )

    # Separate historical from predictions
    df_plot_hist = df_plot[df_plot["Kilde"] == "Historisk"].sort_values("År")
    df_plot_preds = df_plot[df_plot["Kilde"] != "Historisk"]

    # Set up the plot
    fig, ax = plt.subplots(figsize=(12, 7))

    # Plot historical line (solid, thick, red)
    ax.plot(
        df_plot_hist["År"],
        df_plot_hist["Personer"],
        color="red",
        linewidth=2.5,
        label="Historisk",
        zorder=5,
    )

    # Plot each prediction as a dashed line: light grey (oldest) to dark (newest)
    sorted_preds = sorted(df_plot_preds["Kilde"].unique())
    n_preds = len(sorted_preds)
    for i, framskriving in enumerate(sorted_preds):
        df_f = df_plot_preds[df_plot_preds["Kilde"] == framskriving].sort_values("År")
        # Gradient from light grey (0.80) to near-black (0.10)
        grey_value = 0.80 - (0.70 * i / max(n_preds - 1, 1))
        color = (grey_value, grey_value, grey_value)
        linewidth = 1.0 + (1.5 * i / max(n_preds - 1, 1))
        ax.plot(
            df_f["År"],
            df_f["Personer"],
            color=color,
            linewidth=linewidth,
            linestyle="--",
            label=framskriving,
        )
        # Add label at the end of the line with the framskriving start year
        start_year = framskriving.replace("Framskriving ", "")
        last_row = df_f.iloc[-1]
        ax.annotate(
            start_year,
            xy=(last_row["År"], last_row["Personer"]),
            xytext=(5, 0),
            textcoords="offset points",
            fontsize=9,
            color=color,
            va="center",
        )

    ax.set_xlabel("År", fontsize=12)
    ax.set_ylabel("Befolkning", fontsize=12)
    ax.set_title("Befolkningsutvikling i Telemark — Historisk og framskrevet", fontsize=14)
    ax.legend(loc="upper left", fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(2000, 2050)

    # Format y-axis with thousands separator
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: f"{int(x):,}".replace(",", " ")))

    plt.tight_layout()
    plt.show()

    # Output table with line styling for recreation in other tools
    print("\n" + "=" * 70)
    print("Line styling reference (for recreation in other tools)")
    print("=" * 70)
    print(f"{'Linje':<25} {'Farge (hex)':<14} {'Bredde':<10} {'Stil':<10}")
    print("-" * 70)
    print(f"{'Historisk':<25} {'#FF0000':<14} {'2.5':<10} {'Solid':<10}")
    for i, framskriving in enumerate(sorted_preds):
        grey_value = 0.80 - (0.70 * i / max(n_preds - 1, 1))
        lw = 1.0 + (1.5 * i / max(n_preds - 1, 1))
        grey_int = int(grey_value * 255)
        hex_color = f"#{grey_int:02X}{grey_int:02X}{grey_int:02X}"
        print(f"{framskriving:<25} {hex_color:<14} {lw:<10.2f} {'Stiplet':<10}")
    print("-" * 70)

print("\nDone!")
