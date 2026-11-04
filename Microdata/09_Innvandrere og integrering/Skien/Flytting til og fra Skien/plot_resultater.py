"""Lager søylediagrammer (png) fra tabellene i resultatfilene fra microdata.no.

Hver resultatfil (.csv) består av blokker med en tittel etterfulgt av en tabell.
Kjør scriptet uten argumenter for å lage alle grafene i TITLER, eller med én eller
flere titler som argumenter for å lage bare de.
"""

import math
import re
import sys
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

MAPPE = Path(__file__).resolve().parent
UT_MAPPE = MAPPE / "Graphs"

TITLER = [
    "Innflyttere til Skien etter kategori",
    "Innflytting til Skien",
    "Alder ved innflytting (antall og prosent)",
    "Fødeland innflyttere",
    "Norskfødte og innvandrere blant innflyttere",
    "Utflytting fra Skien",
    "Alder ved utflytting (antall og prosent)",
    "Fødeland utflyttere",
    "Innvandringsgrunn innflyttere (antall og prosent)",
    "Sekundærbosatt botid",
    "Sekundærbosatt alder (antall og prosent)",
    "Sekundærbosatt landbakgrunn",
    "Sekundærbosatt fra fylke",
    "Sekundærbosatt utdanningsnivå 16 år og eldre (antall og prosent)",
    "Sekundærbosatt husholdningstype",
    "Familieinnvandret per år",
    "Familieinnvandrede etter alder",
]

TOPP_N_FODELAND = 15

# Rader som ikke skal plottes
UTELATTE_KATEGORIER = {"Total", "SYSMISS"}

# Husholdningstype mangler etiketter i eldre kjøringer (bare "1.1", "1.2" osv.)
HUSHTYPE = {
    "1.1": "Aleneboende",
    "1.2": "Par uten hjemmeboende barn",
    "1.3": "Par med små barn (yngste 0-5 år)",
    "1.4": "Par med store barn (yngste 6-17 år)",
    "1.5": "Mor/far med små barn (yngste 0-5 år)",
    "1.6": "Mor/far med store barn (yngste 6-17 år)",
    "1.7": "Enfamiliehusholdning med voksne barn",
    "2.1": "Flerfamiliehusholdning uten barn 0-17 år",
    "2.2": "Flerfamiliehusholdning med små barn",
    "2.3": "Flerfamiliehusholdning med store barn",
}

FARGE = "#1C6C6C"


def les_tabeller():
    """Returnerer {tittel: [(kategori, antall, prosent eller None), ...]} for alle csv-filer."""
    tabeller = {}
    for fil in sorted(MAPPE.glob("*.csv")):
        tittel = None
        for linje in fil.read_text(encoding="utf-8").splitlines():
            linje = linje.strip()
            if not linje:
                continue
            if not linje.startswith("|"):
                tittel = linje.rstrip(":").strip()
                tabeller[tittel] = []
                continue
            celler = [c.strip() for c in linje.strip("|").split("|")]
            # Overskrift- og skillelinjer har ikke et tall i andre kolonne
            if len(celler) != 2 or not re.match(r"^-?\d", celler[1]):
                continue
            verdier = celler[1].split()
            antall = float(verdier[0])
            prosent = float(verdier[1].replace(",", ".")) if len(verdier) > 1 else None
            tabeller[tittel].append((celler[0], antall, prosent))
    return tabeller


def rens_kategori(kategori):
    """Fjerner kode i starten av kategorinavnet, f.eks. '534 - Pakistan' -> 'Pakistan'."""
    if kategori in HUSHTYPE:
        return HUSHTYPE[kategori]
    return re.sub(r"^[0-9A-ZÆØÅ.]+ - ", "", kategori)


def avrund(x):
    return int(math.floor(x + 0.5))


def lag_graf(tittel, rader):
    rader = [r for r in rader if r[0] not in UTELATTE_KATEGORIER]
    if tittel.startswith("Fødeland"):
        # Norge holdes utenfor, ellers forsvinner de andre landene i grafen
        rader = [r for r in rader if not r[0].startswith("000 ")]
        rader = sorted(rader, key=lambda r: r[1], reverse=True)[:TOPP_N_FODELAND]
    elif "fylke" in tittel:
        rader = sorted(rader, key=lambda r: r[1], reverse=True)

    kategorier = [rens_kategori(r[0]) for r in rader]
    antall = [r[1] for r in rader]
    har_prosent = all(r[2] is not None for r in rader)
    etiketter = [f"{avrund(r[2])} %" if har_prosent else f"{int(r[1])}" for r in rader]

    # År og aldersgrupper gir stående søyler, alt annet liggende
    liggende = not all(re.match(r"^\d{4}$|^\d+(-\d+)? år|^Under \d+ år|^Over \d+ år", k) for k in kategorier)
    if liggende:
        fig, ax = plt.subplots(figsize=(9, 0.45 * len(rader) + 1.5))
        # Første rad øverst
        kategorier, antall, etiketter = kategorier[::-1], antall[::-1], etiketter[::-1]
        soyler = ax.barh(kategorier, antall, color=FARGE)
        ax.set_xlabel("Antall personer")
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_xlim(0, max(antall) * 1.12)
    else:
        fig, ax = plt.subplots(figsize=(9, 5))
        soyler = ax.bar([textwrap.fill(k, 10) for k in kategorier], antall, color=FARGE)
        ax.set_ylabel("Antall personer")
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_ylim(0, max(antall) * 1.12)

    ax.bar_label(soyler, labels=etiketter, padding=3, fontsize=9)
    ax.set_title(re.sub(r"\s*\(antall og prosent\)", "", tittel), loc="left", fontsize=13)
    fig.tight_layout()

    filnavn = tittel.lower().translate(str.maketrans({"æ": "ae", "ø": "o", "å": "a"}))
    filnavn = re.sub(r"[^a-z0-9]+", "_", filnavn).strip("_") + ".png"
    UT_MAPPE.mkdir(exist_ok=True)
    fig.savefig(UT_MAPPE / filnavn, dpi=150)
    plt.close(fig)
    return filnavn


def main(titler):
    tabeller = les_tabeller()
    for tittel in titler:
        if tittel not in tabeller:
            print(f"Fant ikke tabellen: {tittel}")
            continue
        print(f"Lagret {lag_graf(tittel, tabeller[tittel])}")


if __name__ == "__main__":
    main(sys.argv[1:] or TITLER)
