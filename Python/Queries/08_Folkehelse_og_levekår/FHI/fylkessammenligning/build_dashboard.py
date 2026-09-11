"""
Fylkessammenligning Dashboard Builder
=======================================
Reads results/fylkessammenligning_summary.csv and renders it into a
self-contained dashboard HTML page (results/dashboard.html) for publishing
as a Claude Artifact.

Groups the flat summary rows by base indicator (one row per FHI table x
breakdown-variant) so the default view surfaces one headline per indicator
(its most extreme breakdown variant), with all variants available on
expand.

Usage:
    python build_dashboard.py
"""

import json
import re
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
SUMMARY_CSV = BASE_DIR / "results" / "fylkessammenligning_summary.csv"
QUERIES_DIR = BASE_DIR.parent / "queries"
OUTPUT_HTML = BASE_DIR / "results" / "dashboard.html"
OUTPUT_HTML_STANDALONE = BASE_DIR / "results" / "dashboard_standalone.html"


def sanitize_filename(filename):
    """Same sanitizer as generate_scripts.py, so this maps indicator slugs
    back to their original (properly cased, æøå-preserving) Norwegian name."""
    name = filename.lower()
    replacements = {"å": "aa", "æ": "ae", "ø": "oe", "é": "e", "è": "e", "ê": "e", "ü": "u", "ö": "o", "ä": "a"}
    for old, new in replacements.items():
        name = name.replace(old, new)
    name = re.sub(r"[^a-z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    name = name.replace(" ", "_")
    return name.strip("_")


def build_label_map():
    """indikator slug -> original Norwegian query name, for nicer display labels."""
    label_map = {}
    for txt_path in QUERIES_DIR.rglob("*.txt"):
        stem = txt_path.stem
        label_map[sanitize_filename(stem)] = stem
    return label_map


def main():
    df = pd.read_csv(SUMMARY_CSV, encoding="utf-8")
    df["fylke_verdier"] = df["fylke_verdier"].apply(json.loads)
    label_map = build_label_map()

    groups = []
    for indikator, g in df.groupby("indikator", sort=False):
        g = g.sort_values("z_score", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
        variants = g.to_dict(orient="records")
        headline = variants[0]
        groups.append({
            "indikator": indikator,
            "label": label_map.get(indikator, indikator.replace("_", " ").capitalize()),
            "headline": headline,
            "variants": variants,
            "significant_count": int(g["significant"].sum()),
            "total_variants": len(g),
        })

    groups.sort(key=lambda x: abs(x["headline"]["z_score"]), reverse=True)

    total_indicators = len(groups)
    total_variants = len(df)
    total_significant = int(df["significant"].sum())
    higher_count = int(((df["z_score"] > 0) & df["significant"]).sum())
    lower_count = int(((df["z_score"] < 0) & df["significant"]).sum())
    indicators_with_deviation = sum(1 for g in groups if g["significant_count"] > 0)

    stats = {
        "total_indicators": total_indicators,
        "total_variants": total_variants,
        "indicators_with_deviation": indicators_with_deviation,
        "total_significant": total_significant,
        "higher_count": higher_count,
        "lower_count": lower_count,
    }

    def to_native(o):
        if isinstance(o, (pd.Timestamp,)):
            return str(o)
        if hasattr(o, "item"):  # numpy scalar (bool_, int64, float64, ...)
            return o.item()
        raise TypeError(f"Cannot serialize {type(o)}: {o!r}")

    data_json = json.dumps({"groups": groups, "stats": stats}, ensure_ascii=False, allow_nan=False, default=to_native)

    html = render_html(data_json)
    OUTPUT_HTML.parent.mkdir(exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    OUTPUT_HTML_STANDALONE.write_text(wrap_standalone(html), encoding="utf-8")
    print(f"Wrote dashboard fragment (for Artifact publishing) to {OUTPUT_HTML}")
    print(f"Wrote standalone document (for opening directly / local dev server) to {OUTPUT_HTML_STANDALONE}")
    print(f"  {total_indicators} indicators, {total_variants} variants, {total_significant} significant "
          f"({higher_count} higher, {lower_count} lower)")


def render_html(data_json):
    return TEMPLATE.replace("__DATA_JSON__", data_json)


def wrap_standalone(fragment_html):
    """The Artifact tool requires a bare fragment (no doctype/html/head/body -
    it wraps the file itself at publish time). But opening that same fragment
    directly in a browser (double-click, or a local dev server like Five
    Server) skips that wrapping, and some servers/extensions mis-handle a
    <script> tag that isn't inside a properly declared <head>/<body> document
    - producing exactly the "raw JS printed as page text" bug. This wraps the
    identical content into a normal standalone HTML5 document for that case.

    Forces the dark theme (data-theme="dark") regardless of the recipient's
    system setting - this file is meant to be emailed/shared as a fixed
    snapshot, so it should look the same (the dark palette used on the
    Artifact page) for every recipient rather than following their OS theme."""
    marker = '<div class="page">'
    split_at = fragment_html.index(marker)
    head_part, body_part = fragment_html[:split_at], fragment_html[split_at:]
    return f'''<!doctype html>
<html lang="no" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
{head_part}
</head>
<body>
{body_part}
</body>
</html>'''


TEMPLATE = r"""<title>Fylkessammenligning</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700;800&family=Public+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500;600&display=swap">
<style>
:root {
  --bg: #f6f8f9;
  --surface: #ffffff;
  --surface-2: #eef2f4;
  --ink: #16222b;
  --ink-muted: #55636d;
  --border: #dce3e7;
  --accent: #2c4a5e;
  --accent-soft: #e4ebee;
  --higher: #b54b3a;
  --higher-soft: #f6e6e2;
  --lower: #0089a0;
  --lower-soft: #dcf0f3;
  --neutral-data: #8b95a1;
  --focus: #2c4a5e;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg: #10171d;
    --surface: #182229;
    --surface-2: #1e2930;
    --ink: #e7edf1;
    --ink-muted: #9aabb5;
    --border: #2a3740;
    --accent: #6e93a8;
    --accent-soft: #22323c;
    --higher: #cc6b4e;
    --higher-soft: #3a2620;
    --lower: #009ab2;
    --lower-soft: #10333a;
    --neutral-data: #7a8792;
    --focus: #6e93a8;
  }
}
:root[data-theme="dark"] {
  --bg: #10171d;
  --surface: #182229;
  --surface-2: #1e2930;
  --ink: #e7edf1;
  --ink-muted: #9aabb5;
  --border: #2a3740;
  --accent: #6e93a8;
  --accent-soft: #22323c;
  --higher: #cc6b4e;
  --higher-soft: #3a2620;
  --lower: #009ab2;
  --lower-soft: #10333a;
  --neutral-data: #7a8792;
  --focus: #6e93a8;
}

* { box-sizing: border-box; }
body {
  background: var(--bg);
  color: var(--ink);
  font: 15px/1.5 "Public Sans", system-ui, sans-serif;
  margin: 0;
}
h1, h2, h3, .num-label {
  font-family: "Archivo", system-ui, sans-serif;
  text-wrap: balance;
}
.mono, .z-badge, .rank-badge, .value, .stat-value {
  font-family: "IBM Plex Mono", ui-monospace, monospace;
  font-variant-numeric: tabular-nums;
}

a { color: var(--accent); }

.page {
  max-width: 1040px;
  margin: 0 auto;
  padding: 28px 20px 80px;
}

header.top {
  display: flex;
  flex-wrap: wrap;
  gap: 24px;
  justify-content: space-between;
  align-items: flex-end;
  padding-bottom: 20px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 20px;
}
header.top .titles h1 {
  font-size: 28px;
  font-weight: 800;
  margin: 0 0 6px;
  letter-spacing: -0.01em;
}
header.top .titles p {
  margin: 0;
  color: var(--ink-muted);
  max-width: 60ch;
  font-size: 14px;
}

.stats-row {
  display: grid;
  grid-template-columns: repeat(5, minmax(90px, 1fr));
  gap: 10px;
}
.stat-tile {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 10px 14px;
  min-width: 110px;
  position: relative;
  cursor: help;
}
.stat-tile .stat-value {
  font-size: 22px;
  font-weight: 600;
  line-height: 1.1;
}
.stat-tile .stat-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--ink-muted);
  margin-top: 3px;
  border-bottom: 1px dotted var(--ink-muted);
  display: inline-block;
}
.stat-tile.split .stat-value { display: flex; align-items: baseline; gap: 8px; }
.stat-tile.split .higher { color: var(--higher); }
.stat-tile.split .lower { color: var(--lower); }

.stat-tile[data-tooltip]:hover::after,
.stat-tile[data-tooltip]:focus-visible::after {
  content: attr(data-tooltip);
  position: absolute;
  left: 0;
  top: calc(100% + 9px);
  z-index: 20;
  width: max-content;
  max-width: 280px;
  background: var(--ink);
  color: var(--bg);
  font: 12px/1.45 "Public Sans", sans-serif;
  font-weight: 400;
  text-transform: none;
  letter-spacing: normal;
  padding: 9px 11px;
  border-radius: 8px;
  white-space: pre-line;
  box-shadow: 0 6px 18px rgba(0,0,0,0.22);
  pointer-events: none;
}
.stat-tile[data-tooltip]:hover::before,
.stat-tile[data-tooltip]:focus-visible::before {
  content: "";
  position: absolute;
  left: 16px;
  top: 100%;
  border: 6px solid transparent;
  border-bottom-color: var(--ink);
  z-index: 20;
}
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

.controls {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
  margin-bottom: 18px;
  position: sticky;
  top: 0;
  background: var(--bg);
  padding: 10px 0;
  z-index: 5;
}
.controls input[type="search"] {
  flex: 1 1 220px;
  padding: 8px 12px;
  border-radius: 8px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--ink);
  font: inherit;
}
.controls input[type="search"]:focus-visible,
.chip:focus-visible,
.group-head:focus-visible,
.variant-row:focus-visible {
  outline: 2px solid var(--focus);
  outline-offset: 2px;
}
.chips { display: flex; gap: 6px; flex-wrap: wrap; }
.chip {
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--ink-muted);
  border-radius: 999px;
  padding: 6px 12px;
  font-size: 13px;
  cursor: pointer;
  font-family: "Public Sans", sans-serif;
}
.chip[aria-pressed="true"] {
  background: var(--accent-soft);
  border-color: var(--accent);
  color: var(--ink);
  font-weight: 600;
}
.toggle-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: var(--ink-muted);
  cursor: pointer;
  user-select: none;
}

.legend {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  font-size: 12px;
  color: var(--ink-muted);
  margin-bottom: 14px;
}
.legend .swatch { display: inline-flex; align-items: center; gap: 5px; }
.legend .dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }

.group-list { display: flex; flex-direction: column; gap: 8px; }

.group {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
}
.group-head {
  display: grid;
  grid-template-columns: 1fr 190px 70px 46px 22px;
  gap: 14px;
  align-items: center;
  padding: 12px 14px;
  cursor: pointer;
  background: none;
  border: none;
  width: 100%;
  text-align: left;
  color: inherit;
  font: inherit;
}
.group-head:hover { background: var(--surface-2); }
.group-head .titles { min-width: 0; }
.group-head .ind-label {
  font-weight: 600;
  font-size: 14px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.group-head .breakdown {
  font-size: 12px;
  color: var(--ink-muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.group-head .sig-count {
  font-size: 11px;
  color: var(--ink-muted);
  margin-left: 6px;
}

.range-bar { width: 190px; height: 30px; }
.z-badge {
  font-size: 13px;
  font-weight: 600;
  text-align: right;
  padding: 3px 7px;
  border-radius: 6px;
}
.z-badge.higher { color: var(--higher); background: var(--higher-soft); }
.z-badge.lower { color: var(--lower); background: var(--lower-soft); }
.z-badge.flat { color: var(--ink-muted); background: var(--surface-2); }

.rank-badge {
  font-size: 12px;
  color: var(--ink-muted);
  text-align: right;
  white-space: nowrap;
}

.chev {
  transition: transform 0.15s ease;
  color: var(--ink-muted);
  justify-self: end;
}
.group.open .chev { transform: rotate(90deg); }

.group-body {
  display: none;
  border-top: 1px solid var(--border);
  padding: 14px;
  background: var(--surface-2);
}
.group.open .group-body { display: block; }

.detail-strip { margin-bottom: 14px; }
.detail-strip svg { width: 100%; height: auto; display: block; }
.detail-caption {
  font-size: 12px;
  color: var(--ink-muted);
  margin-bottom: 6px;
}

.variant-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.variant-table th {
  text-align: left;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  color: var(--ink-muted);
  font-weight: 500;
  padding: 4px 8px;
  border-bottom: 1px solid var(--border);
}
.variant-table td { padding: 6px 8px; border-bottom: 1px solid var(--border); vertical-align: middle; }
.variant-row { cursor: pointer; background: none; border: 0; width: 100%; }
.variant-row:hover td { background: var(--surface); }
.variant-row.selected td { background: var(--accent-soft); }
.variant-table .value { text-align: right; white-space: nowrap; }
.variant-table .breakdown-cell {
  max-width: 320px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.empty-state {
  text-align: center;
  color: var(--ink-muted);
  padding: 50px 0;
  font-size: 14px;
}

footer.note {
  margin-top: 28px;
  font-size: 12px;
  color: var(--ink-muted);
  border-top: 1px solid var(--border);
  padding-top: 14px;
}

.list-header {
  display: grid;
  grid-template-columns: 1fr 190px 70px 46px 22px;
  gap: 14px;
  padding: 0 14px 6px;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--ink-muted);
}
.list-header .col-range,
.list-header .col-z,
.list-header .col-rank {
  text-align: right;
}

@media (max-width: 640px) {
  .group-head, .list-header { grid-template-columns: 1fr 34px 22px; }
  .range-bar, .rank-badge, .list-header .col-range, .list-header .col-rank { display: none; }
  .stats-row { grid-template-columns: repeat(2, 1fr); }
}
</style>

<div class="page">
  <header class="top">
    <div class="titles">
      <h1>Fylkessammenligning</h1>
      <p>Telemark sammenlignet med de 14 andre norske fylkene, på tvers av FHI sine folkehelseindikatorer, for nyeste tilgjengelige år per indikator. For hver indikator beregnes hvor mange standardavvik (z-score) Telemarks verdi ligger fra gjennomsnittet til de andre 14 fylkene. En forskjell merkes som «betydelig» når |z| ≥ 1,5, eller når Telemark er blant de to høyeste eller to laveste av alle 15 fylker. Dette er en utforskende oversikt for å finne indikatorer verdt å se nærmere på - <strong>ikke</strong> en formell statistisk signifikanstest.</p>
    </div>
    <div class="stats-row" id="stats-row"></div>
  </header>

  <div class="legend">
    <span class="swatch"><span class="dot" style="background:var(--higher)"></span> Telemark høyere enn andre fylker</span>
    <span class="swatch"><span class="dot" style="background:var(--lower)"></span> Telemark lavere enn andre fylker</span>
    <span class="swatch"><span class="dot" style="background:var(--neutral-data)"></span> Hele landet (referanse)</span>
  </div>

  <div class="controls">
    <input type="search" id="search" placeholder="Søk i indikatorer …" aria-label="Søk i indikatorer">
    <div class="chips" id="direction-chips" role="group" aria-label="Filtrer på retning"></div>
    <label class="toggle-label">
      <input type="checkbox" id="sig-only" checked>
      Vis kun betydelige forskjeller
    </label>
  </div>

  <div class="list-header">
    <span class="col-indicator">Indikator</span>
    <span class="col-range">Fylkesfordeling</span>
    <span class="col-z">Z-verdi</span>
    <span class="col-rank">Rangering</span>
    <span></span>
  </div>
  <div class="group-list" id="group-list"></div>
  <div class="empty-state" id="empty-state" hidden>Ingen indikatorer matcher filteret.</div>

  <footer class="note">
    Datagrunnlag: FHI Statistikkbank, siste tilgjengelige år per indikator. z-score beregnes mot de 14 andre fylkene (Hele landet holdes utenfor som referanse). Betydelig forskjell = |z| ≥ 1,5 eller Telemark er blant de to høyeste/laveste av 15 fylker. Ikke en formell statistisk test. Lokal analyse, ikke publisert til nettsiden.
  </footer>
</div>

<script>
const DATA = __DATA_JSON__;

const fmt = (v, decimals = 3) => {
  if (v === null || v === undefined || Number.isNaN(v)) return "–";
  return Number(v).toLocaleString("nb-NO", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
};
const fmtZ = (v) => (v > 0 ? "+" : "") + v.toFixed(2);

function directionOf(row) {
  if (!row.significant) return "flat";
  return row.z_score > 0 ? "higher" : "lower";
}

function escapeXml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function hoverDot(cx, cy, label, visibleMarkup, hitRadius = 7) {
  return `<g>
    <circle cx="${cx}" cy="${cy}" r="${hitRadius}" fill="transparent" style="cursor:help"><title>${escapeXml(label)}</title></circle>
    ${visibleMarkup}
  </g>`;
}

function rangeBarSVG(row, width = 190, height = 30) {
  const vals = Object.values(row.fylke_verdier).filter((v) => v !== null && v !== undefined);
  const lo = Math.min(...vals);
  const hi = Math.max(...vals);
  const pad = (hi - lo) * 0.12 || Math.abs(hi || 1) * 0.1;
  const min = lo - pad, max = hi + pad;
  const x = (v) => 6 + ((v - min) / (max - min || 1)) * (width - 12);
  const midY = height / 2;
  const dir = directionOf(row);
  const color = dir === "higher" ? "var(--higher)" : dir === "lower" ? "var(--lower)" : "var(--neutral-data)";

  const otherDots = Object.entries(row.fylke_verdier)
    .filter(([f, v]) => f !== "Telemark" && f !== "Hele landet" && v !== null)
    .map(([f, v]) => hoverDot(
      x(v).toFixed(1), midY, `${f}: ${fmt(v)}`,
      `<circle cx="${x(v).toFixed(1)}" cy="${midY}" r="2.4" fill="var(--neutral-data)" opacity="0.55" pointer-events="none"/>`
    ))
    .join("");

  const landet = row.landet_verdi;
  const landetMark = (landet !== null && landet !== undefined)
    ? hoverDot(
        x(landet).toFixed(1), midY, `Hele landet: ${fmt(landet)}`,
        `<rect x="${(x(landet) - 3).toFixed(1)}" y="${(midY - 3).toFixed(1)}" width="6" height="6" fill="var(--ink-muted)" transform="rotate(45 ${x(landet).toFixed(1)} ${midY})" pointer-events="none"/>`
      )
    : "";

  const tx = x(row.telemark_verdi);
  const telemarkMark = hoverDot(
    tx.toFixed(1), midY, `Telemark: ${fmt(row.telemark_verdi)}`,
    `<circle cx="${tx.toFixed(1)}" cy="${midY}" r="5" fill="${color}" stroke="var(--surface)" stroke-width="1.5" pointer-events="none"/>`,
    8
  );

  return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Telemark ${fmt(row.telemark_verdi)} mot andre fylker">
    <line x1="6" y1="${midY}" x2="${width - 6}" y2="${midY}" stroke="var(--border)" stroke-width="1"/>
    ${otherDots}
    ${landetMark}
    ${telemarkMark}
  </svg>`;
}

function detailStripSVG(row, width = 900, height = 130) {
  const entries = Object.entries(row.fylke_verdier).filter(([, v]) => v !== null && v !== undefined);
  const vals = entries.map(([, v]) => v);
  const lo = Math.min(...vals), hi = Math.max(...vals);
  const pad = (hi - lo) * 0.1 || Math.abs(hi || 1) * 0.1;
  const min = lo - pad, max = hi + pad;
  const marginX = 70;
  const x = (v) => marginX + ((v - min) / (max - min || 1)) * (width - marginX * 2);
  const baseY = 60;
  const dir = directionOf(row);
  const color = dir === "higher" ? "var(--higher)" : dir === "lower" ? "var(--lower)" : "var(--neutral-data)";

  const sorted = entries.slice().sort((a, b) => a[1] - b[1]);
  const labelRows = sorted.map(([fylke], i) => {
    const isTelemark = fylke === "Telemark";
    const isLandet = fylke === "Hele landet";
    const y = 95 + (i % 2) * 14;
    if (!isTelemark && !isLandet) return "";
    return `<text x="${x(sorted.find(([f]) => f === fylke)[1]).toFixed(1)}" y="${y}" font-size="11" font-family="Public Sans, sans-serif" fill="${isTelemark ? color : "var(--ink-muted)"}" text-anchor="middle" font-weight="${isTelemark ? 600 : 400}">${fylke}</text>`;
  }).join("");

  const dots = entries.map(([fylke, v]) => {
    if (fylke === "Telemark") return "";
    if (fylke === "Hele landet") {
      return hoverDot(
        x(v).toFixed(1), baseY, `Hele landet: ${fmt(v)}`,
        `<rect x="${(x(v) - 4).toFixed(1)}" y="${baseY - 4}" width="8" height="8" fill="var(--ink-muted)" transform="rotate(45 ${x(v).toFixed(1)} ${baseY})" pointer-events="none"/>`,
        9
      );
    }
    return hoverDot(
      x(v).toFixed(1), baseY, `${fylke}: ${fmt(v)}`,
      `<circle cx="${x(v).toFixed(1)}" cy="${baseY}" r="4.5" fill="var(--neutral-data)" opacity="0.65" pointer-events="none"/>`,
      9
    );
  }).join("");

  const tv = row.telemark_verdi;
  const telemark = hoverDot(
    x(tv).toFixed(1), baseY, `Telemark: ${fmt(tv)}`,
    `<circle cx="${x(tv).toFixed(1)}" cy="${baseY}" r="7.5" fill="${color}" stroke="var(--surface)" stroke-width="2" pointer-events="none"/>`,
    11
  );

  const axisLabels = [min + pad, (min + max) / 2, max - pad].map((v) =>
    `<text x="${x(v).toFixed(1)}" y="${baseY + 32}" font-size="10" font-family="IBM Plex Mono, monospace" fill="var(--ink-muted)" text-anchor="middle">${fmt(v, 2)}</text>`
  ).join("");

  return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Alle fylker for valgt variant">
    <line x1="${marginX}" y1="${baseY}" x2="${width - marginX}" y2="${baseY}" stroke="var(--border)" stroke-width="1"/>
    ${dots}
    ${telemark}
    ${labelRows}
    ${axisLabels}
  </svg>`;
}

function rankTitle(row) {
  const missing = 14 - row.andre_fylker_antall;
  if (missing <= 0) return "";
  const plural = missing > 1 ? "r" : "";
  return ` title="Rangert blant ${row.rangering_av} fylker - ${missing} fylke${plural} mangler data for denne varianten"`;
}

function zBadgeClass(row) {
  const d = directionOf(row);
  return d === "flat" ? "flat" : d;
}

const STAT_TOOLTIPS = {
  indicators:
    "Antall FHI-datasett som inngår i sammenligningen - hvert datasett er én indikator.\n\n" +
    "Eks: «Barnevern» og «Hoftebrudd» er to av de 27 indikatorene.",
  variants:
    "Antall unike kombinasjoner av en indikator og dens undergrupper (alder, diagnose, innvandringsbakgrunn osv.), sammenlignet hver for seg.\n\n" +
    "Eks: «Sykehusinnlagte sykdommer og skader» for 45-64 år med diagnosen Forgiftninger er én variant. Samme indikator for 25-44 år med Hodeskader er en annen.",
  withDeviation:
    "Antall av de 27 indikatorene som har minst én variant med betydelig forskjell fra de andre fylkene.\n\n" +
    "Eks: «Sykehusinnlagte sykdommer og skader» telles med her selv om bare noen få av dens mange varianter har en betydelig forskjell.",
  significant:
    "Antall indikatorvarianter der Telemark tydelig skiller seg fra de andre fylkene: |z| ≥ 1,5, eller Telemark er blant de to høyeste/laveste av alle 15 fylker. Dette er en tommelfingerregel for å finne avvik verdt å se nærmere på - ikke en statistisk signifikanstest.\n\n" +
    "Eks: Forgiftninger 45-64 år (z=+5,8) og hjerte-karkonsultasjoner 75-79 år (Telemark lavest av 15) telles begge med.",
  direction:
    "Av variantene med betydelig forskjell: hvor mange ganger Telemark ligger over snittet for de andre fylkene, og hvor mange ganger under.\n\n" +
    "Eks: Forgiftninger telles som «høyere» (Telemark over snittet). Hjerte-karkonsultasjoner blant eldre telles som «lavere» (Telemark under snittet).",
};

function statTile(value, label, tooltip, extraClass = "") {
  return `<div class="stat-tile ${extraClass}" tabindex="0" data-tooltip="${tooltip.replace(/"/g, "&quot;")}">
    <div class="stat-value">${value}</div>
    <div class="stat-label">${label}</div>
    <span class="sr-only">${tooltip}</span>
  </div>`;
}

function renderStats() {
  const s = DATA.stats;
  const el = document.getElementById("stats-row");
  el.innerHTML =
    statTile(s.total_indicators, "Indikatorer", STAT_TOOLTIPS.indicators) +
    statTile(s.total_variants, "Indikatorvarianter", STAT_TOOLTIPS.variants) +
    statTile(s.indicators_with_deviation, "Indikatorer med avvik", STAT_TOOLTIPS.withDeviation) +
    statTile(s.total_significant, "Betydelige avvik", STAT_TOOLTIPS.significant) +
    statTile(`<span class="higher">${s.higher_count}↑</span><span class="lower">${s.lower_count}↓</span>`, "Høyere / lavere", STAT_TOOLTIPS.direction, "split");
}

let directionFilter = "all";

function renderDirectionChips() {
  const el = document.getElementById("direction-chips");
  const options = [
    ["all", "Alle"],
    ["higher", "Telemark høyere"],
    ["lower", "Telemark lavere"],
  ];
  el.innerHTML = options.map(([key, label]) =>
    `<button class="chip" data-dir="${key}" aria-pressed="${key === directionFilter}">${label}</button>`
  ).join("");
  el.querySelectorAll(".chip").forEach((btn) => {
    btn.addEventListener("click", () => {
      directionFilter = btn.dataset.dir;
      renderDirectionChips();
      renderGroups();
    });
  });
}

function matchesFilters(group) {
  const q = document.getElementById("search").value.trim().toLowerCase();
  if (q && !group.label.toLowerCase().includes(q)) return false;
  const sigOnly = document.getElementById("sig-only").checked;
  if (sigOnly && group.significant_count === 0) return false;
  if (directionFilter !== "all") {
    const dir = directionOf(group.headline);
    if (directionFilter === "higher" && dir !== "higher") return false;
    if (directionFilter === "lower" && dir !== "lower") return false;
  }
  return true;
}

const openGroups = new Set();
const selectedVariant = {};

function renderGroups() {
  const list = document.getElementById("group-list");
  const empty = document.getElementById("empty-state");
  const groups = DATA.groups.filter(matchesFilters);
  empty.hidden = groups.length > 0;
  list.innerHTML = groups.map((g) => groupHTML(g)).join("");

  groups.forEach((g) => {
    const groupEl = list.querySelector(`[data-group="${cssEscape(g.indikator)}"]`);
    const head = groupEl.querySelector(".group-head");
    head.addEventListener("click", () => {
      if (openGroups.has(g.indikator)) openGroups.delete(g.indikator);
      else openGroups.add(g.indikator);
      renderGroups();
    });
    if (openGroups.has(g.indikator)) {
      groupEl.classList.add("open");
      const body = groupEl.querySelector(".group-body");
      renderGroupBody(body, g);
    }
  });
}

function cssEscape(s) {
  return s.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function groupHTML(g) {
  const h = g.headline;
  const sigNote = g.total_variants > 1
    ? `<span class="sig-count">${g.significant_count}/${g.total_variants} varianter med betydelig forskjell</span>`
    : "";
  return `
    <div class="group" data-group="${cssEscape(g.indikator)}">
      <button class="group-head" aria-expanded="${openGroups.has(g.indikator)}">
        <div class="titles">
          <div class="ind-label">${g.label}${sigNote}</div>
          <div class="breakdown">${h.breakdown === "Totalt" ? "Totalt, " + h.år.slice(0,4) : h.breakdown + " · " + h.år.slice(0,4)}</div>
        </div>
        <div class="range-bar">${rangeBarSVG(h)}</div>
        <div class="z-badge ${zBadgeClass(h)}">${fmtZ(h.z_score)}</div>
        <div class="rank-badge"${rankTitle(h)}>#${h.rangering}/${h.rangering_av}</div>
        <svg class="chev" width="16" height="16" viewBox="0 0 16 16"><path d="M6 3l5 5-5 5" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </button>
      <div class="group-body"></div>
    </div>
  `;
}

function renderGroupBody(body, g) {
  const selected = selectedVariant[g.indikator] || g.headline;
  const showTable = g.total_variants > 1;
  body.innerHTML = `
    <div class="detail-strip">
      <div class="detail-caption">${selected.breakdown === "Totalt" ? "Totalt" : selected.breakdown} · ${selected.år.slice(0,4)} · ${selected.verdi_type} · Telemark ${fmt(selected.telemark_verdi)} vs. andre fylker snitt ${fmt(selected.andre_fylker_snitt)}</div>
      ${detailStripSVG(selected)}
    </div>
    ${showTable ? `
    <table class="variant-table">
      <thead><tr><th>Variant</th><th style="text-align:right">Telemark</th><th style="text-align:right">Andre snitt</th><th style="text-align:right">z</th><th style="text-align:right">Rangering</th></tr></thead>
      <tbody>
        ${g.variants.map((v, i) => `
          <tr class="variant-row ${v === selected ? "selected" : ""}" data-idx="${i}">
            <td class="breakdown-cell">${v.breakdown}</td>
            <td class="value">${fmt(v.telemark_verdi)}</td>
            <td class="value">${fmt(v.andre_fylker_snitt)}</td>
            <td class="value">${fmtZ(v.z_score)}</td>
            <td class="value"${rankTitle(v)}>#${v.rangering}/${v.rangering_av}</td>
          </tr>`).join("")}
      </tbody>
    </table>` : ""}
  `;
  if (showTable) {
    body.querySelectorAll(".variant-row").forEach((tr) => {
      tr.addEventListener("click", (e) => {
        e.stopPropagation();
        selectedVariant[g.indikator] = g.variants[Number(tr.dataset.idx)];
        renderGroupBody(body, g);
      });
    });
  }
}

document.getElementById("search").addEventListener("input", renderGroups);
document.getElementById("sig-only").addEventListener("change", renderGroups);

renderStats();
renderDirectionChips();
renderGroups();
</script>
"""

if __name__ == "__main__":
    main()
