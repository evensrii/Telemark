---
description: Current plan and status for the Flyttestrømmer visualization app
---

# Flyttestrømmer – Telemark (Visualization)

## Architecture
Single self-contained HTML file deployed via GitHub Pages. All data is fetched dynamically from CSV files on GitHub at page load — no hardcoded data, no build step.

**All future changes are made directly to `docs/Flytting/index.html`.**
The Python HTML generator (`visualisering_flyttestroemmer.py`) is retired — it embedded data as JSON via f-strings, which is no longer needed since the page fetches CSVs at runtime.

## Files
- **GitHub Pages site**: `docs/Flytting/index.html` (the live visualization)
- **Data script**: `Python/Queries/01_Befolkning/Flytting/flytting_til_og_fra_kommunene.py`
- **CSV output (fraflytting)**: `Data/01_Befolkning/Flytting/fraflytting_fra_telemarkskommuner.csv`
- **CSV output (tilflytting)**: `Data/01_Befolkning/Flytting/tilflytting_til_telemarkskommuner.csv`
- **Fylkesinndeling metadata**: `Python/Metadata/ssb_klass_fylkesinndeling_2026.csv`
- **Legacy (not deployed)**: `Egne applikasjoner/Flytting/visualisering_flyttestroemmer.py` + `flyttestroemmer.html`

## Data pipeline
1. `flytting_til_og_fra_kommunene.py` fetches SSB table 13864 (inter-municipal migration), latest year via `top(1)`
2. Adds kommune numbers, county names (from fylkesinndeling CSV), and coordinates
3. Outputs two CSVs with columns: `År, Fra kommune, Fra fylke, Til kommune, Til fylke, Antall, Fra lat, Fra lon, Til lat, Til lon`
4. CSVs are pushed to `Data/01_Befolkning/Flytting/` on GitHub

## Client-side data loading (index.html)
1. PapaParse (CDN) parses CSVs fetched from GitHub raw URLs
2. `addInternalFlag()` derives the `internal` boolean (both Fra and Til in Telemark)
3. `aggregateToFylke()` builds county-level aggregates (excluding internal Telemark moves)
4. Year is extracted from the `År` column and displayed in the header subtitle
5. Loading overlay shown while data fetches, hidden when ready

## Deployment
- GitHub Pages serves `docs/` folder
- URL: `https://evensrii.github.io/Telemark/Flytting/`
- No build step — just push changes to `docs/Flytting/index.html`
- Other pages can coexist in separate subfolders under `docs/`

## Completed features

### Layout & header
- [x] 3-column layout: sidebar (280px) | map (flex) | right panel (760px)
- [x] No scroll — page fits 100vh
- [x] Taller header (90px min-height, 32px font, 54px logo, 16px subtitle)
- [x] Centered title "Flyttestrømmer Telemark", left-aligned subtitle with level + year
- [x] Telemark fylkeskommune logo in header (from GitHub raw URL)
- [x] Header subtitle shows "Fylkesnivå - {year}" / "Kommunenivå - {year}" dynamically

### Map
- [x] County-level (Fylke) as default view — aggregated flows between Telemark and other counties
- [x] Municipality-level (Kommune) view with Skien as default
- [x] Clean flow arrows (Bézier curves, no polygon arrowheads)
- [x] Direction gradient on flow lines (light at origin → opaque at destination)
- [x] Flow lines: smooth gradient with 30 segments, butt lineCap, overlapping points
- [x] Flow lines: origin dot markers (small colored circle at flow start)
- [x] Thicker lines for clustered flows (log2 boost)
- [x] Hover highlight on flow lines (+3 weight, full opacity)
- [x] Invisible wider hit-area polyline for easier tooltip access
- [x] Arrow (→) in all tooltips: lines, markers, clustered flows
- [x] Fylke view: marker tooltips say "Til Telemark" / "Fra Telemark"
- [x] Kommune view: origin markers drawn (like fylke view)
- [x] Data labels on map markers (all labeled in fylke view; zoom-responsive in kommune view)
- [x] Data labels persist during zoom
- [x] Data labels: geographic bias — labels far from Oslo get distance bonus
- [x] Data labels: major cities (Oslo, Bergen, Stavanger, etc.) always shown if in flows
- [x] Fylke view: setView([64.5, 13.0], 5) for tighter framing
- [x] Kommune view: setView([59.2, 9.0], 8)
- [x] Map zoom: setTimeout wrapper for reliable setView after DOM/Plotly updates
- [x] No auto-zoom when adjusting cluster slider or threshold

### Sidebar controls
- [x] Level toggle (Fylke / Kommune)
- [x] Direction select (Tilflytting / Fraflytting / Begge retninger for fylke)
- [x] Kommune view: "Begge retninger" removed, default Tilflytting
- [x] Internal movement: 3-option radio (exclude / include / only internal Telemark)
- [x] Radio labels: "Uten intern flytting i fylket" / "Med intern flytting i fylket"
- [x] "Kun intern" mode: auto-set threshold=1, cluster=0%, force all labels
- [x] Threshold slider for minimum count (kommune view only)
- [x] Distance-based flow clustering with adjustable slider (kommune view only)
  - Radius scales with distance from Telemark
  - Clustered nodes labeled "Bergen og omegn" / "m. fl." (internal mode)
- [x] Merged threshold+cluster into "Forenkle kartvisningen" section
- [x] Stats box: Fraflytting number in red, netto green/red
- [x] Stats labels: dynamic "(uten intern flytting)" / "(med intern flytting)" / "(kun internt i Telemark)"
- [x] More spacing between sidebar sections (gap 20px)

### Right panel (table + Sankeys)
- [x] Right panel: table, Sankey Tilflytting, Sankey Fraflytting — equal 1/3 heights
- [x] Table container: flex 1.4 for more vertical space
- [x] Table filters by selected direction
- [x] Table titles: "Flyttestrømmer – Telemark" / "Flyttestrømmer – [kommune]"
- [x] Table: show all rows in kommune view
- [x] Sankey nodes colored with Telemark fylke palette (14 distinct colors)
- [x] Sankey tooltips: integer values + "personer", arrow format
- [x] Sankey bands: hand cursor on hover
- [x] Sankey titles: "Flytting til/fra Telemark" (fylke) / "Flytting til/fra [kommune]" (kommune)
- [x] Sankey drill-down in kommune view: click a link to see municipality breakdown
- [x] Sankey drilldown title: "Flytting til/fra [kommune] – [fylke]"
- [x] Sankey back button: "← Tilbake til fylkeoversikt"
- [x] Sankey drill-down: Plotly.purge before newPlot to prevent handler stacking
- [x] Kommune view: show only one Sankey (til or fra) based on direction selection

### Data loading (GitHub Pages version)
- [x] Dynamic CSV fetching from GitHub raw URLs (PapaParse)
- [x] Client-side derivation of `internal` flag
- [x] Client-side county-level aggregation
- [x] Loading overlay with spinner while data fetches
- [x] Year extracted from CSV `År` column and shown in subtitle
- [x] Deferred rendering — page renders only after data is loaded
- [x] Error message displayed if CSV fetch fails
- [x] Threshold slider only affects map lines + table, not stats or Sankeys

### Styling
- [x] Bigger fonts (body 16px, controls 14px, Sankey 13px, header 24px)
- [x] Removed border layers (GeoJSON + WFS)

## Pending / ideas
- [ ] Consider adding year selector if multi-year data becomes available
- [ ] Tune clustering slider default value after testing
- [ ] Consider removing clustering slider after finding optimal value
- [ ] Test full end-to-end flow: run Python script → push CSVs → verify GitHub Pages site
