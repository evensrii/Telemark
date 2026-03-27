---
description: Current plan and status for the Flyttestrømmer visualization app
---

# Flyttestrømmer – Telemark (Visualization)

## Files
- **Data script**: `Python/Queries/01_Befolkning/Flytting/flytting_til_og_fra_kommunene.py`
- **Visualization script**: `Egne applikasjoner/Flytting/visualisering_flyttestroemmer.py`
- **Output HTML**: `Egne applikasjoner/Flytting/flyttestroemmer.html`
- **Telemark GeoJSON**: `Kart/Kartfiler/Telemark_kommuner_WGS1984.geojson`
- **Fylkesinndeling metadata**: `Python/Metadata/ssb_klass_fylkesinndeling_2026.csv`

## Data pipeline
1. `flytting_til_og_fra_kommunene.py` fetches SSB table 13864 (inter-municipal migration)
2. Adds kommune numbers, county names (from fylkesinndeling CSV), and coordinates
3. Outputs two CSVs with columns: `Fra kommune, Fra fylke, Til kommune, Til fylke, Antall, Fra lat, Fra lon, Til lat, Til lon`
4. Currently reads from `Python/Temp/` — switch back to `Data/01_Befolkning/Flytting` after re-running the data script

## Completed features
- [x] County-level (Fylke) as default view — aggregated flows between Telemark and other counties
- [x] Municipality-level (Kommune) view with Skien as default
- [x] Level toggle (Fylke / Kommune) in sidebar
- [x] County columns added to output CSVs
- [x] Clean flow arrows (Bézier curves, no polygon arrowheads)
- [x] Internal movement: 3-option radio (exclude / include / only internal Telemark)
- [x] Threshold slider for minimum count (kommune view only)
- [x] Distance-based flow clustering with adjustable slider (kommune view only)
  - Radius scales with distance from Telemark
  - Clustered nodes labeled "Bergen og omegn" etc.
- [x] 3-column layout: sidebar (280px) | map (flex) | right panel (760px)
- [x] Right panel: table, Sankey Tilflytting, Sankey Fraflytting — equal 1/3 heights
- [x] Sankey nodes colored with Telemark fylke palette (14 distinct colors)
- [x] Sankey drill-down in kommune view: click a link to see municipality breakdown, back button
- [x] Sankey tooltips: integer values + "personer", arrow format
- [x] Data labels on map markers (all labeled in fylke view; zoom-responsive in kommune view)
- [x] Data labels persist during zoom (fixed disappearing bug)
- [x] Bigger fonts (body 16px, controls 14px, Sankey 13px, header 24px)
- [x] No scroll — page fits 100vh
- [x] Taller header with centered title "Flyttestrømmer Telemark", left-aligned subtitle, logo from GitHub
- [x] Telemark fylkeskommune logo in header (from GitHub raw URL)
- [x] Stats box: Fraflytting number in red, netto green/red
- [x] More spacing between sidebar sections (gap 20px)
- [x] Direction gradient on flow lines (light at origin → opaque at destination)
- [x] Thicker lines for clustered flows (log2 boost)
- [x] Hover highlight on flow lines (+3 weight, full opacity)
- [x] No auto-zoom when adjusting cluster slider or threshold
- [x] Map initial zoom: fitBounds Norway (south to north)
- [x] Arrow (→) in all tooltips: lines, markers, clustered flows
- [x] Kommune view: "Begge retninger" removed, default Tilflytting
- [x] Kommune view: origin markers drawn (like fylke view)
- [x] Fylke view: marker tooltips say "Til Telemark" / "Fra Telemark"
- [x] Removed border layers (GeoJSON + WFS) per user request
- [x] Encoding fix: fylkesinndeling CSV read with utf-8-sig
- [x] Left menu: merged threshold+cluster into "Forenkle kartvisningen" section, renamed labels
- [x] Left menu: removed "(ekskl. intern flytting)" from fylke stats label
- [x] Threshold slider only affects map lines + table, not stats or Sankeys
- [x] Table filters by selected direction (tilflytting / fraflytting / both)
- [x] Sankey bands: hand cursor on hover
- [x] Sankey drill-down: back button restructured into flex header row (no longer absolute-positioned)
- [x] Sankey drill-down: back button handlers attached via JS addEventListener (not inline onclick)
- [x] Sankey drill-down: Plotly.purge before newPlot to prevent handler stacking
- [x] Map: fylke view uses setView([64.5, 13.0], 5) for tighter framing
- [x] Map: kommune view zoomed in to setView([59.2, 9.0], 8)
- [x] Invisible wider hit-area polyline on flow lines for easier tooltip access
- [x] Flow lines: smooth gradient with 30 segments, butt lineCap, overlapping points (no dots)
- [x] Data labels: geographic bias — labels far from Oslo get distance bonus
- [x] Data labels: major cities (Oslo, Bergen, Stavanger, etc.) always shown if in flows
- [x] "Kun intern flytting i Telemark" option in kommune view (radio group)
- [x] Radio labels renamed: "Uten intern flytting i fylket" / "Med intern flytting i fylket"
- [x] Cluster suffix: "m. fl." when internal-only mode, "og omegn" otherwise
- [x] "Kun intern" mode: auto-set threshold=1, cluster=0%, force all labels
- [x] Stats labels: dynamic "(uten intern flytting)" / "(med intern flytting)" / "(kun internt i Telemark)"
- [x] Kommune view: show only one Sankey (til or fra) based on direction selection
- [x] Sankey titles: "Flytting til/fra Telemark" (fylke) / "Flytting til/fra [kommune]" (kommune)
- [x] Sankey drilldown title: "Flytting til/fra [kommune] – [fylke]"
- [x] Sankey back button: "← Tilbake til fylkeoversikt"
- [x] Table titles: "Flyttestrømmer – Telemark" / "Flyttestrømmer – [kommune]"
- [x] Table: show all rows in kommune view (showAll parameter)
- [x] Table container: flex 1.4 for more vertical space
- [x] Header: taller (90px min-height, 32px font, 54px logo, 16px subtitle)
- [x] Map zoom: setTimeout wrapper for reliable setView after DOM/Plotly updates
- [x] Flow lines: origin dot markers (small colored circle at flow start)

## Pending / ideas
- [ ] Switch data_dir back to `Data/01_Befolkning/Flytting` after re-running data script
- [ ] Consider adding year selector if multi-year data becomes available
- [ ] Tune clustering slider default value after testing
- [ ] Consider removing clustering slider after finding optimal value
