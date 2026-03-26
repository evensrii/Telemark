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
- [x] Clean flow arrows (Bézier curves with small circle endpoints, no polygon arrowheads)
- [x] Internal movement checkbox (kommune view only)
- [x] Threshold slider for minimum count (kommune view only)
- [x] Distance-based flow clustering with adjustable slider (kommune view only)
  - Radius scales with distance from Telemark (higher resolution near, lower far)
  - Clustered nodes labeled "Bergen m.fl." etc.
- [x] 3-column layout: sidebar (280px) | map (flex) | right panel (380px)
- [x] Right panel: table (top) → Sankey Tilflytting → Sankey Fraflytting (stacked vertically)
- [x] Sankey nodes colored with Telemark fylke palette (14 distinct colors)
- [x] Data labels on map markers (all labeled in fylke view; zoom-responsive in kommune view)
- [x] Bigger fonts (body 14px, controls 13px, Sankey 12px)
- [x] Stats box with netto calculation
- [x] Direction filter (both / tilflytting / fraflytting)
- [x] Encoding fix: fylkesinndeling CSV read with utf-8-sig (Østfold etc.)
- [x] Removed border layers (GeoJSON + WFS) per user request

## Pending / ideas
- [ ] Switch data_dir back to `Data/01_Befolkning/Flytting` after re-running data script
- [ ] Consider adding year selector if multi-year data becomes available
- [ ] Possibly add kommune-level Sankey showing individual municipalities instead of county aggregation
- [ ] Tune clustering slider default value after testing
- [ ] Consider removing clustering slider after finding optimal value
