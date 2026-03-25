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
- [x] County borders from Kartverket WFS API (dashed lines, shown in fylke view)
- [x] Telemark kommune borders from local GeoJSON (shown in kommune view)
- [x] Internal movement checkbox (kommune view only)
- [x] Threshold slider for minimum count (kommune view only)
- [x] Sankey diagrams with improved proportions (thicker nodes, tighter layout)
- [x] Top results table (right panel, shows top 15 in/out connections, color-coded)
- [x] Stats box with netto calculation
- [x] Direction filter (both / tilflytting / fraflytting)
- [x] Encoding fix: fylkesinndeling CSV read with utf-8-sig (Østfold etc.)

## Pending / ideas
- [ ] Switch data_dir back to `Data/01_Befolkning/Flytting` after re-running data script
- [ ] Consider adding year selector if multi-year data becomes available
- [ ] Possibly add kommune-level Sankey showing individual municipalities instead of county aggregation
- [ ] Test county border loading — depends on Kartverket WFS availability
- [ ] Potential: highlight Telemark polygon with a subtle fill color
