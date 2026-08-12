import os
import re
import pandas as pd
import pdfplumber
from pathlib import Path

from Helper_scripts.github_functions import handle_output_data

# Script metadata
script_name = os.path.basename(__file__)
error_messages = []

# %% Setup paths
base_path = Path(r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark")
data_folder = base_path / "Data" / "Boligbehovsanalyse_2026" / "Bolyst_og_attraktivitet" / "Sykepleierindeksen"
pdf_folder = data_folder / "pdfer"
pdf_files = sorted(pdf_folder.glob("*_sykepleierindeksen.pdf"))

print(f"Found {len(pdf_files)} PDFs:")
for f in pdf_files:
    print(f"  {f.name}")

# %% Helper functions

def get_period(filepath):
    """Extract period like '2025_1' from filename."""
    match = re.match(r'(\d{4}_\d)', filepath.stem)
    return match.group(1) if match else filepath.stem


def find_page(pdf, pattern, start_page=0, end_page=None):
    """Find first page index containing text matching regex pattern."""
    if end_page is None:
        end_page = len(pdf.pages)
    for i in range(start_page, min(end_page, len(pdf.pages))):
        text = pdf.pages[i].extract_text() or ""
        if re.search(pattern, text, re.IGNORECASE):
            return i
    return None


def group_words_by_line(words, y_tol=5):
    """Group extracted words into lines by y-coordinate proximity."""
    if not words:
        return []
    sorted_w = sorted(words, key=lambda w: (w['top'], w['x0']))
    lines = []
    current = [sorted_w[0]]
    for w in sorted_w[1:]:
        if abs(w['top'] - current[0]['top']) <= y_tol:
            current.append(w)
        else:
            lines.append(sorted(current, key=lambda x: x['x0']))
            current = [w]
    lines.append(sorted(current, key=lambda x: x['x0']))
    return lines


def line_text(words):
    """Join words in a line into a single string."""
    return ' '.join(w['text'] for w in words)


# Known Telemark municipality names for page detection
TELEMARK_MUNIS = ['Bamble', 'Porsgrunn', 'Skien', 'Notodden', 'Kragerø', 'Midt-Telemark', 'Nome', 'Distriktene']

# Known area names used across the PDF charts (for cleaning rotated text)
KNOWN_AREAS = [
    "Porsgrunn/Skien", "Fredrikstad/Sarpsborg", "Ålesund m/omegn", "Ålesund m/Sula",
    "Hamar m/Stange", "Kristiansand m/omegn", "Kristiansand", "Drammen m/omegn",
    "Tønsberg m/Færder", "Bodø m/Fauske", "Trondheim", "Romerike", "Bergen",
    "Stavanger m/omegn", "Tromsø", "Follo", "Asker/Bærum", "Oslo", "Norge",
    "Bamble", "Distr. i Telemark", "Kragerø", "Midt-Telemark og Nome",
    "Notodden", "Porsgrunn", "Skien",
]

# Region headers that mark chart sections on multi-chart pages
REGION_HEADERS = ['Telemark', 'Troms', 'Trøndelag', 'Nordland', 'Vestland', 'Rogaland', 'Agder']

def match_known_area(raw_name):
    """Match a raw extracted name (possibly with wrong spaces) to a known area name."""
    raw_stripped = raw_name.replace(' ', '').lower()
    for known in KNOWN_AREAS:
        if known.replace(' ', '').lower() == raw_stripped:
            return known
    return raw_name  # Return as-is if no match

# %% Diagnostic: inspect pages of a single PDF (change index to inspect different PDFs)

def dump_pages(pdf_index=0):
    """Print first line of each page for a PDF to help identify page contents."""
    path = pdf_files[pdf_index]
    print(f"Inspecting: {path.name}")
    with pdfplumber.open(path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            first_lines = '\n    '.join(text.split('\n')[:3])
            print(f"  Page {i}: {first_lines[:120]}")

# Uncomment to run:
# dump_pages(0)

# %% 1. Extract "Sykepleierindeksen - utvalgte områder" (horizontal bar chart)

def extract_alle_omrader(pdf, period):
    """
    Finds the page with title 'Sykepleierindeksen YYYY - utvalgte områder'.
    Extracts area names and percentage values from the horizontal bar chart.
    """
    page_idx = find_page(pdf, r'Sykepleierindeksen\s+\d{4}.*utvalgte\s+områder')
    if page_idx is None:
        print(f"  [alle_omrader] Page not found")
        return []

    page = pdf.pages[page_idx]
    words = page.extract_words()
    lines = group_words_by_line(words)

    results = []
    for lw in lines:
        text = line_text(lw)
        # Match lines containing a percentage like "42,5 %" or "42,5%"
        match = re.search(r'(\d+[.,]\d+)\s*%', text)
        if match:
            pct = float(match.group(1).replace(',', '.'))
            name = text[:match.start()].strip()
            # Remove trailing dots or whitespace artifacts
            name = re.sub(r'[\s\.]+$', '', name).strip()
            if name and pct > 0:
                results.append({'Periode': period, 'Område': name, 'Andel': pct})

    print(f"  [alle_omrader] Page {page_idx}: {len(results)} areas found")
    return results


# %% 2. Extract "Telemark" bar chart (vertical bar chart, last pages)

def find_telemark_page(pdf):
    """Find the Telemark bar chart page by looking for known municipality names."""
    n_pages = len(pdf.pages)
    best_page = None
    best_count = 0
    for i in range(max(0, n_pages - 15), n_pages):
        text = pdf.pages[i].extract_text() or ""
        count = sum(1 for m in TELEMARK_MUNIS if m in text)
        if count > best_count:
            best_count = count
            best_page = i
    return best_page if best_count >= 3 else None


def extract_telemark(pdf, period):
    """
    Finds the Telemark bar chart page (which may also contain Troms/Trøndelag charts).
    Limits extraction to ONLY the Telemark section by detecting chart title boundaries.
    """
    page_idx = find_telemark_page(pdf)
    if page_idx is None:
        print(f"  [telemark] Page not found (no page with >= 3 Telemark municipalities)")
        return []

    page = pdf.pages[page_idx]
    words = page.extract_words()

    # --- Step 1: Find section boundaries ---
    # The page may have multiple charts (Telemark, Troms, Trøndelag)
    # Each chart has a bold title. Find all region headers and their y-positions.
    headers = []
    for w in words:
        txt = w['text'].strip()
        if txt in REGION_HEADERS:
            headers.append({'name': txt, 'top': w['top']})

    headers.sort(key=lambda h: h['top'])

    telemark_y = None
    next_chart_y = float('inf')
    for i, h in enumerate(headers):
        if h['name'] == 'Telemark':
            telemark_y = h['top']
            if i + 1 < len(headers):
                next_chart_y = headers[i + 1]['top']
            break

    if telemark_y is None:
        print(f"  [telemark] 'Telemark' header not found on page {page_idx}")
        return []

    print(f"  [telemark] Page {page_idx}: Telemark section y={telemark_y:.0f} to {next_chart_y if next_chart_y == float('inf') else f'{next_chart_y:.0f}'}")

    # --- Step 2: Filter words to Telemark section only ---
    section_words = [w for w in words if telemark_y <= w['top'] < next_chart_y]

    # --- Step 3: Extract percentage values ---
    num_words = []
    for w in section_words:
        txt = w['text'].replace('%', '').replace(',', '.').strip()
        if re.match(r'^\d+\.?\d*$', txt):
            num_words.append({
                'cx': (w['x0'] + w['x1']) / 2,
                'top': w['top'],
                'val': float(txt),
            })

    if not num_words:
        print(f"  [telemark] No numbers found in Telemark section")
        return []

    # Cluster by x to find Y-axis labels (leftmost cluster with >= 5 members)
    num_sorted = sorted(num_words, key=lambda w: w['cx'])
    x_clusters = []
    current_cluster = [num_sorted[0]]
    for w in num_sorted[1:]:
        if abs(w['cx'] - current_cluster[0]['cx']) < 20:
            current_cluster.append(w)
        else:
            x_clusters.append(current_cluster)
            current_cluster = [w]
    x_clusters.append(current_cluster)

    axis_cx = None
    for c in x_clusters:
        if len(c) >= 5:
            axis_cx = c[0]['cx']
            break

    if axis_cx is not None:
        data_labels = [w for w in num_words if abs(w['cx'] - axis_cx) > 20]
    else:
        data_labels = num_words

    data_labels = sorted(data_labels, key=lambda w: w['cx'])

    # --- Step 4: Extract area names below the bars ---
    if data_labels:
        data_y_min = min(w['top'] for w in data_labels)
    else:
        data_y_min = telemark_y

    skip_texts = {'%', 'Telemark'}
    skip_texts.update(f'{i}%' for i in range(0, 110, 10))
    skip_texts.update(f'{i} %' for i in range(0, 110, 10))

    name_words = [w for w in section_words
                  if w['top'] > data_y_min + 20
                  and not re.match(r'^\d+\s*%?$', w['text'].strip())
                  and w['text'].strip() not in skip_texts
                  and len(w['text'].strip()) > 0]

    # Group by x-proximity for multi-line names (e.g., "Distr. i" + "Telemark")
    name_words_sorted = sorted(name_words, key=lambda w: ((w['x0'] + w['x1']) / 2, w['top']))
    name_groups = []
    if name_words_sorted:
        current_group = [name_words_sorted[0]]
        for w in name_words_sorted[1:]:
            cur_cx = (current_group[0]['x0'] + current_group[0]['x1']) / 2
            w_cx = (w['x0'] + w['x1']) / 2
            if abs(w_cx - cur_cx) < 40:
                current_group.append(w)
            else:
                name_groups.append(current_group)
                current_group = [w]
        name_groups.append(current_group)

    names_with_x = []
    for group in name_groups:
        group_sorted = sorted(group, key=lambda w: w['top'])
        raw_name = ' '.join(w['text'] for w in group_sorted)
        center_x = sum((w['x0'] + w['x1']) / 2 for w in group) / len(group)
        clean_name = match_known_area(raw_name)
        names_with_x.append({'x': center_x, 'name': clean_name})

    names_sorted = sorted(names_with_x, key=lambda n: n['x'])

    # --- Step 5: Match values to names ---
    results = []
    if len(data_labels) == len(names_sorted):
        for val, nm in zip(data_labels, names_sorted):
            results.append({'Periode': period, 'Område': nm['name'], 'Andel': val['val']})
    else:
        for val in data_labels:
            closest = min(names_sorted, key=lambda n: abs(n['x'] - val['cx']), default=None)
            if closest:
                results.append({'Periode': period, 'Område': closest['name'], 'Andel': val['val']})

    print(f"  [telemark] {len(data_labels)} values, {len(names_sorted)} names -> {len(results)} rows")
    for r in results:
        print(f"    {r['Område']}: {r['Andel']}%")
    return results


# %% 3. Extract "Nødvendig inntekt" (vertical bar chart)

def extract_nodvendig_inntekt(pdf, period):
    """
    Finds page with 'Nødvendig inntekt for å kjøpe bolig'.
    Extracts area names (rotated text) and income values (above bars).
    Handles: split numbers (e.g. "390" + "000"), Y-axis filtering, rotated labels.
    """
    page_idx = find_page(pdf, r'Nødvendig inntekt')
    if page_idx is None:
        print(f"  [nødvendig_inntekt] Page not found")
        return []

    page = pdf.pages[page_idx]
    words = page.extract_words()

    # --- Step 1: Extract income values ---
    # Numbers like "390 000" may be split into two words: "390" and "000"
    # Collect all numeric words
    num_words = []
    for w in words:
        txt = w['text'].replace(' ', '').strip()
        if re.match(r'^\d+$', txt):
            num_words.append({
                'text': txt, 'x0': w['x0'], 'x1': w['x1'],
                'top': w['top'], 'cx': (w['x0'] + w['x1']) / 2
            })

    # Sort by position (top then left) for pairing adjacent numbers
    num_words_sorted = sorted(num_words, key=lambda w: (w['top'], w['x0']))

    # Try to form 6-digit values from adjacent 3-digit number pairs
    values_with_pos = []
    used = set()
    for i, w in enumerate(num_words_sorted):
        if i in used:
            continue
        txt = w['text']

        # Already a 6-digit number?
        if len(txt) >= 6 and txt.isdigit():
            val = int(txt)
            if 100000 <= val <= 2000000:
                values_with_pos.append({'cx': w['cx'], 'top': w['top'], 'val': val})
                used.add(i)
                continue

        # Try pairing with next word (e.g., "390" + "000")
        if len(txt) == 3 and txt.isdigit() and i + 1 < len(num_words_sorted):
            next_w = num_words_sorted[i + 1]
            next_txt = next_w['text']
            if (len(next_txt) == 3 and next_txt.isdigit()
                and abs(next_w['top'] - w['top']) < 8  # Same line
                and next_w['x0'] - w['x1'] < 25):     # Close horizontally
                val = int(txt + next_txt)
                if 100000 <= val <= 2000000:
                    cx = (w['x0'] + next_w['x1']) / 2
                    values_with_pos.append({'cx': cx, 'top': w['top'], 'val': val})
                    used.add(i)
                    used.add(i + 1)

    if not values_with_pos:
        print(f"  [nødvendig_inntekt] Page {page_idx}: no income values found")
        return []

    # --- Step 2: Filter out Y-axis labels ---
    # Y-axis labels are: (a) at leftmost x-cluster AND (b) exact multiples of 100 000
    # This ensures data labels near the axis (like 390 000) are NOT filtered out
    vals_sorted = sorted(values_with_pos, key=lambda v: v['cx'])
    x_clusters = []
    current_cluster = [vals_sorted[0]]
    for v in vals_sorted[1:]:
        if abs(v['cx'] - current_cluster[0]['cx']) < 30:
            current_cluster.append(v)
        else:
            x_clusters.append(current_cluster)
            current_cluster = [v]
    x_clusters.append(current_cluster)

    # Axis cluster: leftmost with >= 5 members
    axis_cx = None
    for c in x_clusters:
        if len(c) >= 5:
            axis_cx = c[0]['cx']
            break

    # Filter: must be BOTH near the axis x AND an exact multiple of 100000
    if axis_cx is not None:
        data_values = [v for v in values_with_pos
                       if not (abs(v['cx'] - axis_cx) < 40 and v['val'] % 100000 == 0)]
    else:
        data_values = values_with_pos

    data_values = sorted(data_values, key=lambda v: v['cx'])
    print(f"  [nødvendig_inntekt] Page {page_idx}: {len(values_with_pos)} total numbers, axis at x={axis_cx if axis_cx is None else f'{axis_cx:.0f}'}, {len(data_values)} data values")

    # --- Step 3: Extract area names from rotated characters ---
    chars = page.chars
    rotated_chars = [c for c in chars if not c.get('upright', True) and c['text'].strip()]

    names_with_x = []
    if rotated_chars:
        # Group rotated chars by x-proximity (each bar label has similar x0)
        rotated_sorted = sorted(rotated_chars, key=lambda c: c['x0'])
        groups = []
        current_group = [rotated_sorted[0]]
        for c in rotated_sorted[1:]:
            if abs(c['x0'] - current_group[0]['x0']) < 12:
                current_group.append(c)
            else:
                if len(current_group) >= 3:  # Min chars for a name
                    groups.append(current_group)
                current_group = [c]
        if len(current_group) >= 3:
            groups.append(current_group)

        for group in groups:
            # Sort by 'top' DESCENDING for correct reading order (rotated 90° CW)
            group_sorted = sorted(group, key=lambda c: c['top'], reverse=True)

            # Concatenate all chars (no space detection - use known-name matching instead)
            raw_name = ''.join(c['text'] for c in group_sorted).strip()
            center_x = sum(c['x0'] for c in group) / len(group)

            # Skip if it looks like a number or axis label
            if raw_name and len(raw_name) > 2 and not re.match(r'^[\d\s]+$', raw_name):
                # Match against known area names to fix spacing
                clean_name = match_known_area(raw_name)
                names_with_x.append({'x': center_x, 'name': clean_name})

    names_sorted = sorted(names_with_x, key=lambda n: n['x'])

    # --- Step 4: Match values to names ---
    results = []
    if len(data_values) == len(names_sorted):
        for val, nm in zip(data_values, names_sorted):
            results.append({'Periode': period, 'Område': nm['name'], 'Nødvendig inntekt': val['val']})
    elif names_sorted:
        # Fallback: match by closest x
        for val in data_values:
            closest = min(names_sorted, key=lambda n: abs(n['x'] - val['cx']), default=None)
            if closest:
                results.append({'Periode': period, 'Område': closest['name'], 'Nødvendig inntekt': val['val']})
    else:
        # No names found - just output values without names for debugging
        print(f"  [nødvendig_inntekt] WARNING: No rotated names found ({len(rotated_chars)} rotated chars)")
        for val in data_values:
            results.append({'Periode': period, 'Område': f'unknown_x{val["cx"]:.0f}', 'Nødvendig inntekt': val['val']})

    print(f"  [nødvendig_inntekt] {len(data_values)} values, {len(names_sorted)} names -> {len(results)} rows")
    for r in results:
        print(f"    {r['Område']}: {r['Nødvendig inntekt']}")
    return results


# %% 4. Extract "Sykepleierindeksen historisk" (table)

def extract_historisk(pdf, period):
    """
    Finds pages with 'Sykepleierindeksen historisk'.
    The table may span 2 pages (first part + 'forts.' continuation).
    Uses text-based parsing with strict year header detection.
    """
    # Find relevant pages
    page_indices = []
    for i in range(len(pdf.pages)):
        text = pdf.pages[i].extract_text() or ""
        if re.search(r'Sykepleierindeksen\s+historisk', text, re.IGNORECASE):
            page_indices.append(i)

    if not page_indices:
        print(f"  [historisk] Pages not found")
        return None

    print(f"  [historisk] Found on pages: {page_indices}")

    all_years = []  # Ordered list of all years found across pages
    all_data = {}   # {area_name: {year: value}}

    for page_idx in page_indices:
        page = pdf.pages[page_idx]
        text = page.extract_text() or ""
        lines = text.split('\n')

        page_years = []  # Years specific to this page's header

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Detect year header line: must be predominantly 4-digit years (2005-2030)
            tokens = line.split()
            year_tokens = [t for t in tokens if re.match(r'^20[0-3]\d$', t)]

            # Accept as header if: >= 3 years AND years are > 50% of tokens
            if len(year_tokens) >= 3 and len(year_tokens) >= len(tokens) * 0.5:
                page_years = year_tokens
                # Add to all_years (preserving order, no duplicates)
                for y in page_years:
                    if y not in all_years:
                        all_years.append(y)
                print(f"    Page {page_idx} year header: {page_years}")
                continue

            # Skip lines until we've found a year header on this page
            if not page_years:
                continue

            # Data line: area name followed by percentage values
            # Name contains letters, spaces, /, -, :
            # Values are like "3,2 %" or "47,8%" or "3,2"
            match = re.match(r'^([A-Za-zÆØÅæøå/\s\.\-:]+?)\s+(\d+[.,]\d.*)$', line)
            if not match:
                continue

            name = match.group(1).strip()
            rest = match.group(2)

            if not name or len(name) < 2:
                continue

            # Extract all decimal values from the rest of the line
            values = re.findall(r'(\d+[.,]\d+)', rest)

            if len(values) < 3:
                continue

            # Store values mapped to this page's years
            if name not in all_data:
                all_data[name] = {}

            for i, val_str in enumerate(values):
                if i < len(page_years):
                    year = page_years[i]
                    val = float(val_str.replace(',', '.'))
                    all_data[name][year] = val

    if not all_years or not all_data:
        print(f"  [historisk] Could not parse (years={len(all_years)}, areas={len(all_data)})")
        for pi in page_indices:
            text = pdf.pages[pi].extract_text() or ""
            print(f"  Page {pi} first 500 chars:")
            print(f"    {text[:500]}")
        return None

    # Sort years numerically
    all_years = sorted(all_years, key=int)

    # Build DataFrame
    rows = []
    for name, year_values in all_data.items():
        row = {'Område': name}
        for year in all_years:
            row[year] = year_values.get(year)
        rows.append(row)

    df_hist = pd.DataFrame(rows)

    # Drop year columns that are entirely empty (from pages where header was found but no data parsed)
    year_cols = [c for c in df_hist.columns if c != 'Område']
    empty_cols = [c for c in year_cols if df_hist[c].isna().all()]
    if empty_cols:
        df_hist = df_hist.drop(columns=empty_cols)
        print(f"  [historisk] Dropped empty year columns: {empty_cols}")

    remaining_years = [c for c in df_hist.columns if c != 'Område']
    print(f"  [historisk] {len(page_indices)} pages, {len(df_hist)} areas, years: {remaining_years[0]}-{remaining_years[-1]}")
    for r in rows[:3]:
        vals = [f"{r.get(y, '-')}" for y in all_years[:4]]
        print(f"    {r['Område']}: {', '.join(vals)}...")
    return df_hist


# %% Process all PDFs

all_alle_omrader = []
all_telemark = []
all_nodvendig_inntekt = []

for pdf_path in pdf_files:
    period = get_period(pdf_path)
    print(f"\nProcessing: {pdf_path.name} (period: {period})")

    with pdfplumber.open(pdf_path) as pdf:
        # 1. Alle områder
        rows = extract_alle_omrader(pdf, period)
        all_alle_omrader.extend(rows)

        # 2. Telemark
        rows = extract_telemark(pdf, period)
        all_telemark.extend(rows)

        # 3. Nødvendig inntekt
        rows = extract_nodvendig_inntekt(pdf, period)
        all_nodvendig_inntekt.extend(rows)

# 4. Historisk - only from the most recent (last) PDF
latest_pdf_path = pdf_files[-1]
latest_period = get_period(latest_pdf_path)
print(f"\nExtracting historisk from latest PDF: {latest_pdf_path.name} (period: {latest_period})")

with pdfplumber.open(latest_pdf_path) as pdf:
    latest_historisk = extract_historisk(pdf, latest_period)

# %% Build DataFrames

df_alle = pd.DataFrame(all_alle_omrader)
df_telemark = pd.DataFrame(all_telemark)
df_inntekt = pd.DataFrame(all_nodvendig_inntekt)

print(f"\n--- Results ---")
print(f"Alle områder: {len(df_alle)} rows across {df_alle['Periode'].nunique() if not df_alle.empty else 0} periods")
print(f"Telemark: {len(df_telemark)} rows across {df_telemark['Periode'].nunique() if not df_telemark.empty else 0} periods")
print(f"Nødvendig inntekt: {len(df_inntekt)} rows across {df_inntekt['Periode'].nunique() if not df_inntekt.empty else 0} periods")
print(f"Historisk: {latest_historisk.shape if latest_historisk is not None else 'None'} (from {latest_period})")

# %% Preview results

if not df_alle.empty:
    print("\n--- Alle områder (sample) ---")
    print(df_alle.head(10).to_string(index=False))

if not df_telemark.empty:
    print("\n--- Telemark (sample) ---")
    print(df_telemark.head(10).to_string(index=False))

if not df_inntekt.empty:
    print("\n--- Nødvendig inntekt (sample) ---")
    print(df_inntekt.head(10).to_string(index=False))

if latest_historisk is not None:
    print("\n--- Historisk (sample) ---")
    print(latest_historisk.head(5).to_string(index=False))

# %% Compare and upload to GitHub

github_folder = "Data/Boligbehovsanalyse_2026/Bolyst_og_attraktivitet/Sykepleierindeksen"
temp_folder = os.environ.get("TEMP_FOLDER")

# 1. Alle områder
if not df_alle.empty:
    is_new_data = handle_output_data(
        df_alle, 
        "sykepleierindeksen_alle_områder.csv", 
        github_folder, 
        temp_folder, 
        keepcsv=True
    )
    if is_new_data:
        print("New data detected: sykepleierindeksen_alle_områder.csv")
    else:
        print("No new data: sykepleierindeksen_alle_områder.csv")

# 2. Telemark
if not df_telemark.empty:
    is_new_data = handle_output_data(
        df_telemark, 
        "sykepleierindeks_telemark.csv", 
        github_folder, 
        temp_folder, 
        keepcsv=True
    )
    if is_new_data:
        print("New data detected: sykepleierindeks_telemark.csv")
    else:
        print("No new data: sykepleierindeks_telemark.csv")

# 3. Nødvendig inntekt
if not df_inntekt.empty:
    is_new_data = handle_output_data(
        df_inntekt, 
        "nødvendig_inntekt.csv", 
        github_folder, 
        temp_folder, 
        keepcsv=True
    )
    if is_new_data:
        print("New data detected: nødvendig_inntekt.csv")
    else:
        print("No new data: nødvendig_inntekt.csv")

# 4. Historisk
if latest_historisk is not None:
    is_new_data = handle_output_data(
        latest_historisk, 
        "sykepleierindeksen_historisk_alle_områder.csv", 
        github_folder, 
        temp_folder, 
        keepcsv=True
    )
    if is_new_data:
        print("New data detected: sykepleierindeksen_historisk_alle_områder.csv")
    else:
        print("No new data: sykepleierindeksen_historisk_alle_områder.csv")

print("\nDone!")
