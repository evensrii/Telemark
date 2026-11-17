import re
import uuid

## Optional extras from the line chart rules (Formatering/formatering_line.txt), asked for in the "Comment" column of
## the figure overview. Used by create_graphs_from_list.py and create_static_graphs.py.


def extras_from_comment(comment, ns):
    """Optional extras from the line chart rules, asked for in the comment."""
    visualize = {}
    m = re.search(r"[Hh]ighlight between (\d{4}) and (\d{4}), named \"([^\"]+)\"", comment)
    if m:
        start, end, label = m.groups()
        y_top = ns.get("y_max", "")
        visualize["range-annotations"] = [{
            "id": uuid.uuid4().hex[:10], "type": "x", "display": "range", "color": "#888", "opacity": 10,
            "strokeType": "solid", "strokeWidth": 1, "pattern": {"gap": 3, "type": "solid", "width": 3},
            "position": {"x0": f"{start}/01/01 00:00", "x1": f"{end}/01/01 00:00", "y0": "0", "y1": "0", "yAxis": "y"},
            "showMobile": True, "showDesktop": True,
        }]
        visualize["text-annotations"] = [{
            "id": uuid.uuid4().hex[:10], "text": label, "align": "tl", "size": 14, "bold": False, "italic": False,
            "underline": False, "bg": False, "dx": 0, "dy": 0,
            "position": {"x": f"{start}/01/01 14:00", "y": str(y_top)},
            "connectorLine": {"enabled": False}, "showMobile": True, "showDesktop": True, "mobileFallback": True,
        }]
        print(f"Highlight {start}-{end} '{label}' (text at y={y_top})")
    if re.search(r"[Cc]olou?ring below each line", comment):
        df, color_map = ns["df"], dict(ns.get("color_map", {}))
        series = [s for s in df.columns[1:]]
        # Series with only scattered single values (no two values in a row) are shown as dots, not lines
        point_series = [s for s in series
                        if not (df[s].notna() & df[s].shift(1).notna()).any() and df[s].notna().any()]
        line_series = [s for s in series if s not in point_series]
        for p in point_series:  # dots take the color of the line they belong to (a shared word, e.g. "Mdir")
            words = set(re.findall(r"\w+", p))
            owner = next((s for s in line_series if words & set(re.findall(r"\w+", s))), None)
            if owner:
                color_map[p] = color_map[owner]
        visualize["color-category"] = {"map": color_map}
        visualize["custom-area-fills"] = [{
            "id": uuid.uuid4().hex[:10], "from": name, "to": "--zero-baseline--", "color": color_map[name],
            "opacity": 0.3, "colorNegative": "#F76454", "interpolation": "linear", "useMixedColors": False,
        } for name in line_series]
        # Area look: no dots or value labels on the lines, series names in a legend; dot series only as dots
        visualize["lines"] = {
            name: {"symbols": {"enabled": name in point_series, "on": "every", "size": 6},
                   "valueLabels": {"enabled": False},
                   "colorKey": name in line_series,
                   "directLabel": False}
            for name in series
        }
        print(f"Area fill below {line_series}; dots only: {point_series}")
    return visualize

