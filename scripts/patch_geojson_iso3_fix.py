import json
from pathlib import Path

INP = Path("data/ne_50m_admin_0_countries.geojson")
OUT = Path("data/ne_50m_admin_0_countries_patched.geojson")

# Force-correct a few known problem areas by name
NAME_TO_ISO3 = {
    "South Sudan": "SSD",
    "Djibouti": "DJI",
    "Western Sahara": "ESH",
}

geo = json.loads(INP.read_text(encoding="utf-8"))

for f in geo["features"]:
    props = f.get("properties", {})
    name = props.get("NAME") or props.get("ADMIN") or props.get("NAME_LONG") or ""

    # Default: try to use existing 3-letter codes if present
    iso_a3 = str(props.get("ISO_A3", "")).strip()
    adm0_a3 = str(props.get("ADM0_A3", "")).strip()

    iso3_fix = ""
    for cand in [iso_a3, adm0_a3]:
        if len(cand) == 3 and cand.isalpha() and cand != "ZZZ":
            iso3_fix = cand.upper()
            break

    # Override by name for the known problematic ones
    if name in NAME_TO_ISO3:
        iso3_fix = NAME_TO_ISO3[name]

    if iso3_fix:
        props["ISO3_FIX"] = iso3_fix
        f["properties"] = props

OUT.write_text(json.dumps(geo), encoding="utf-8")
print(f"Wrote: {OUT}")