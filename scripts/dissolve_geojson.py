import json
from pathlib import Path

from shapely.geometry import shape, mapping
from shapely.ops import unary_union

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

INP = DATA_DIR / "ne_50m_admin_0_countries_patched.geojson"
OUT = DATA_DIR / "ne_50m_admin_0_countries_DISSOLVED.geojson"


def norm(x: str) -> str:
    return str(x or "").strip().lower()


def get_iso3_fix(props: dict) -> str | None:
    v = props.get("ISO3_FIX") or props.get("ISO_A3") or props.get("ADM0_A3")
    if not v:
        return None
    v = str(v).strip().upper()
    if v in ("", "NONE", "NULL", "-99"):
        return None
    return v


def is_somaliland(props: dict) -> bool:
    # Natural Earth sometimes uses different fields; check a few common ones.
    fields = [
        props.get("ADMIN"),
        props.get("NAME"),
        props.get("NAME_LONG"),
        props.get("NAME_EN"),
        props.get("FORMAL_EN"),
        props.get("SOVEREIGNT"),
        props.get("GEOUNIT"),
    ]
    text = " | ".join(norm(f) for f in fields)
    return "somaliland" in text


def main():
    if not INP.exists():
        raise SystemExit(f"Missing input geojson: {INP}")

    geo = json.loads(INP.read_text(encoding="utf-8"))
    feats = geo.get("features", [])

    # 1) Build groups by ISO3_FIX, but force Somaliland -> SOM
    groups: dict[str, list] = {}
    props_keep: dict[str, dict] = {}

    for f in feats:
        props = f.get("properties", {}) or {}

        iso3 = get_iso3_fix(props)

        # Force Somaliland to Somalia (SOM)
        if is_somaliland(props):
            iso3 = "SOM"
            props["ISO3_FIX"] = "SOM"
            props["ISO_A3"] = "SOM"
            props["ADM0_A3"] = "SOM"

        if not iso3:
            # keep as-is (unkeyed); these won’t be drawn by your choropleth anyway
            continue

        geom = f.get("geometry")
        if not geom:
            continue

        groups.setdefault(iso3, []).append(shape(geom))
        props_keep.setdefault(iso3, props)

    # 2) Dissolve each ISO3 into a single geometry
    out_features = []
    for iso3, geoms in groups.items():
        merged_geom = unary_union(geoms)

        out_features.append(
            {
                "type": "Feature",
                "properties": {**props_keep[iso3], "ISO3_FIX": iso3},
                "geometry": mapping(merged_geom),
            }
        )

    out_geo = {"type": "FeatureCollection", "features": out_features}
    OUT.write_text(json.dumps(out_geo), encoding="utf-8")

    print(f"✅ Wrote dissolved geojson: {OUT}")
    print(f"   Features before: {len(feats)}")
    print(f"   Features after:  {len(out_features)}")


if __name__ == "__main__":
    main()