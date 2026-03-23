# scripts/find_missing_globe_regions.py

import json
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# ✅ 1) Set this to the SAME geojson used in your globe
GEOJSON_PATH = DATA_DIR / "ne_50m_admin_0_countries.geojson"   # <-- change if your file is elsewhere

# ✅ 2) Set this to a CSV that represents your CURRENT "has data" countries/regions
# It must contain ISO-3 codes for the regions you currently cover.
# Common examples: allowed_countries_qids.csv (if it includes iso3), or any output dataset with country_iso3 column.
HAVE_DATA_CSV = DATA_DIR / "allowed_countries_iso3.csv"  # <-- change if needed

# ✅ 3) What column in HAVE_DATA_CSV contains ISO-3?
# If you're not sure, leave as None and the script will auto-detect common names.
ISO3_COL = None

# Output
OUTPUT_PATH = DATA_DIR / "missing_countries_on_globe.csv"


def norm_iso3(x: str) -> str:
    if pd.isna(x):
        return ""
    x = str(x).strip().upper()
    # some datasets contain 'None' or 'nan'
    if x in {"NAN", "NONE", "NULL", ""}:
        return ""
    return x


def pick_first(props: dict, keys: list[str]) -> str:
    """Return first non-empty property string among keys."""
    for k in keys:
        v = props.get(k)
        if v is None:
            continue
        v = str(v).strip()
        if v and v.lower() not in {"none", "null", "nan"}:
            return v
    return ""


def load_geo_features(geojson_path: Path) -> pd.DataFrame:
    with geojson_path.open("r", encoding="utf-8") as f:
        gj = json.load(f)

    feats = gj.get("features", [])
    rows = []

    for ft in feats:
        props = ft.get("properties", {}) or {}

        # Try multiple common ISO-3 keys used in Natural Earth / other geojsons
        iso3 = pick_first(props, ["ISO_A3", "ISO3", "ADM0_A3", "WB_A3", "SOV_A3", "GU_A3", "ISO_A3_EH"])
        iso3 = norm_iso3(iso3)

        # Some geojsons put name under different keys
        name = pick_first(props, ["NAME_LONG", "NAME_EN", "NAME", "ADMIN", "SOVEREIGNT", "FORMAL_EN", "GEOUNIT"])
        name = str(name).strip()

        rows.append(
            {
                "geo_iso3": iso3,
                "geo_name": name,
                # helpful for debugging
                "raw_iso3": pick_first(props, ["ISO_A3", "ISO3", "ADM0_A3", "WB_A3", "SOV_A3", "GU_A3", "ISO_A3_EH"]),
                "raw_name": name,
            }
        )

    df = pd.DataFrame(rows)

    # Filter out shapes that don't have a usable ISO3 (some datasets use "-99" for disputed/aggregates)
    df["geo_iso3"] = df["geo_iso3"].apply(norm_iso3)
    df = df[(df["geo_iso3"] != "") & (df["geo_iso3"] != "-99")].drop_duplicates(subset=["geo_iso3"])

    return df.sort_values("geo_iso3").reset_index(drop=True)


def load_have_data_iso3(have_data_csv: Path, iso3_col: str | None) -> set[str]:
    df = pd.read_csv(have_data_csv)

    if iso3_col is None:
        # Auto-detect common column names
        candidates = ["iso3", "ISO3", "iso_a3", "ISO_A3", "country_iso3", "COUNTRY_ISO3", "alpha_3", "ALPHA_3"]
        found = [c for c in candidates if c in df.columns]
        if not found:
            raise ValueError(
                f"Could not auto-detect an ISO-3 column in {have_data_csv}. "
                f"Columns found: {list(df.columns)}\n"
                f"Set ISO3_COL manually in the script to the correct column name."
            )
        iso3_col = found[0]

    iso3_set = set(df[iso3_col].apply(norm_iso3).tolist())
    iso3_set = {x for x in iso3_set if x and x != "-99"}
    return iso3_set


def main():
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(f"GEOJSON_PATH not found: {GEOJSON_PATH}")

    if not HAVE_DATA_CSV.exists():
        raise FileNotFoundError(f"HAVE_DATA_CSV not found: {HAVE_DATA_CSV}")

    geo_df = load_geo_features(GEOJSON_PATH)
    have_iso3 = load_have_data_iso3(HAVE_DATA_CSV, ISO3_COL)

    # "White regions" = in geojson but not in your current data coverage
    missing = geo_df[~geo_df["geo_iso3"].isin(have_iso3)].copy()

    # Save
    missing.rename(columns={"geo_iso3": "iso3", "geo_name": "name"}, inplace=True)
    missing = missing[["iso3", "name", "raw_iso3"]].sort_values(["name", "iso3"])
    missing.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

    print(f"GeoJSON regions: {len(geo_df)}")
    print(f"Have-data ISO3:  {len(have_iso3)}")
    print(f"Missing (white): {len(missing)}")
    print(f"Saved: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()