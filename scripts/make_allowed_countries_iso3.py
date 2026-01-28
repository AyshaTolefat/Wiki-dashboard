import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

IN_PATH = DATA_DIR / "allowed_countries_qids.csv"
OUT_PATH = DATA_DIR / "allowed_countries_iso3.csv"


def to_qid(val) -> str:
    if isinstance(val, str) and val.startswith("http"):
        return val.rsplit("/", 1)[-1]
    return str(val)


# Direct hard-coded ISO3 overrides for tricky names
DIRECT_ISO3 = {
    "Cape Verde": "CPV",
    "Democratic Republic of the Congo": "COD",
    "Turkey": "TUR",
}

# Name fixes: Wikidata labels -> pycountry official names (used for lookups)
NAME_FIXES = {
    "United States": "United States of America",
    "Russia": "Russian Federation",
    "Czech Republic": "Czechia",
    "Ivory Coast": "Côte d'Ivoire",
    "The Gambia": "Gambia",
    "The Bahamas": "Bahamas",
    "South Korea": "Korea, Republic of",
    "North Korea": "Korea, Democratic People's Republic of",
    "Syria": "Syrian Arab Republic",
    "Tanzania": "Tanzania, United Republic of",
    "Vatican City": "Holy See (Vatican City State)",
    "Bolivia": "Bolivia, Plurinational State of",
    "Venezuela": "Venezuela, Bolivarian Republic of",
    "Iran": "Iran, Islamic Republic of",
    "Laos": "Lao People's Democratic Republic",
    "Moldova": "Moldova, Republic of",
    "Palestine": "Palestine, State of",
    "People's Republic of China": "China",
}


def main():
    try:
        import pycountry
    except ImportError:
        raise SystemExit("pycountry not installed. Run: pip install pycountry")

    if not IN_PATH.exists():
        raise SystemExit(f"Missing: {IN_PATH}")

    df = pd.read_csv(IN_PATH)

    required_cols = {"country", "countryLabel"}
    if not required_cols.issubset(df.columns):
        raise SystemExit(
            f"Expected columns {sorted(required_cols)} in {IN_PATH.name}, got: {list(df.columns)}"
        )

    df["qid"] = df["country"].apply(to_qid)

    def label_to_iso3(label: str):
        if not isinstance(label, str) or not label.strip():
            return None

        # 1) deterministic overrides first
        if label in DIRECT_ISO3:
            return DIRECT_ISO3[label]

        # 2) apply name fixes then lookup in pycountry
        name = NAME_FIXES.get(label, label)

        # direct lookup
        c = pycountry.countries.get(name=name)
        if c:
            return c.alpha_3

        # fuzzy search fallback
        try:
            res = pycountry.countries.search_fuzzy(name)
            if res:
                return res[0].alpha_3
        except Exception:
            pass

        return None

    df["iso3"] = df["countryLabel"].apply(label_to_iso3)

    missing = df[df["iso3"].isna()][["countryLabel", "qid"]].reset_index(drop=True)

    print(f"Total countries: {len(df)}")
    print(f"Missing ISO3: {len(missing)}")
    if len(missing) > 0:
        print("Examples missing (first 25):")
        print(missing.head(25).to_string(index=False))

    out = df[["country", "countryLabel", "qid", "iso3"]]
    out.to_csv(OUT_PATH, index=False)
    print(f"Saved: {OUT_PATH}")


if __name__ == "__main__":
    main()