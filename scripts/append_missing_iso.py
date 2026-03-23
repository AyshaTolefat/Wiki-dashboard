import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

ALLOWED = DATA_DIR / "allowed_countries_iso3.csv"
MISSING = DATA_DIR / "missing_iso3_to_qid.csv"

allowed = pd.read_csv(ALLOWED)
missing = pd.read_csv(MISSING)

missing["country"] = "http://www.wikidata.org/entity/" + missing["qid"]
missing["countryLabel"] = missing["wikidata_label"]
missing["iso3"] = missing["iso3"].str.upper().str.strip()

new_rows = missing[["country", "countryLabel", "qid", "iso3"]]

# Remove iso3 already present
existing_iso3 = set(allowed["iso3"].astype(str).str.upper())
new_rows = new_rows[~new_rows["iso3"].isin(existing_iso3)]

updated = pd.concat([allowed, new_rows], ignore_index=True)

updated.to_csv(ALLOWED, index=False)

print(f"Added {len(new_rows)} new ISO3 entries.")