import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

RAW = DATA_DIR / "gender_occupation_raw_long.csv"
ALLOWED = DATA_DIR / "allowed_countries_qids.csv"

raw = pd.read_csv(RAW, usecols=["country_qid"])
allowed = pd.read_csv(ALLOWED)

allowed["country_qid"] = allowed["country"].astype(str).str.rsplit("/", n=1).str[-1]

raw_c = set(raw["country_qid"].astype(str).unique())
allowed_c = set(allowed["country_qid"].astype(str).unique())
missing = sorted(allowed_c - raw_c)

print("Countries expected:", len(allowed_c))
print("Countries present: ", len(raw_c))
print("Countries missing: ", len(missing))
if missing:
    print("Missing QIDs (first 50):", missing[:50])
