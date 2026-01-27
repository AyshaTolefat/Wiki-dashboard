import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

ALLOWED = DATA_DIR / "allowed_countries_qids.csv"
RAW = DATA_DIR / "gender_occupation_raw_long.csv"
OUT = DATA_DIR / "missing_countries_qids.txt"

allowed = pd.read_csv(ALLOWED)
allowed["country_qid"] = allowed["country"].astype(str).str.rsplit("/", n=1).str[-1]

raw = pd.read_csv(RAW, usecols=["country_qid"])
present = set(raw["country_qid"].astype(str).unique())

missing = sorted(set(allowed["country_qid"]) - present)

OUT.write_text("\n".join(missing) + "\n", encoding="utf-8")

print("Countries expected:", len(set(allowed["country_qid"])))
print("Countries present: ", len(present))
print("Countries missing: ", len(missing))
print("Wrote missing list to:", OUT)
