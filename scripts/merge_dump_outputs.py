import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

MASTER = DATA_DIR / "gender_occupation_raw_long.csv"
DUMP_DIR = DATA_DIR / "dump_outputs"

dfs = []
if MASTER.exists():
    dfs.append(pd.read_csv(MASTER))

dump_files = sorted(DUMP_DIR.glob("gender_occ_Q*.csv"))
for f in dump_files:
    dfs.append(pd.read_csv(f))

df = pd.concat(dfs, ignore_index=True)

# Ensure count is numeric
df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)

# Sum duplicates (important because dump extraction flushed multiple times)
group_cols = ["country_qid", "country", "occupation_qid", "occupation", "genderCategory"]
df = df.groupby(group_cols, as_index=False)["count"].sum()

df.to_csv(MASTER, index=False)

print("Merged master:", MASTER)
print("Dump files merged:", len(dump_files))
print("Rows:", len(df))
print("Countries:", df["country_qid"].nunique())
