import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

df = pd.read_csv(DATA_DIR / "gender_occupation_with_sectors.csv")
df["count"] = df["count"].astype(int)

other = df[df["sector"] == "Other / Unclassified"]
top = (
    other.groupby(["occupation_qid", "occupationLabel"], as_index=False)["count"]
    .sum()
    .sort_values("count", ascending=False)
    .head(100)
)

top.to_csv(DATA_DIR / "top_other_occupations.csv", index=False)
print("Wrote top_other_occupations.csv")
