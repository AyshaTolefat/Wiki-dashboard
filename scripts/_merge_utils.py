from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

def load_missing_qids(path: Path | None = None) -> list[str]:
    """
    Load QIDs from missing_iso3_to_qid.csv
    """
    if path is None:
        path = DATA_DIR / "missing_iso3_to_qid.csv"

    df = pd.read_csv(path)
    qids = df["qid"].dropna().astype(str).str.strip()
    qids = [q for q in qids if q.startswith("Q")]
    return sorted(set(qids))


def merge_into_csv(existing_csv: Path, new_df: pd.DataFrame, key_cols: list[str]) -> None:
    """
    Append new_df into existing_csv and remove duplicates based on key_cols.
    Keeps newest rows.
    """
    if existing_csv.exists():
        old = pd.read_csv(existing_csv)
        combined = pd.concat([old, new_df], ignore_index=True)
    else:
        combined = new_df.copy()

    combined = combined.drop_duplicates(subset=key_cols, keep="last")
    combined.to_csv(existing_csv, index=False, encoding="utf-8")

    print(f"✅ Updated {existing_csv.name} — now {len(combined)} rows")