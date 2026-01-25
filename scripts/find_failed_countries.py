import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

ALLOWED_PATH = DATA_DIR / "allowed_countries_qids.csv"
LANGUAGES_PATH = DATA_DIR / "languages_by_country.csv"
FAILED_PATH = DATA_DIR / "failed_countries_languages.csv"

def to_qid(val: str) -> str:
    if isinstance(val, str) and val.startswith("http"):
        return val.rsplit("/", 1)[-1]
    return str(val)

def main():
    if not ALLOWED_PATH.exists():
        raise SystemExit("allowed_countries_qids.csv not found")

    if not LANGUAGES_PATH.exists():
        raise SystemExit("languages_by_country.csv not found")

    allowed = pd.read_csv(ALLOWED_PATH)
    langs = pd.read_csv(LANGUAGES_PATH)

    allowed["country_qid"] = allowed["country"].apply(to_qid)
    langs["country_qid"] = langs["country"].apply(to_qid)

    succeeded_qids = set(langs["country_qid"].unique())

    failed = allowed[~allowed["country_qid"].isin(succeeded_qids)].copy()

    failed = failed[["country", "countryLabel", "country_qid"]]
    failed.to_csv(FAILED_PATH, index=False)

    print(f"Found {len(failed)} failed countries")
    print(f"Saved to {FAILED_PATH}")
    print("Preview:")
    print(failed.head())

if __name__ == "__main__":
    main()
