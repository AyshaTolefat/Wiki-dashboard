import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

ALLOWED_PATH = DATA_DIR / "allowed_countries_qids.csv"
OUTPUT_PATH = DATA_DIR / "ethnic_group_by_country_gender.csv"
FAILED_PATH = DATA_DIR / "failed_countries_ethnic_group.csv"


def to_qid(val) -> str:
    if isinstance(val, str) and val.startswith("http"):
        return val.rsplit("/", 1)[-1]
    return str(val)


def main():
    if not ALLOWED_PATH.exists():
        raise SystemExit(f"Missing: {ALLOWED_PATH}")

    if not OUTPUT_PATH.exists():
        raise SystemExit(f"Missing: {OUTPUT_PATH}")

    allowed = pd.read_csv(ALLOWED_PATH)
    out = pd.read_csv(OUTPUT_PATH)

    # allowed countries list uses full URIs in 'country'
    allowed["country_qid"] = allowed["country"].apply(to_qid)

    # output may store country as QID or URI; normalize
    succeeded_qids = set(out["country"].apply(to_qid).unique())

    failed = allowed[~allowed["country_qid"].isin(succeeded_qids)].copy()
    failed = failed[["country", "countryLabel", "country_qid"]].reset_index(drop=True)

    failed.to_csv(FAILED_PATH, index=False)

    print(f"Allowed countries: {len(allowed)}")
    print(f"Succeeded countries: {len(succeeded_qids)}")
    print(f"Failed countries: {len(failed)}")
    print(f"Saved: {FAILED_PATH}")
    if len(failed) > 0:
        print("Preview:")
        print(failed.head(10))


if __name__ == "__main__":
    main()
