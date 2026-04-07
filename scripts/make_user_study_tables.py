from pathlib import Path
import pandas as pd

# =========================
# FIND PROJECT ROOT SAFELY
# =========================

CURRENT_FILE = Path(__file__).resolve()

PROJECT_ROOT = CURRENT_FILE
while not ((PROJECT_ROOT / "data").exists() and (PROJECT_ROOT / "dashboard").exists()):
    if PROJECT_ROOT.parent == PROJECT_ROOT:
        raise FileNotFoundError("Could not find project root containing both 'data' and 'dashboard'.")
    PROJECT_ROOT = PROJECT_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = PROJECT_ROOT / "user_study_tables"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DECADES_PATH      = DATA_DIR / "gender_decades_by_country.csv"
GENDER_TOTAL_PATH = DATA_DIR / "gender_country_1900_present_per_country.csv"
LANG_PATH         = DATA_DIR / "languages_by_country.csv"
ETHNIC_PATH       = DATA_DIR / "ethnic_group_by_country_gender.csv"
OCC_PATH          = DATA_DIR / "gender_occupation_with_isco_refined.csv"
AGE_PATH          = DATA_DIR / "age_groups_by_country.csv"

TOP_N_LANGUAGES_PER_COUNTRY = 10
TOP_N_ETHNIC_GROUPS_PER_COUNTRY = 12
TOP_N_OCCUPATION_CATEGORIES_PER_COUNTRY = 12
MIN_DECADE = 1900

QID_TO_COUNTRY = {
    "Q148": "China",
    "Q30": "United States",
    "Q145": "United Kingdom",
    "Q155": "Brazil",
    "Q16": "Canada",
    "Q668": "India",
}


def save(df: pd.DataFrame, filename: str) -> None:
    path = OUT_DIR / filename
    df.to_csv(path, index=False)
    print(f"Saved {path} ({len(df)} rows)")


def top_n_per_group(df: pd.DataFrame, group_col: str, value_col: str, n: int) -> pd.DataFrame:
    return (
        df.sort_values([group_col, value_col], ascending=[True, False])
          .groupby(group_col, group_keys=False)
          .head(n)
          .reset_index(drop=True)
    )


def build_gender_overall():
    df = pd.read_csv(GENDER_TOTAL_PATH)

    df = df[["countryLabel", "genderCategory", "count"]].copy()
    df.columns = ["Country", "Gender", "Count"]

    df["Gender"] = df["Gender"].replace({
        "Unknown / not stated": "Unknown"
    })

    df = df.sort_values(["Country", "Count"], ascending=[True, False]).reset_index(drop=True)
    save(df, "study_gender_overall_all_countries.csv")


def build_gender_over_decades():
    df = pd.read_csv(DECADES_PATH)

    df = df[["countryLabel", "decade", "genderCategory", "count"]].copy()
    df.columns = ["Country", "Decade", "Gender", "Count"]

    df["Gender"] = df["Gender"].replace({
        "Unknown / not stated": "Unknown"
    })

    if MIN_DECADE is not None:
        df = df[df["Decade"] >= MIN_DECADE].copy()

    df = df.sort_values(["Country", "Decade", "Gender"]).reset_index(drop=True)
    save(df, "study_gender_over_decades_all_countries.csv")


def build_age_groups():
    df = pd.read_csv(AGE_PATH)

    df = df[["countryLabel", "ageGroup", "count"]].copy()
    df.columns = ["Country", "Age Group", "Count"]

    age_order = {
        "0-12 child": 0,
        "0-14 child": 0,
        "13-20 teen": 1,
        "15-24 youth": 1,
        "21-59 adult": 2,
        "25-59 adult": 2,
        "60+ senior": 3,
    }
    df["_age_order"] = df["Age Group"].map(age_order).fillna(999)

    df = (
        df.sort_values(["Country", "_age_order", "Count"], ascending=[True, True, False])
          .drop(columns="_age_order")
          .reset_index(drop=True)
    )

    save(df, "study_age_groups_all_countries.csv")


def build_languages():
    df = pd.read_csv(LANG_PATH)

    df = df[["countryLabel", "languageLabel", "type", "count"]].copy()
    df.columns = ["Country", "Language", "Type", "Count"]

    df = df[df["Type"].str.lower() == "spoken"].copy()

    df = (
        df.groupby(["Country", "Language"], as_index=False)["Count"]
          .sum()
    )

    df = top_n_per_group(df, "Country", "Count", TOP_N_LANGUAGES_PER_COUNTRY)
    save(df, "study_languages_spoken_all_countries.csv")


def build_ethnic_groups():
    df = pd.read_csv(ETHNIC_PATH)

    df = df[["countryLabel", "ethnicGroupLabel", "genderCategory", "count"]].copy()
    df.columns = ["Country", "Ethnic Group", "Gender", "Count"]

    df["Gender"] = df["Gender"].replace({
        "Unknown / not stated": "Unknown"
    })

    df = (
        df.groupby(["Country", "Ethnic Group", "Gender"], as_index=False)["Count"]
          .sum()
    )

    totals = (
        df.groupby(["Country", "Ethnic Group"], as_index=False)["Count"]
          .sum()
    )

    top_groups = top_n_per_group(totals, "Country", "Count", TOP_N_ETHNIC_GROUPS_PER_COUNTRY)

    df = df.merge(
        top_groups[["Country", "Ethnic Group"]],
        on=["Country", "Ethnic Group"],
        how="inner"
    )

    df = df.sort_values(["Country", "Ethnic Group", "Gender"]).reset_index(drop=True)
    save(df, "study_ethnic_groups_all_countries.csv")


def build_occupation_categories():
    df = pd.read_csv(OCC_PATH)

    if "countryLabel" in df.columns:
        df["Country"] = df["countryLabel"]
    elif "country_qid" in df.columns:
        df["Country"] = df["country_qid"].astype(str).map(QID_TO_COUNTRY)
    elif "country" in df.columns:
        extracted = df["country"].astype(str).str.extract(r"(Q\d+)", expand=False)
        df["Country"] = extracted.map(QID_TO_COUNTRY)
    else:
        raise ValueError("Could not find usable country column in occupation file.")

    df = df[df["Country"].notna()].copy()

    if "sector" not in df.columns:
        raise ValueError("Occupation file must contain 'sector'.")

    df = df[["Country", "sector", "count"]].copy()
    df.columns = ["Country", "Occupation Category", "Count"]

    df = (
        df.groupby(["Country", "Occupation Category"], as_index=False)["Count"]
          .sum()
    )

    df = top_n_per_group(df, "Country", "Count", TOP_N_OCCUPATION_CATEGORIES_PER_COUNTRY)
    save(df, "study_occupation_categories_all_countries.csv")


def main():
    print("Project root:", PROJECT_ROOT)
    print("Data dir:", DATA_DIR)
    print("Output dir:", OUT_DIR)

    build_gender_overall()
    build_gender_over_decades()
    build_age_groups()
    build_languages()
    build_ethnic_groups()
    build_occupation_categories()

    print("All study-ready tables generated successfully.")


if __name__ == "__main__":
    main()