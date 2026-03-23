from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

MISSING_QIDS_PATH = DATA_DIR / "missing_iso3_to_qid.csv"
OUTPUT_PATH = DATA_DIR / "gender_country_1900_present_per_country.csv"

GENDER_COUNTRY_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?genderCategory (COUNT(?person) AS ?count) WHERE {{
  ?person wdt:P31 wd:Q5 .
  OPTIONAL {{ ?person wdt:P21 ?gender . }}
  ?person wdt:P19 / wdt:P17 ?country .
  VALUES ?country {{ wd:{country_qid} }}

  OPTIONAL {{ ?person wdt:P569 ?dob . }}
  FILTER( !BOUND(?dob) || ?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime )

  BIND(
    IF(
      !BOUND(?gender),
      "Unknown / not stated",
      IF(
        ?gender IN (wd:Q6581097, wd:Q2449503),
        "Male",
        IF(
          ?gender IN (wd:Q6581072, wd:Q1052281),
          "Female",
          "Non-binary or other"
        )
      )
    )
    AS ?genderCategory
  )
}}
GROUP BY ?genderCategory
"""

def run_sparql(query: str) -> pd.DataFrame:
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader(
        "User-Agent",
        "Wiki-dashboard/1.0 (https://github.com/AyshaTolefat)"
    )

    results = sparql.query().convert()
    rows = []
    for binding in results["results"]["bindings"]:
        row = {}
        for var, value_dict in binding.items():
            row[var] = value_dict.get("value")
        rows.append(row)

    return pd.DataFrame(rows)

def fetch_gender_for_country(country_qid: str, country_label: str) -> pd.DataFrame | None:
    query = GENDER_COUNTRY_QUERY.format(country_qid=country_qid)
    try:
        df = run_sparql(query)
    except Exception as e:
        print(f"⚠️ Error fetching {country_label} ({country_qid}): {e}")
        return None

    if df.empty:
        return None

    df["count"] = df["count"].astype(int)
    df["country"] = f"http://www.wikidata.org/entity/{country_qid}"
    df["countryLabel"] = country_label

    # Ensure exact column names match your existing CSV
    return df[["country", "countryLabel", "genderCategory", "count"]]

def merge_into_existing(existing_path: Path, new_df: pd.DataFrame) -> None:
    key_cols = ["country", "genderCategory"]

    if existing_path.exists():
        old = pd.read_csv(existing_path)

        # append and dedupe (keep latest)
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=key_cols, keep="last")
    else:
        combined = new_df.drop_duplicates(subset=key_cols, keep="last")

    combined.to_csv(existing_path, index=False, encoding="utf-8")
    print(f"✅ Appended missing-only data into {existing_path.name}")
    print(f"   Total rows now: {len(combined)}")

def main():
    if not MISSING_QIDS_PATH.exists():
        raise FileNotFoundError(f"Missing QID file not found: {MISSING_QIDS_PATH}")

    missing = pd.read_csv(MISSING_QIDS_PATH)

    if "qid" not in missing.columns:
        raise ValueError(f"'qid' column not found in {MISSING_QIDS_PATH}. Columns: {list(missing.columns)}")

    # Prefer best available label column
    label_col = None
    for c in ["wikidata_label", "name", "countryLabel"]:
        if c in missing.columns:
            label_col = c
            break

    if label_col is None:
        missing["wikidata_label"] = missing["qid"]
        label_col = "wikidata_label"

    # Clean
    missing["qid"] = missing["qid"].astype(str).str.strip()
    missing = missing[missing["qid"].str.match(r"^Q\d+$", na=False)].drop_duplicates(subset=["qid"]).reset_index(drop=True)

    total = len(missing)
    print(f"➡️ Fetching gender distribution for MISSING territories only: {total}")

    all_rows = []
    for idx, row in missing.iterrows():
        qid = row["qid"]
        label = str(row[label_col]) if pd.notna(row[label_col]) else qid

        print(f"[{idx+1}/{total}] {label} ({qid})")
        df_c = fetch_gender_for_country(qid, label)
        if df_c is not None:
            all_rows.append(df_c)

        time.sleep(1.2)

    if not all_rows:
        print("No new data fetched.")
        return

    new_df = pd.concat(all_rows, ignore_index=True)

    # Merge directly into your existing CSV
    merge_into_existing(OUTPUT_PATH, new_df)

if __name__ == "__main__":
    main()