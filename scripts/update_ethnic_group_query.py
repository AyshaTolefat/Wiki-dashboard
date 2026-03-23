from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MISSING_QIDS_PATH = DATA_DIR / "missing_iso3_to_qid.csv"
OUTPUT_PATH = DATA_DIR / "ethnic_group_by_country_gender.csv"

ETHNIC_GROUP_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?countryLabel ?ethnicGroup ?ethnicGroupLabel ?genderCategory (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .

  OPTIONAL {{ ?person wdt:P21 ?gender . }}
  OPTIONAL {{ ?person wdt:P569 ?dob . }}

  FILTER( !BOUND(?dob) || ?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime )

  ?person wdt:P172 ?ethnicGroup .

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

  SERVICE wikibase:label {{
      bd:serviceParam wikibase:language "en,[AUTO_LANGUAGE],mul,fr,de,es,zh,ru,ja" .
  }}
}}
GROUP BY ?country ?countryLabel ?ethnicGroup ?ethnicGroupLabel ?genderCategory
ORDER BY DESC(?count)
"""

def run_sparql(query: str, retries: int = 3, sleep_between: float = 10.0) -> pd.DataFrame:
    for attempt in range(1, retries + 1):
        try:
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

        except Exception as e:
            if attempt == retries:
                print(f"  giving up: {e}")
                raise
            print("  retrying...")
            time.sleep(sleep_between)

def merge_into_existing(existing_path: Path, new_df: pd.DataFrame) -> None:
    # One row per unique combination of these keys
    key_cols = ["country", "ethnicGroup", "genderCategory"]

    if existing_path.exists():
        old = pd.read_csv(existing_path)
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=key_cols, keep="last")
    else:
        combined = new_df.drop_duplicates(subset=key_cols, keep="last")

    combined.to_csv(existing_path, index=False, encoding="utf-8")
    print(f"✅ Updated {existing_path.name}: {len(combined)} rows total")

def main():
    if not MISSING_QIDS_PATH.exists():
        raise FileNotFoundError(f"Missing QID file not found: {MISSING_QIDS_PATH}")

    missing = pd.read_csv(MISSING_QIDS_PATH)
    if "qid" not in missing.columns:
        raise ValueError(f"'qid' column not found in {MISSING_QIDS_PATH}. Columns: {list(missing.columns)}")

    # choose best label
    label_col = None
    for c in ["wikidata_label", "name", "countryLabel"]:
        if c in missing.columns:
            label_col = c
            break
    if label_col is None:
        missing["wikidata_label"] = missing["qid"]
        label_col = "wikidata_label"

    # clean qids
    missing["qid"] = missing["qid"].astype(str).str.strip()
    missing = missing[missing["qid"].str.match(r"^Q\d+$", na=False)].drop_duplicates(subset=["qid"]).reset_index(drop=True)

    total = len(missing)
    print(f"➡️ Fetching ETHNIC GROUP counts for MISSING territories only: {total}")

    all_rows = []

    for idx, row in missing.iterrows():
        qid = row["qid"]
        label = str(row[label_col]) if pd.notna(row[label_col]) else qid

        print(f"[{idx + 1}/{total}] {label} ({qid})")

        query = ETHNIC_GROUP_QUERY.format(country_qid=qid)

        try:
            df_country = run_sparql(query)
        except Exception:
            print("  error / timeout")
            time.sleep(2.0)
            continue

        if df_country.empty:
            print("  no results")
            time.sleep(1.0)
            continue

        # types
        if "count" in df_country.columns:
            df_country["count"] = df_country["count"].astype(int)

        # enforce consistent country uri/label (like your old script does)
        df_country["country"] = f"http://www.wikidata.org/entity/{qid}"
        df_country["countryLabel"] = label

        cols = ["country", "countryLabel", "ethnicGroup", "ethnicGroupLabel", "genderCategory", "count"]
        df_country = df_country[cols]

        all_rows.append(df_country)
        time.sleep(1.2)

    if not all_rows:
        print("No data fetched for any missing territory.")
        return

    new_df = pd.concat(all_rows, ignore_index=True)

    # Merge into existing full CSV
    merge_into_existing(OUTPUT_PATH, new_df)
    print("Done.")

if __name__ == "__main__":
    main()