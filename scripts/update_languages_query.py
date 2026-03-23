from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MISSING_QIDS_PATH = DATA_DIR / "missing_iso3_to_qid.csv"
OUTPUT_PATH = DATA_DIR / "languages_by_country.csv"

LANGUAGES_COUNTRY_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?countryLabel ?language ?languageLabel ?type (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .

  OPTIONAL {{ ?person wdt:P569 ?dob . }}
  FILTER( !BOUND(?dob) || ?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime )

  {{
    ?person wdt:P103 ?language .
    BIND("native" AS ?type)
  }}
  UNION
  {{
    ?person wdt:P1412 ?language .
    BIND("spoken" AS ?type)
  }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
GROUP BY ?country ?countryLabel ?language ?languageLabel ?type
ORDER BY ?country ?type DESC(?count)
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

def fetch_languages_for_country(country_qid: str, country_label: str) -> pd.DataFrame | None:
    query = LANGUAGES_COUNTRY_QUERY.format(country_qid=country_qid)

    try:
        df = run_sparql(query)
    except Exception as e:
        print(f"  ERROR fetching languages for {country_label} ({country_qid}): {e}")
        return None

    if df.empty:
        return None

    df["count"] = df["count"].astype(int)

    # enforce consistent URI + label
    df["country"] = f"http://www.wikidata.org/entity/{country_qid}"
    df["countryLabel"] = country_label

    cols = ["country", "countryLabel", "language", "languageLabel", "type", "count"]
    return df[cols]

def merge_into_existing(existing_path: Path, new_df: pd.DataFrame) -> None:
    # Unique “cell” is country + language + type (native/spoken)
    key_cols = ["country", "language", "type"]

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

    # ✅ correct regex (Q followed by digits)
    missing["qid"] = missing["qid"].astype(str).str.strip()
    missing = missing[missing["qid"].str.match(r"^Q\d+$", na=False)].drop_duplicates(subset=["qid"]).reset_index(drop=True)

    total = len(missing)
    print(f"➡️ Fetching LANGUAGES for MISSING territories only: {total}")

    all_rows = []
    for idx, row in missing.iterrows():
        qid = row["qid"]
        label = str(row[label_col]) if pd.notna(row[label_col]) else qid

        print(f"[{idx + 1}/{total}] {label} ({qid})")
        df_c = fetch_languages_for_country(qid, label)
        if df_c is not None:
            all_rows.append(df_c)
        else:
            print("  no results / error")

        time.sleep(1.0)

    if not all_rows:
        print("No data fetched for any missing territory.")
        return

    new_df = pd.concat(all_rows, ignore_index=True)
    merge_into_existing(OUTPUT_PATH, new_df)
    print("Done.")

if __name__ == "__main__":
    main()