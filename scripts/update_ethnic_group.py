# fetch_ethnic_groups_special_territories.py
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

OUTPUT_PATH = DATA_DIR / "ethnic_group_by_country_gender.csv"

SPECIAL_TERRITORIES = {
    "Q1246": "Kosovo",
    "Q23681": "Northern Cyprus",
    "Q34754": "Somaliland",
}

ETHNIC_GROUP_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?countryLabel ?ethnicGroup ?ethnicGroupLabel ?genderCategory (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 ?birthPlace .
  ?birthPlace wdt:P131* ?country .

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
            sparql.addCustomHttpHeader("User-Agent", "Wiki-dashboard/1.0 (special territories)")
            results = sparql.query().convert()

            rows = []
            for binding in results["results"]["bindings"]:
                rows.append({var: value_dict.get("value") for var, value_dict in binding.items()})
            return pd.DataFrame(rows)

        except Exception as e:
            if attempt == retries:
                print(f"  giving up: {e}")
                raise
            print("  retrying...")
            time.sleep(sleep_between)

def merge_into_existing(existing_path: Path, new_df: pd.DataFrame) -> None:
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
    all_rows = []

    for idx, (qid, label) in enumerate(SPECIAL_TERRITORIES.items(), start=1):
        print(f"[{idx}/{len(SPECIAL_TERRITORIES)}] {label} ({qid})")

        query = ETHNIC_GROUP_QUERY.format(country_qid=qid)
        df_country = run_sparql(query)

        if df_country.empty:
            print("  no results")
            time.sleep(1.0)
            continue

        df_country["count"] = df_country["count"].astype(int)
        df_country["country"] = f"http://www.wikidata.org/entity/{qid}"
        df_country["countryLabel"] = label

        cols = ["country", "countryLabel", "ethnicGroup", "ethnicGroupLabel", "genderCategory", "count"]
        df_country = df_country[cols]

        all_rows.append(df_country)
        time.sleep(1.2)

    if not all_rows:
        print("No data fetched for any special territory.")
        return

    new_df = pd.concat(all_rows, ignore_index=True)
    merge_into_existing(OUTPUT_PATH, new_df)
    print("Done.")

if __name__ == "__main__":
    main()