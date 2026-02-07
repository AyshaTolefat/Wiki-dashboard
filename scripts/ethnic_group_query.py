from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ALLOWED_COUNTRIES_PATH = DATA_DIR / "allowed_countries_qids.csv"
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
    bd:serviceParam wikibase:language "en" .
  }}
}}
GROUP BY ?country ?countryLabel ?ethnicGroup ?ethnicGroupLabel ?genderCategory
ORDER BY DESC(?count)
"""

def run_sparql(query: str, retries: int = 3, sleep_between: int = 10) -> pd.DataFrame:
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

        except Exception:
            if attempt == retries:
                print("  giving up on this query")
                raise
            print("  retrying")
            time.sleep(sleep_between)

def load_allowed_countries() -> pd.DataFrame:
    if not ALLOWED_COUNTRIES_PATH.exists():
        raise SystemExit(f"Expected {ALLOWED_COUNTRIES_PATH} to exist")

    df = pd.read_csv(ALLOWED_COUNTRIES_PATH)

    def to_qid(val: str) -> str:
        if isinstance(val, str) and val.startswith("http"):
            return val.rsplit("/", 1)[-1]
        return str(val)

    df["country_qid"] = df["country"].apply(to_qid)
    return df[["country", "country_qid", "countryLabel"]]

def fetch_ethnic_group_by_country_gender_all() -> pd.DataFrame | None:
    countries_df = load_allowed_countries()
    total = len(countries_df)

    print("Querying Wikidata: ethnic group by country and gender")

    all_rows: list[pd.DataFrame] = []

    for idx, row in countries_df.iterrows():
        country_uri = row["country"]
        country_qid = row["country_qid"]
        label = row["countryLabel"]

        print(f"[{idx + 1}/{total}] {label} ({country_qid})")

        query = ETHNIC_GROUP_QUERY.format(country_qid=country_qid)

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

        # enforce allowed-list uri/label for consistency
        df_country["country"] = country_uri
        df_country["countryLabel"] = label

        cols = ["country", "countryLabel", "ethnicGroup", "ethnicGroupLabel", "genderCategory", "count"]
        df_country = df_country[cols]

        all_rows.append(df_country)
        time.sleep(1.2)

    if not all_rows:
        print("No data fetched for any country.")
        return None

    df_all = pd.concat(all_rows, ignore_index=True)
    df_all.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved to: {OUTPUT_PATH}")
    print("Preview:")
    print(df_all.head())

    return df_all

if __name__ == "__main__":
    fetch_ethnic_group_by_country_gender_all()
