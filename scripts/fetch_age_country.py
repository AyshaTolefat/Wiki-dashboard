from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ALLOWED_COUNTRIES_PATH = DATA_DIR / "allowed_countries_qids.csv"
OUTPUT_PATH = DATA_DIR / "age_groups_by_country.csv"

AGE_COUNTRY_QUERY ="""
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?countryLabel ?ageGroup (COUNT(?person) AS ?count) WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .               
  ?person wdt:P19 / wdt:P17 ?country .    

  OPTIONAL {{ ?person wdt:P569 ?dob . }}  
  OPTIONAL {{ ?person wdt:P570 ?dod . }}  

  FILTER(BOUND(?dob))

  FILTER(?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime)

  FILTER(!BOUND(?dod))

  BIND(year(NOW()) AS ?currentYear)
  BIND(xsd:integer(?currentYear - year(?dob)) AS ?age)

  FILTER(?age >= 0 && ?age <= 120)

  BIND(
    IF(?age <= 12, "0-12 child",
      IF(?age <= 20, "13-20 teen",
        IF(?age <= 59, "21-59 adult",
          "60+ senior"
        )
      )
    ) AS ?ageGroup
  )

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
GROUP BY ?country ?countryLabel ?ageGroup
ORDER BY ?country ?ageGroup
"""

def run_sparql(query: str, retries: int=3, sleep_between: int=10) -> pd.DataFrame:
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
                print("  giving up on this query")
                raise
            else:
                print(f"  retrying")
                time.sleep(sleep_between)

def load_allowed_countries() -> pd.DataFrame:
    df = pd.read_csv(ALLOWED_COUNTRIES_PATH)

    def to_qid(val: str) -> str:
        if isinstance(val, str) and val.startswith("http"):
            return val.rsplit("/", 1)[-1]
        return str(val)

    df["country_qid"] = df["country"].apply(to_qid)
    return df[["country_qid", "countryLabel"]]

def fetch_age_for_country(country_qid: str, country_label: str) -> pd.DataFrame | None:
    query = AGE_COUNTRY_QUERY.format(country_qid=country_qid)
    try:
        df = run_sparql(query)
    except Exception as e:
        print(f"  ERROR")
        return None
    if df.empty:
        return None
    if "count" in df.columns:
        df["count"] = df["count"].astype(int)
    return df[["country", "countryLabel", "ageGroup", "count"]]

def fetch_age_by_country_all() -> pd.DataFrame | None:
    countries_df = load_allowed_countries()
    total = len(countries_df)
    print(f"Querying Wikidata")

    all_rows: list[pd.DataFrame] = []

    for idx, row in countries_df.iterrows():
        qid = row["country_qid"]
        label = row["countryLabel"]
        print(f"[{idx + 1}/{total}] {label} ({qid})")
        df_country = fetch_age_for_country(qid, label)
        if df_country is not None:
            all_rows.append(df_country)
        time.sleep(1.0)

    if not all_rows:
        print("No data fetched for any country.")
        return None

    df_all = pd.concat(all_rows, ignore_index=True)

    df_all.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved age-group-by-country data")
    print("Preview:")
    print(df_all.head())

    return df_all


if __name__ == "__main__":
    fetch_age_by_country_all()