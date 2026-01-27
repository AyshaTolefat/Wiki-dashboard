from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ALLOWED_COUNTRIES_PATH = DATA_DIR / "allowed_countries_qids.csv"
OUTPUT_PATH = DATA_DIR / "jobs_gender_by_country.csv"

JOBS_GENDER_COUNTRY_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>

SELECT ?country ?countryLabel ?occupation ?occupationLabel ?genderCategory (COUNT(?person) AS ?count) WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .                    
  ?person wdt:P19 / wdt:P17 ?country .        

  OPTIONAL {{ ?person wdt:P21 ?gender . }}   
  OPTIONAL {{ ?person wdt:P569 ?dob . }}      
  OPTIONAL {{ ?person wdt:P106 ?occupation . }} 

  FILTER(BOUND(?occupation))                  

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

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
GROUP BY ?country ?countryLabel ?occupation ?occupationLabel ?genderCategory
ORDER BY ?countryLabel ?occupationLabel ?genderCategory
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
        except Exception as e:
            if attempt == retries:
                print("giving up")
                raise
            else:
                print("Error")
                time.sleep(sleep_between)

def load_allowed_countries() -> pd.DataFrame:
    df = pd.read_csv(ALLOWED_COUNTRIES_PATH)
    def to_qid(val: str) -> str:
        if isinstance(val, str) and val.startswith("http"):
            return val.rsplit("/", 1)[-1]
        return str(val)
    df["country_qid"] = df["country"].apply(to_qid)
    return df[["country_qid", "countryLabel"]]

def fetch_jobs_gender_by_country() -> pd.DataFrame:
    print("Fetching jobs + gender distribution by country")
    countries_df = load_allowed_countries()
    all_rows = []
    total = len(countries_df)
    for idx, row in countries_df.iterrows():
        qid = row["country_qid"]
        label = row["countryLabel"]
        print(f"[{idx+1}/{total}] {label} ({qid})")
        query = JOBS_GENDER_COUNTRY_QUERY.format(country_qid=qid)
        try:
            df_chunk = run_sparql(query)
        except Exception:
            print("error")
            continue
        if df_chunk.empty:
            print("no results for this country")
            continue
        if "count" in df_chunk.columns:
            df_chunk["count"] = df_chunk["count"].astype(int)
        all_rows.append(df_chunk)
        time.sleep(1.0)
    if not all_rows:
        print("No data fetched at all.")
        return pd.DataFrame()
    df_all = pd.concat(all_rows, ignore_index=True)
    cols = ["country", "countryLabel", "occupation", "occupationLabel", "genderCategory", "count"]
    df_all = df_all[cols]
    df_all.to_csv(OUTPUT_PATH, index=False)
    print("Saved jobs x gender x country data")
    print("Preview:")
    print(df_all.head())
    return df_all

if __name__ == "__main__":
    fetch_jobs_gender_by_country()