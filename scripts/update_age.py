# fetch_age_special_territories.py
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_PATH = DATA_DIR / "age_groups_by_country.csv"

SPECIAL_TERRITORIES = {
    "Q1246": "Kosovo",
    "Q23681": "Northern Cyprus",
    "Q34754": "Somaliland"
}

AGE_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?countryLabel ?ageGroup (COUNT(?person) AS ?count) WHERE {{
  VALUES ?country {{ wd:{qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 ?birthPlace .
  ?birthPlace wdt:P131* ?country .

  OPTIONAL {{ ?person wdt:P569 ?dob . }}
  OPTIONAL {{ ?person wdt:P570 ?dod . }}

  FILTER(BOUND(?dob))
  FILTER(?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime)
  FILTER(!BOUND(?dod))

  BIND(year(NOW()) - year(?dob) AS ?age)
  FILTER(?age >= 0 && ?age <= 120)

  BIND(
    IF(?age <= 14, "0-14 child",
      IF(?age <= 24, "15-24 youth",
        IF(?age <= 59, "25-59 adult",
          "60+ senior"
        )
      )
    ) AS ?ageGroup
  )

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
GROUP BY ?country ?countryLabel ?ageGroup
"""

def run(q):
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(q)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent","Wiki-dashboard/1.0")
    res = sparql.query().convert()
    return pd.DataFrame([{k:v["value"] for k,v in b.items()} for b in res["results"]["bindings"]])

def main():
    all_rows = []
    for qid,label in SPECIAL_TERRITORIES.items():
        print(label)
        df = run(AGE_QUERY.format(qid=qid))
        if not df.empty:
            df["count"] = df["count"].astype(int)
            df["country"] = f"http://www.wikidata.org/entity/{qid}"
            df["countryLabel"] = label
            all_rows.append(df[["country","countryLabel","ageGroup","count"]])
        time.sleep(1.2)

    if not all_rows:
        print("No data")
        return

    new_df = pd.concat(all_rows)

    if OUTPUT_PATH.exists():
        old = pd.read_csv(OUTPUT_PATH)
        combined = pd.concat([old,new_df])
        combined = combined.drop_duplicates(subset=["country","ageGroup"], keep="last")
    else:
        combined = new_df

    combined.to_csv(OUTPUT_PATH,index=False)
    print("Updated age file.")

if __name__ == "__main__":
    main()