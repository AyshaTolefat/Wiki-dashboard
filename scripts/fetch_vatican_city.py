from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
from pathlib import Path
import time

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MASTER = DATA_DIR / "gender_occupation_raw_long.csv"

COUNTRY_QID = "Q237"  # Vatican City

QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?countryLabel ?occupation ?occupationLabel ?genderCategory (COUNT(?person) AS ?count) WHERE {
  VALUES ?country { wd:Q237 }

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .
  ?person wdt:P106 ?occupation .

  OPTIONAL { ?person wdt:P21 ?gender . }
  OPTIONAL { ?person wdt:P569 ?dob . }
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
    ) AS ?genderCategory
  )

  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
GROUP BY ?country ?countryLabel ?occupation ?occupationLabel ?genderCategory
"""

def run_sparql(q: str) -> pd.DataFrame:
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(q)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent", "Wiki-dashboard/1.0 (https://github.com/AyshaTolefat)")
    res = sparql.query().convert()
    rows = []
    for b in res["results"]["bindings"]:
        rows.append({k: v.get("value") for k, v in b.items()})
    return pd.DataFrame(rows)

def main():
    df = run_sparql(QUERY)
    if df.empty:
        print("No rows returned for Q237.")
        return

    # match your master schema
    df_out = pd.DataFrame({
        "country_qid": ["Q237"] * len(df),
        "country": df["country"],
        "occupation_qid": df["occupation"].str.rsplit("/", n=1).str[-1],
        "occupation": df["occupation"],
        "genderCategory": df["genderCategory"],
        "count": df["count"].astype(int),
    })

    master = pd.read_csv(MASTER)
    combined = pd.concat([master, df_out], ignore_index=True)

    group_cols = ["country_qid", "country", "occupation_qid", "occupation", "genderCategory"]
    combined["count"] = combined["count"].astype(int)
    combined = combined.groupby(group_cols, as_index=False)["count"].sum()

    combined.to_csv(MASTER, index=False)
    print("Added Vatican City (Q237) rows to master.")
    print("Countries now:", combined["country_qid"].nunique())

if __name__ == "__main__":
    main()
