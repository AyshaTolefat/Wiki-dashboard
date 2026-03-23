# fetch_gender_special.py
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_PATH = DATA_DIR / "gender_country_1900_present_per_country.csv"

SPECIAL = {
    "Q1246":"Kosovo",
    "Q23681":"Northern Cyprus",
    "Q34754":"Somaliland"
}

QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?genderCategory (COUNT(?person) AS ?count) WHERE {{
  VALUES ?country {{ wd:{qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 ?birthPlace .
  ?birthPlace wdt:P131* ?country .

  OPTIONAL {{ ?person wdt:P21 ?gender . }}

  BIND(
    IF(!BOUND(?gender),"Unknown / not stated",
      IF(?gender IN (wd:Q6581097, wd:Q2449503),"Male",
        IF(?gender IN (wd:Q6581072, wd:Q1052281),"Female",
          "Non-binary or other"
        )
      )
    ) AS ?genderCategory
  )
}}
GROUP BY ?genderCategory
"""

def run(q):
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(q)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent","Wiki-dashboard/1.0")
    res = sparql.query().convert()
    return pd.DataFrame([{k:v["value"] for k,v in b.items()} for b in res["results"]["bindings"]])

def main():
    rows=[]
    for qid,label in SPECIAL.items():
        df=run(QUERY.format(qid=qid))
        if not df.empty:
            df["count"]=df["count"].astype(int)
            df["country"]=f"http://www.wikidata.org/entity/{qid}"
            df["countryLabel"]=label
            rows.append(df[["country","countryLabel","genderCategory","count"]])
        time.sleep(1.2)

    new=pd.concat(rows)

    if OUTPUT_PATH.exists():
        old=pd.read_csv(OUTPUT_PATH)
        combined=pd.concat([old,new])
        combined=combined.drop_duplicates(subset=["country","genderCategory"],keep="last")
    else:
        combined=new

    combined.to_csv(OUTPUT_PATH,index=False)
    print("Updated gender totals.")

if __name__=="__main__":
    main()