# fetch_occupations_special_territories.py
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

OUT_REFINED = DATA_DIR / "gender_occupation_with_isco_refined.csv"

SPECIAL_TERRITORIES = {
    "Q1246": "Kosovo",
    "Q23681": "Northern Cyprus",
    "Q34754": "Somaliland",
}

OCC_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?occupation ?occupationLabel ?genderCategory (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 ?birthPlace .
  ?birthPlace wdt:P131* ?country .
  ?person wdt:P106 ?occupation .

  OPTIONAL {{ ?person wdt:P21 ?gender . }}
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

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
GROUP BY ?occupation ?occupationLabel ?genderCategory
ORDER BY DESC(?count)
"""

def run_sparql(query: str, retries: int = 3, sleep_between: float = 10.0) -> pd.DataFrame:
    for attempt in range(1, retries + 1):
        try:
            sparql = SPARQLWrapper(ENDPOINT)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            sparql.addCustomHttpHeader("User-Agent", "Wiki-dashboard/1.0 (special territories)")
            res = sparql.query().convert()
            rows = [{k: v.get("value") for k, v in b.items()} for b in res["results"]["bindings"]]
            return pd.DataFrame(rows)
        except Exception as e:
            if attempt == retries:
                print(f"  giving up: {e}")
                raise
            print("  retrying...")
            time.sleep(sleep_between)

def qid_from_uri(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]

def ensure_refined_schema(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "country_qid","country",
        "occupation_qid","occupation",
        "genderCategory","count",
        "occupationLabel",
        "sector","sector_source",
        "isco_major_code","isco_major_title",
        "isco_sub_major_code","isco_sub_major_title",
        "isco_mapping_method","isco_mapping_notes"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols]

def main():
    new_rows = []

    for idx, (country_qid, country_label) in enumerate(SPECIAL_TERRITORIES.items(), start=1):
        print(f"[{idx}/{len(SPECIAL_TERRITORIES)}] {country_label} ({country_qid})")

        q = OCC_QUERY.format(country_qid=country_qid)
        df = run_sparql(q)

        if df.empty:
            print("  no results")
            time.sleep(1.0)
            continue

        df["count"] = df["count"].astype(int)

        for _, row in df.iterrows():
            occ_uri = row["occupation"]
            occ_qid = qid_from_uri(occ_uri)

            new_rows.append({
                "country_qid": country_qid,
                "country": f"http://www.wikidata.org/entity/{country_qid}",
                "occupation_qid": occ_qid,
                "occupation": occ_uri,
                "genderCategory": row["genderCategory"],
                "count": int(row["count"]),
                "occupationLabel": row.get("occupationLabel", ""),

                # defaults so dashboard works immediately
                "sector": "Other / Unclassified",
                "sector_source": "special_territory_default",
                "isco_major_code": "",
                "isco_major_title": "",
                "isco_sub_major_code": "",
                "isco_sub_major_title": "",
                "isco_mapping_method": "unmapped",
                "isco_mapping_notes": "Added via special-territory WDQS update; run mapping scripts to classify",
            })

        time.sleep(1.2)

    if not new_rows:
        print("No occupation rows fetched for special territories.")
        return

    new_df = pd.DataFrame(new_rows)
    new_df = ensure_refined_schema(new_df)

    if OUT_REFINED.exists():
        old = pd.read_csv(OUT_REFINED)
        old = ensure_refined_schema(old)
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["country_qid", "occupation_qid", "genderCategory"],
            keep="last"
        )
    else:
        combined = new_df.drop_duplicates(
            subset=["country_qid", "occupation_qid", "genderCategory"],
            keep="last"
        )

    combined.to_csv(OUT_REFINED, index=False, encoding="utf-8")
    print(f"✅ Updated {OUT_REFINED.name}: {len(combined)} rows total")

if __name__ == "__main__":
    main()