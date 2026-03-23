from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
from pathlib import Path
import time

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

MISSING_LIST = DATA_DIR / "missing_countries_on_globe.csv"
OUTPUT = DATA_DIR / "missing_iso3_to_qid.csv"

sparql = SPARQLWrapper(ENDPOINT)
sparql.setReturnFormat(JSON)

QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?place ?placeLabel WHERE {{
  ?place wdt:P298 "{iso3}" .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
LIMIT 5
"""

def qid_from_uri(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]

def main():
    df = pd.read_csv(MISSING_LIST)

    # fix column name if needed
    if "iso3" not in df.columns:
        # your file sometimes has geo_iso3 etc
        for c in df.columns:
            if "iso" in c.lower():
                df.rename(columns={c: "iso3"}, inplace=True)
                break

    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()

    results = []
    failed = []

    for iso3, name in zip(df["iso3"], df.get("name", [""] * len(df))):
        q = QUERY.format(iso3=iso3)
        try:
            sparql.setQuery(q)
            data = sparql.query().convert()
            bindings = data["results"]["bindings"]

            if not bindings:
                failed.append((iso3, name))
            else:
                # take first match
                place_uri = bindings[0]["place"]["value"]
                place_label = bindings[0]["placeLabel"]["value"]
                results.append(
                    {
                        "iso3": iso3,
                        "name": name if isinstance(name, str) else "",
                        "wikidata_label": place_label,
                        "qid": qid_from_uri(place_uri),
                    }
                )
        except Exception as e:
            failed.append((iso3, name))
        time.sleep(0.1)  # be nice to Wikidata

    out = pd.DataFrame(results).sort_values("iso3")
    out.to_csv(OUTPUT, index=False, encoding="utf-8")

    if failed:
        print("Failed ISO3s:")
        for iso3, name in failed:
            print(" -", iso3, name)

    print(f"Saved: {OUTPUT} ({len(out)} rows)")

if __name__ == "__main__":
    main()