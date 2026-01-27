from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

MASTER = DATA_DIR / "gender_occupation_raw_long.csv"
OUT = DATA_DIR / "gender_occupation_with_labels.csv"
CACHE = DATA_DIR / "occupation_labels_cache.csv"

BATCH = 400   # safe WDQS batch size
SLEEP = 1.0

LABEL_QUERY = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?occupation ?occupationLabel WHERE {
  VALUES ?occupation { %s }
  OPTIONAL {
    ?occupation rdfs:label ?occupationLabel .
    FILTER(LANG(?occupationLabel) = "en")
  }
}
"""

def run_sparql(query: str) -> pd.DataFrame:
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent", "Wiki-dashboard/1.0 (https://github.com/AyshaTolefat)")
    results = sparql.query().convert()
    rows = []
    for b in results["results"]["bindings"]:
        rows.append({k: v.get("value") for k, v in b.items()})
    return pd.DataFrame(rows)

def batched(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    df = pd.read_csv(MASTER)
    occ_qids = sorted(df["occupation_qid"].astype(str).unique())

    # load cache if exists
    if CACHE.exists():
        cache_df = pd.read_csv(CACHE)
        cached = set(cache_df["occupation_qid"].astype(str).unique())
    else:
        cache_df = pd.DataFrame(columns=["occupation_qid", "occupationLabel"])
        cached = set()

    missing = [q for q in occ_qids if q not in cached]
    print("Unique occupations:", len(occ_qids))
    print("Cached occupations:", len(cached))
    print("Need to fetch:", len(missing))

    new_rows = []

    for idx, chunk in enumerate(batched(missing, BATCH), start=1):
        values = " ".join(f"wd:{q}" for q in chunk)
        q = LABEL_QUERY % values
        try:
            out = run_sparql(q)
        except Exception as e:
            print("WDQS error; retrying after 10s:", e)
            time.sleep(10)
            out = run_sparql(q)

        if not out.empty:
            out["occupation_qid"] = out["occupation"].str.rsplit("/", n=1).str[-1]
            out = out[["occupation_qid", "occupationLabel"]]
            new_rows.append(out)

        print(f"Batch {idx}: fetched {len(chunk)}")
        time.sleep(SLEEP)

    if new_rows:
        new_df = pd.concat(new_rows, ignore_index=True)
        cache_df = pd.concat([cache_df, new_df], ignore_index=True)
        cache_df = cache_df.drop_duplicates(subset=["occupation_qid"]).reset_index(drop=True)
        cache_df.to_csv(CACHE, index=False)
        print("Updated cache:", CACHE)

    # merge into master
    df = df.merge(cache_df, on="occupation_qid", how="left")

    # fill label fallback
    df["occupationLabel"] = df["occupationLabel"].fillna(df["occupation_qid"])

    df.to_csv(OUT, index=False)
    print("Wrote:", OUT)

if __name__ == "__main__":
    main()
