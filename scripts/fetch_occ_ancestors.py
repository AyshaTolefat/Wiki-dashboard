from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

INP = DATA_DIR / "gender_occupation_with_labels.csv"
OUT = DATA_DIR / "occupation_ancestors_cache.csv"

BATCH = 150
SLEEP = 1.0

QUERY = """
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?occupation ?ancestor WHERE {
  VALUES ?occupation { %s }
  ?occupation wdt:P279* ?ancestor .
}
"""

def run(q):
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(q)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent", "Wiki-dashboard/1.0")
    res = sparql.query().convert()
    rows = []
    for b in res["results"]["bindings"]:
        rows.append({k: v["value"] for k, v in b.items()})
    return pd.DataFrame(rows)

def batched(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

df = pd.read_csv(INP, usecols=["occupation_qid"]).drop_duplicates()
occs = df["occupation_qid"].astype(str).tolist()

if OUT.exists():
    cache = pd.read_csv(OUT)
    done = set(cache["occupation_qid"])
else:
    cache = pd.DataFrame(columns=["occupation_qid", "ancestor_qid"])
    done = set()

missing = [o for o in occs if o not in done]
print("Need ancestors for:", len(missing))

all_new = []
for i, chunk in enumerate(batched(missing, BATCH), 1):
    values = " ".join(f"wd:{q}" for q in chunk)
    q = QUERY % values
    out = run(q)
    if not out.empty:
        out["occupation_qid"] = out["occupation"].str.rsplit("/", n=1).str[-1]
        out["ancestor_qid"] = out["ancestor"].str.rsplit("/", n=1).str[-1]
        all_new.append(out[["occupation_qid", "ancestor_qid"]])
    print(f"Batch {i}")
    time.sleep(SLEEP)

if all_new:
    cache = pd.concat([cache] + all_new).drop_duplicates()
    cache.to_csv(OUT, index=False)

print("Wrote:", OUT)
