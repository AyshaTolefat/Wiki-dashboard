from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path
from typing import List, Iterable


# -----------------------------
# Paths / constants
# -----------------------------

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ALLOWED_COUNTRIES_PATH = DATA_DIR / "allowed_countries_qids.csv"
RAW_OUT = DATA_DIR / "gender_occupation_raw_long.csv"

CHECKPOINT_DIR = DATA_DIR / "checkpoints_gender_occ"
CHECKPOINT_DIR.mkdir(exist_ok=True)

# Safer defaults for large countries (tune if needed)
OCC_PAGE_SIZE = 1000
OCC_VALUES_BATCH_START = 100   # auto-reduces on failure down to min
OCC_VALUES_BATCH_MIN = 25

SLEEP_BETWEEN_CALLS = 1.2

# Optional: set to True if you want to start fresh each run
# (otherwise it will append and you can dedupe later)
START_FRESH = False


# -----------------------------
# SPARQL queries
# -----------------------------

OCCUPATIONS_LIST_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?occupation WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 ?pob .
  ?pob wdt:P17 ?country .
  ?person wdt:P106 ?occupation .

  OPTIONAL {{ ?person wdt:P569 ?dob . }}
  FILTER( !BOUND(?dob) || ?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime )
}}
ORDER BY ?occupation
LIMIT {limit}
OFFSET {offset}
"""

# No labels here (faster). Label enrichment happens later.
GENDER_COUNTS_FOR_OCC_BATCH_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?occupation ?genderCategory (COUNT(?person) AS ?count) WHERE {{
  VALUES ?country {{ wd:{country_qid} }}
  VALUES ?occupation {{ {occupation_values} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 ?pob .
  ?pob wdt:P17 ?country .
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
    ) AS ?genderCategory
  )
}}
GROUP BY ?country ?occupation ?genderCategory
"""


# -----------------------------
# Helpers
# -----------------------------

def run_sparql(query: str, retries: int = 6) -> pd.DataFrame:
    """
    Run a SPARQL query with exponential backoff retries.
    """
    backoff = 10
    last_err: Exception | None = None

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
            for b in results["results"]["bindings"]:
                rows.append({k: v.get("value") for k, v in b.items()})
            return pd.DataFrame(rows)

        except Exception as e:
            last_err = e
            if attempt == retries:
                raise
            print(f"    WDQS error (attempt {attempt}/{retries}). Backing off {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 240)

    # Should never reach here
    raise last_err  # type: ignore[misc]


def load_allowed_countries() -> pd.DataFrame:
    if not ALLOWED_COUNTRIES_PATH.exists():
        raise SystemExit(f"Missing required file: {ALLOWED_COUNTRIES_PATH}")

    df = pd.read_csv(ALLOWED_COUNTRIES_PATH)

    def to_qid(val: str) -> str:
        if isinstance(val, str) and val.startswith("http"):
            return val.rsplit("/", 1)[-1]
        return str(val)

    df["country_qid"] = df["country"].apply(to_qid)
    # Keep label for console logging; we don't write it in raw output
    return df[["country_qid", "countryLabel"]]


def batched(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def occ_uri_to_values(occ_uris: List[str]) -> str:
    qids = [u.rsplit("/", 1)[-1] for u in occ_uris]
    return " ".join(f"wd:{qid}" for qid in qids)


def append_csv(df: pd.DataFrame, path: Path):
    header = not path.exists()
    df.to_csv(path, mode="a", header=header, index=False)


def occupations_checkpoint_path(country_qid: str) -> Path:
    return CHECKPOINT_DIR / f"occupations_{country_qid}.csv"


def done_batches_path(country_qid: str, batch_size: int) -> Path:
    # batch-size specific to avoid incorrect resume when batch size changes
    return CHECKPOINT_DIR / f"done_batches_{country_qid}_{batch_size}.txt"


def load_done_batches(country_qid: str, batch_size: int) -> set[int]:
    p = done_batches_path(country_qid, batch_size)
    if not p.exists():
        return set()
    return {int(x.strip()) for x in p.read_text().splitlines() if x.strip().isdigit()}


def mark_batch_done(country_qid: str, batch_size: int, batch_idx: int):
    p = done_batches_path(country_qid, batch_size)
    with p.open("a") as f:
        f.write(f"{batch_idx}\n")


def list_all_occupations_for_country(country_qid: str) -> List[str]:
    """
    Fetch all distinct occupations for a country (paged).
    Saves/uses a checkpoint file per country so we don't re-list on reruns.
    """
    ck = occupations_checkpoint_path(country_qid)
    if ck.exists():
        df = pd.read_csv(ck)
        return df["occupation"].astype(str).tolist()

    occs: List[str] = []
    offset = 0

    while True:
        q = OCCUPATIONS_LIST_QUERY.format(
            country_qid=country_qid,
            limit=OCC_PAGE_SIZE,
            offset=offset,
        )
        df = run_sparql(q)
        if df.empty:
            break

        occs.extend(df["occupation"].astype(str).tolist())
        offset += OCC_PAGE_SIZE
        time.sleep(SLEEP_BETWEEN_CALLS)

    # De-dupe while preserving order
    seen = set()
    out: List[str] = []
    for o in occs:
        if o not in seen:
            out.append(o)
            seen.add(o)

    pd.DataFrame({"occupation": out}).to_csv(ck, index=False)
    return out


def fetch_gender_counts_for_country(country_qid: str, occs: List[str]):
    """
    Count gender categories for all occupations for a country.
    Adaptive batch sizing: reduces VALUES batch size when failures occur.
    Resume: marks completed batch indices in a batch-size specific checkpoint file.
    """
    batch_size = OCC_VALUES_BATCH_START
    batches = list(batched(occs, batch_size))
    done = load_done_batches(country_qid, batch_size)

    batch_idx = 0
    while batch_idx < len(batches):
        if batch_idx in done:
            batch_idx += 1
            continue

        occ_chunk = batches[batch_idx]
        values = occ_uri_to_values(occ_chunk)
        q = GENDER_COUNTS_FOR_OCC_BATCH_QUERY.format(
            country_qid=country_qid,
            occupation_values=values,
        )

        try:
            df_chunk = run_sparql(q)

            if not df_chunk.empty:
                df_chunk["count"] = df_chunk["count"].astype(int)
                df_chunk["country_qid"] = country_qid
                df_chunk["occupation_qid"] = (
                    df_chunk["occupation"].astype(str).str.rsplit("/", n=1).str[-1]
                )
                df_chunk = df_chunk[
                    ["country_qid", "country", "occupation_qid", "occupation", "genderCategory", "count"]
                ]
                append_csv(df_chunk, RAW_OUT)

            mark_batch_done(country_qid, batch_size, batch_idx)
            time.sleep(SLEEP_BETWEEN_CALLS)
            batch_idx += 1

        except Exception as e:
            print(f"    Batch {batch_idx} failed at size {batch_size}: {e}")

            if batch_size <= OCC_VALUES_BATCH_MIN:
                print("    Reached minimum batch size; skipping this batch to continue.")
                mark_batch_done(country_qid, batch_size, batch_idx)
                batch_idx += 1
                continue

            # Reduce batch size and rebuild remaining batches; reset done for new batch size
            new_batch_size = max(OCC_VALUES_BATCH_MIN, batch_size // 2)
            print(f"    Reducing batch size to {new_batch_size} and rebuilding remaining batches...")

            remaining: List[str] = []
            for b in batches[batch_idx:]:
                remaining.extend(b)

            batch_size = new_batch_size
            batches = batches[:batch_idx] + list(batched(remaining, batch_size))
            done = load_done_batches(country_qid, batch_size)
            # do not increment batch_idx; retry with smaller chunk


def extract_all():
    if START_FRESH and RAW_OUT.exists():
        RAW_OUT.unlink()
        print(f"Deleted existing raw output: {RAW_OUT}")

    countries = load_allowed_countries()
    print(f"Writing raw extraction to: {RAW_OUT}")

    for idx, row in countries.iterrows():
        qid = row["country_qid"]
        label = row["countryLabel"]
        print(f"[{idx + 1}/{len(countries)}] {label} ({qid})")

        try:
            occs = list_all_occupations_for_country(qid)
        except Exception as e:
            print(f"  Failed to list occupations: {e}")
            continue

        print(f"  occupations found: {len(occs)}")
        if not occs:
            continue

        try:
            fetch_gender_counts_for_country(qid, occs)
        except Exception as e:
            print(f"  Failed counting genders for this country: {e}")
            continue

    print("Done.")


if __name__ == "__main__":
    extract_all()