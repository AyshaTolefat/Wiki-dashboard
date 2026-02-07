from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path
from urllib.error import HTTPError

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

FAILED_PATH = DATA_DIR / "failed_countries_ethnic_group.csv"
OUTPUT_PATH = DATA_DIR / "ethnic_group_by_country_gender.csv"

# Tuning
RETRIES = 4
BASE_SLEEP_BETWEEN_RETRIES = 10
SLEEP_BETWEEN_REQUESTS = 1.2

START_YEAR = 1900
END_YEAR = 2026
INITIAL_CHUNK = 20
MIN_CHUNK = 1

CHUNK_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?country ?ethnicGroup ?genderCategory (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .
  ?person wdt:P172 ?ethnicGroup .
  ?person wdt:P569 ?dob .
  BIND(year(?dob) AS ?y)
  FILTER(?y >= {y_min} && ?y <= {y_max})

  OPTIONAL {{ ?person wdt:P21 ?gender . }}

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
}}
GROUP BY ?country ?ethnicGroup ?genderCategory
ORDER BY DESC(?count)
"""

def run_sparql(query: str) -> pd.DataFrame:
    last_error = None
    for attempt in range(1, RETRIES + 1):
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

        except HTTPError as e:
            last_error = e
            if e.code in (429, 502, 503, 504):
                sleep_time = min(60, BASE_SLEEP_BETWEEN_RETRIES * (2 ** (attempt - 1)))
                print(f"    HTTP {e.code} retrying in {sleep_time}s")
                time.sleep(sleep_time)
                continue
            raise

        except Exception as e:
            last_error = e
            sleep_time = min(60, BASE_SLEEP_BETWEEN_RETRIES * (2 ** (attempt - 1)))
            print(f"    retrying in {sleep_time}s")
            time.sleep(sleep_time)

    raise last_error

def ensure_output_has_header():
    if OUTPUT_PATH.exists():
        return
    cols = ["country", "countryLabel", "ethnicGroup", "ethnicGroupLabel", "genderCategory", "count"]
    pd.DataFrame(columns=cols).to_csv(OUTPUT_PATH, index=False)

def append_rows(df_new: pd.DataFrame):
    cols = ["country", "countryLabel", "ethnicGroup", "ethnicGroupLabel", "genderCategory", "count"]
    df_new = df_new[cols]
    df_new.to_csv(OUTPUT_PATH, mode="a", header=False, index=False)

def year_ranges(start: int, end: int, size: int):
    y = start
    while y <= end:
        yield (y, min(end, y + size - 1))
        y += size

def fetch_range(country_qid: str, y_min: int, y_max: int) -> pd.DataFrame:
    q = CHUNK_QUERY.format(country_qid=country_qid, y_min=y_min, y_max=y_max)
    df = run_sparql(q)
    if df.empty:
        return df
    df["count"] = df["count"].astype(int)
    return df

def fetch_range_adaptive(country_qid: str, y_min: int, y_max: int, chunk_size: int) -> pd.DataFrame:
    try:
        return fetch_range(country_qid, y_min, y_max)
    except HTTPError as e:
        if e.code not in (429, 502, 503, 504):
            raise
        if chunk_size <= MIN_CHUNK:
            print(f"    still failing at {y_min}-{y_max}")
            return pd.DataFrame()
        mid = (y_min + y_max) // 2
        print(f"    splitting {y_min}-{y_max} -> {y_min}-{mid} and {mid+1}-{y_max}")
        left = fetch_range_adaptive(country_qid, y_min, mid, max(MIN_CHUNK, chunk_size // 2))
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        right = fetch_range_adaptive(country_qid, mid + 1, y_max, max(MIN_CHUNK, chunk_size // 2))
        if left.empty:
            return right
        if right.empty:
            return left
        return pd.concat([left, right], ignore_index=True)

def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    group_cols = ["country", "ethnicGroup", "genderCategory"]
    return df.groupby(group_cols, as_index=False)["count"].sum()

def main():
    if not FAILED_PATH.exists():
        raise SystemExit(f"Missing {FAILED_PATH} (generate it from the first run)")

    ensure_output_has_header()
    failed = pd.read_csv(FAILED_PATH)
    total = len(failed)
    print(f"Chunked rerun (no LIMIT) for {total} failed countries")

    still_failed = []

    for i, row in failed.iterrows():
        country_uri = row["country"]
        country_label = row["countryLabel"]
        qid = row["country_qid"]

        print(f"[{i+1}/{total}] {country_label} ({qid})")

        frames = []
        ok_any = False

        for (ymin, ymax) in year_ranges(START_YEAR, END_YEAR, INITIAL_CHUNK):
            print(f"    chunk {ymin}-{ymax}")
            part = fetch_range_adaptive(qid, ymin, ymax, INITIAL_CHUNK)
            if not part.empty:
                frames.append(part)
                ok_any = True
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        if not ok_any:
            print("  still failing")
            still_failed.append({"country": country_uri, "countryLabel": country_label, "country_qid": qid})
            continue

        df_country = pd.concat(frames, ignore_index=True)
        df_country = aggregate(df_country)

        # attach labels + schema
        df_country["country"] = country_uri
        df_country["countryLabel"] = country_label
        df_country["ethnicGroupLabel"] = ""

        df_country = df_country[["country", "countryLabel", "ethnicGroup", "ethnicGroupLabel", "genderCategory", "count"]]
        append_rows(df_country)

        print(f"  appended {len(df_country)} rows")
        time.sleep(2.0)

    pd.DataFrame(still_failed).to_csv(FAILED_PATH, index=False)
    print(f"Updated failed list: {FAILED_PATH}")
    print("Done.")

if __name__ == "__main__":
    main()
