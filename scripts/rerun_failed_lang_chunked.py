from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path
from urllib.error import HTTPError

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

FAILED_PATH = DATA_DIR / "failed_countries_languages.csv"
OUTPUT_PATH = DATA_DIR / "languages_by_country.csv"

# --- TUNING ---
RETRIES = 4                       # retries per (country, year-range, property)
BASE_SLEEP_BETWEEN_RETRIES = 10
SLEEP_BETWEEN_REQUESTS = 1.2

START_YEAR = 1900
END_YEAR = 2026

# Start with bigger chunks; script will split automatically on 504/timeouts.
INITIAL_CHUNK = 20
MIN_CHUNK = 1  # will go down to single-year if needed

# IMPORTANT: remove label service from heavy queries (we'll keep QIDs only)
# We'll attach countryLabel from your failed list (trusted)
# languageLabel will be left blank; you can resolve labels later in a cheap step.

NATIVE_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?country ?language (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .
  ?person wdt:P569 ?dob .
  BIND(year(?dob) AS ?y)
  FILTER(?y >= {y_min} && ?y <= {y_max})

  ?person wdt:P103 ?language .
}}
GROUP BY ?country ?language
ORDER BY DESC(?count)
"""

SPOKEN_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>

SELECT ?country ?language (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .
  ?person wdt:P569 ?dob .
  BIND(year(?dob) AS ?y)
  FILTER(?y >= {y_min} && ?y <= {y_max})

  ?person wdt:P1412 ?language .
}}
GROUP BY ?country ?language
ORDER BY DESC(?count)
"""

def run_sparql(query: str, retries: int = RETRIES, base_sleep: int = BASE_SLEEP_BETWEEN_RETRIES) -> pd.DataFrame:
    last_error = None
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

        except HTTPError as e:
            last_error = e
            # 429/503/504 are common transient failures on WDQS
            if e.code in (429, 502, 503, 504):
                sleep_time = min(60, base_sleep * (2 ** (attempt - 1)))
                print(f"    HTTP {e.code} retrying in {sleep_time}s")
                time.sleep(sleep_time)
                continue
            raise  # other HTTP errors are real problems

        except Exception as e:
            last_error = e
            sleep_time = min(60, base_sleep * (2 ** (attempt - 1)))
            print(f"    retrying in {sleep_time}s")
            time.sleep(sleep_time)

    # If we reach here, retries exhausted
    raise last_error

def ensure_output_has_header():
    if OUTPUT_PATH.exists():
        return
    cols = ["country", "countryLabel", "language", "languageLabel", "type", "count"]
    pd.DataFrame(columns=cols).to_csv(OUTPUT_PATH, index=False)

def append_rows(df_new: pd.DataFrame):
    # enforce schema
    cols = ["country", "countryLabel", "language", "languageLabel", "type", "count"]
    df_new = df_new[cols]
    df_new.to_csv(OUTPUT_PATH, mode="a", header=False, index=False)

def load_failed() -> pd.DataFrame:
    if not FAILED_PATH.exists():
        raise SystemExit(f"Missing {FAILED_PATH}")
    df = pd.read_csv(FAILED_PATH)

    needed = {"country", "countryLabel", "country_qid"}
    if not needed.issubset(df.columns):
        raise SystemExit(f"{FAILED_PATH} must contain {needed}. Found {df.columns.tolist()}")
    return df

def year_ranges(start: int, end: int, size: int):
    y = start
    while y <= end:
        yield (y, min(end, y + size - 1))
        y += size

def fetch_range(country_qid: str, y_min: int, y_max: int, typ: str) -> pd.DataFrame:
    if typ == "native":
        q = NATIVE_QUERY.format(country_qid=country_qid, y_min=y_min, y_max=y_max)
    else:
        q = SPOKEN_QUERY.format(country_qid=country_qid, y_min=y_min, y_max=y_max)

    df = run_sparql(q)
    if df.empty:
        return df

    df["count"] = df["count"].astype(int)
    df["type"] = typ
    # normalize to full URIs for consistency with your other outputs
    if "country" in df.columns:
        # WDQS returns URI in country; keep it
        pass
    if "language" in df.columns:
        pass
    return df

def fetch_range_adaptive(country_qid: str, y_min: int, y_max: int, typ: str, chunk_size: int) -> pd.DataFrame:
    """
    Try a range; if it times out (504/429 etc), split into smaller ranges recursively.
    """
    try:
        return fetch_range(country_qid, y_min, y_max, typ)
    except HTTPError as e:
        # Only split on timeout/throttle-like errors
        if e.code not in (429, 502, 503, 504):
            raise
        if chunk_size <= MIN_CHUNK:
            # can't split further; give up on this 1-year range
            print(f"    still failing at {y_min}-{y_max} ({typ})")
            return pd.DataFrame()
        # split
        mid = (y_min + y_max) // 2
        left_size = max(MIN_CHUNK, (mid - y_min + 1))
        right_size = max(MIN_CHUNK, (y_max - (mid + 1) + 1))
        print(f"    splitting {y_min}-{y_max} ({typ}) -> {y_min}-{mid} and {mid+1}-{y_max}")
        df_left = fetch_range_adaptive(country_qid, y_min, mid, typ, max(MIN_CHUNK, chunk_size // 2))
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        df_right = fetch_range_adaptive(country_qid, mid + 1, y_max, typ, max(MIN_CHUNK, chunk_size // 2))
        if df_left.empty:
            return df_right
        if df_right.empty:
            return df_left
        return pd.concat([df_left, df_right], ignore_index=True)

    except Exception:
        # For non-HTTP errors, also try splitting once if possible
        if chunk_size <= MIN_CHUNK:
            print(f"    still failing at {y_min}-{y_max} ({typ})")
            return pd.DataFrame()
        mid = (y_min + y_max) // 2
        print(f"    splitting {y_min}-{y_max} ({typ}) due to error -> {y_min}-{mid} and {mid+1}-{y_max}")
        df_left = fetch_range_adaptive(country_qid, y_min, mid, typ, max(MIN_CHUNK, chunk_size // 2))
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        df_right = fetch_range_adaptive(country_qid, mid + 1, y_max, typ, max(MIN_CHUNK, chunk_size // 2))
        if df_left.empty:
            return df_right
        if df_right.empty:
            return df_left
        return pd.concat([df_left, df_right], ignore_index=True)

def aggregate_chunks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sum counts across all year ranges for the same (country, language, type).
    """
    if df.empty:
        return df
    group_cols = ["country", "language", "type"]
    out = df.groupby(group_cols, as_index=False)["count"].sum()
    return out

def main():
    ensure_output_has_header()
    failed = load_failed()
    total = len(failed)
    print(f"Chunked rerun (adaptive, no LIMIT) for {total} countries")

    still_failed = []

    for i, row in failed.iterrows():
        country_uri = row["country"]
        country_label = row["countryLabel"]
        qid = row["country_qid"]

        print(f"[{i+1}/{total}] {country_label} ({qid})")

        all_frames = []
        ok_any = False

        for typ in ("native", "spoken"):
            print(f"  {typ}...")
            typ_frames = []
            for (ymin, ymax) in year_ranges(START_YEAR, END_YEAR, INITIAL_CHUNK):
                print(f"    chunk {ymin}-{ymax}")
                df_part = fetch_range_adaptive(qid, ymin, ymax, typ, INITIAL_CHUNK)
                if not df_part.empty:
                    typ_frames.append(df_part)
                    ok_any = True
                time.sleep(SLEEP_BETWEEN_REQUESTS)

            if typ_frames:
                df_typ = pd.concat(typ_frames, ignore_index=True)
                df_typ = aggregate_chunks(df_typ)
                all_frames.append(df_typ)

        if not ok_any:
            print("  still failed (no successful chunks for native or spoken)")
            still_failed.append({"country": country_uri, "countryLabel": country_label, "country_qid": qid})
            continue

        df_country = pd.concat(all_frames, ignore_index=True)

        # Add labels/URIs to match your dashboard schema
        df_country["country"] = country_uri
        df_country["countryLabel"] = country_label
        df_country["languageLabel"] = ""  # resolve later in a small separate label step

        # reorder columns
        df_country = df_country[["country", "countryLabel", "language", "languageLabel", "type", "count"]]

        append_rows(df_country)
        print(f"  appended {len(df_country)} aggregated rows to {OUTPUT_PATH}")

        time.sleep(2.0)

    # Save updated failed list
    pd.DataFrame(still_failed).to_csv(FAILED_PATH, index=False)
    print(f"Updated failed list saved to: {FAILED_PATH}")
    print("Done.")

if __name__ == "__main__":
    main()
