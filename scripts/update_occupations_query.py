from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

MISSING_QIDS_PATH = DATA_DIR / "missing_iso3_to_qid.csv"
OUT_REFINED = DATA_DIR / "gender_occupation_with_isco_refined.csv"

# Safety: territories should be small, but you can cap per country if you want.
MAX_ROWS_PER_COUNTRY = None  # e.g. 5000, or None for no cap

OCC_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?occupation ?occupationLabel ?genderCategory (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .
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
            sparql.addCustomHttpHeader(
                "User-Agent",
                "Wiki-dashboard/1.0 (https://github.com/AyshaTolefat)"
            )
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
    """
    Ensure df has all columns used by gender_occupation_with_isco_refined.csv.
    """
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
    if not MISSING_QIDS_PATH.exists():
        raise FileNotFoundError(f"Missing QID file not found: {MISSING_QIDS_PATH}")

    missing = pd.read_csv(MISSING_QIDS_PATH)
    if "qid" not in missing.columns:
        raise ValueError(f"'qid' column not found in {MISSING_QIDS_PATH}. Columns: {list(missing.columns)}")

    # pick best label column
    label_col = None
    for c in ["wikidata_label", "name", "countryLabel"]:
        if c in missing.columns:
            label_col = c
            break
    if label_col is None:
        missing["wikidata_label"] = missing["qid"]
        label_col = "wikidata_label"

    missing["qid"] = missing["qid"].astype(str).str.strip()
    missing = missing[missing["qid"].str.match(r"^Q\d+$", na=False)].drop_duplicates(subset=["qid"]).reset_index(drop=True)

    total = len(missing)
    print(f"➡️ Fetching OCCUPATIONS for MISSING territories only: {total}")

    new_rows = []

    for idx, r in missing.iterrows():
        country_qid = r["qid"]
        country_label = str(r[label_col]) if pd.notna(r[label_col]) else country_qid
        print(f"[{idx+1}/{total}] {country_label} ({country_qid})")

        q = OCC_QUERY.format(country_qid=country_qid)
        df = None
        try:
            df = run_sparql(q)
        except Exception:
            print("  error / timeout")
            time.sleep(2.0)
            continue

        if df is None or df.empty:
            print("  no results")
            time.sleep(1.0)
            continue

        if MAX_ROWS_PER_COUNTRY is not None and len(df) > MAX_ROWS_PER_COUNTRY:
            df = df.head(MAX_ROWS_PER_COUNTRY)

        df["count"] = df["count"].astype(int)

        # Build rows matching refined schema
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

                # defaults (so dashboard works immediately)
                "sector": "Other / Unclassified",
                "sector_source": "missing_update_default",
                "isco_major_code": "",
                "isco_major_title": "",
                "isco_sub_major_code": "",
                "isco_sub_major_title": "",
                "isco_mapping_method": "unmapped",
                "isco_mapping_notes": "Added via missing-territory WDQS update; run mapping scripts to classify",
            })

        time.sleep(1.2)

    if not new_rows:
        print("No occupation rows fetched for missing territories.")
        return

    new_df = pd.DataFrame(new_rows)
    new_df = ensure_refined_schema(new_df)

    # Load existing refined file (if it exists), then append+dedupe
    if OUT_REFINED.exists():
        old = pd.read_csv(OUT_REFINED)
        old = ensure_refined_schema(old)
        combined = pd.concat([old, new_df], ignore_index=True)

        # de-dupe key for occupation counts
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
    print(f"   Added/updated approx: {len(new_df)} rows (before dedupe)")

if __name__ == "__main__":
    main()