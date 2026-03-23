from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path
from datetime import datetime

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

OUTPUT_PATH = DATA_DIR / "languages_by_country.csv"
CHECKPOINT_PATH = DATA_DIR / "languages_us_germany_checkpoint.csv"

COUNTRIES = [
    {
        "qid": "Q30",
        "label": "United States",
        "uri": "http://www.wikidata.org/entity/Q30",
    },
    {
        "qid": "Q183",
        "label": "Germany",
        "uri": "http://www.wikidata.org/entity/Q183",
    },
]

# SAME LOGIC AS YOUR WORKING QUERY:
# - ?person wdt:P31 wd:Q5
# - ?person wdt:P19 / wdt:P17 ?country
# - OPTIONAL dob
# - FILTER dob >= 1900
# - language from P103 or P1412
#
# Difference:
# - one property at a time
# - one year window at a time
# - label fetched later in a second pass
WINDOW_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?language (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .

  OPTIONAL {{ ?person wdt:P569 ?dob . }}
  FILTER(
    !BOUND(?dob) ||
    (
      ?dob >= "{start}T00:00:00Z"^^xsd:dateTime &&
      ?dob <  "{end}T00:00:00Z"^^xsd:dateTime
    )
  )

  ?person {prop} ?language .
}}
GROUP BY ?country ?language
ORDER BY DESC(?count)
"""

# Second-pass English label fetch for all unique language URIs collected.
LABELS_QUERY_TEMPLATE = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?language ?languageLabel
WHERE {{
  VALUES ?language {{ {values_block} }}
  OPTIONAL {{
    ?language rdfs:label ?languageLabel .
    FILTER(LANG(?languageLabel) = "en")
  }}
}}
"""

def run_sparql(query: str, retries: int = 5, sleep_between: int = 12) -> pd.DataFrame:
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

        except Exception as e:
            last_error = e
            if attempt == retries:
                print(f"    giving up: {e}")
                raise
            print(f"    retrying after error: {e}")
            time.sleep(sleep_between * attempt)

    raise last_error


def year_windows(start_year: int = 1900, step: int = 5):
    current_year = datetime.now().year
    windows = []
    y = start_year
    while y <= current_year:
        end_y = min(y + step, current_year + 1)
        windows.append((y, end_y))
        y = end_y
    return windows


def fetch_one_window(country: dict, prop: str, type_name: str, start_year: int, end_year: int) -> pd.DataFrame | None:
    query = WINDOW_QUERY.format(
        country_qid=country["qid"],
        prop=prop,
        start=f"{start_year}-01-01",
        end=f"{end_year}-01-01",
    )

    try:
        df = run_sparql(query)
    except Exception:
        return None

    if df.empty:
        return None

    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["type"] = type_name
    df["country"] = country["uri"]
    df["countryLabel"] = country["label"]

    cols = ["country", "countryLabel", "language", "type", "count"]
    return df[cols]


def fetch_country_languages(country: dict, window_size: int = 5) -> pd.DataFrame:
    all_parts = []

    # native = P103
    # spoken = P1412
    type_props = [
        ("native", "wdt:P103"),
        ("spoken", "wdt:P1412"),
    ]

    for type_name, prop in type_props:
        print(f"  {type_name}...")
        for start_year, end_year in year_windows(1900, window_size):
            print(f"    {start_year}-{end_year - 1} ...")
            df = fetch_one_window(country, prop, type_name, start_year, end_year)
            if df is None or df.empty:
                print("      no rows / skipped")
            else:
                print(f"      got {len(df)} rows")
                all_parts.append(df)
                save_checkpoint(all_parts)
            time.sleep(1.5)

    if not all_parts:
        return pd.DataFrame(columns=["country", "countryLabel", "language", "type", "count"])

    combined = pd.concat(all_parts, ignore_index=True)

    # Sum counts across windows
    combined = (
        combined.groupby(["country", "countryLabel", "language", "type"], as_index=False)["count"]
        .sum()
    )

    return combined


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def fetch_english_labels_for_languages(language_uris: list[str], batch_size: int = 100) -> dict[str, str]:
    label_map = {}

    if not language_uris:
        return label_map

    unique_uris = sorted(set(language_uris))

    for batch in chunked(unique_uris, batch_size):
        values_block = " ".join(f"<{uri}>" for uri in batch)
        query = LABELS_QUERY_TEMPLATE.format(values_block=values_block)

        try:
            df = run_sparql(query, retries=4, sleep_between=8)
        except Exception:
            df = pd.DataFrame()

        if df is not None and not df.empty:
            for _, row in df.iterrows():
                uri = str(row.get("language", "")).strip()
                label = str(row.get("languageLabel", "")).strip()
                if uri:
                    label_map[uri] = label

        time.sleep(1.0)

    return label_map


def apply_language_labels(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        df["languageLabel"] = ""
        return df

    language_uris = df["language"].dropna().astype(str).tolist()
    label_map = fetch_english_labels_for_languages(language_uris)

    def get_label(uri: str) -> str:
        uri = str(uri).strip()
        label = label_map.get(uri, "").strip()
        if label:
            return label
        if uri.startswith("http://www.wikidata.org/entity/"):
            return uri.rsplit("/", 1)[-1]
        return uri

    df["languageLabel"] = df["language"].apply(get_label)
    return df


def save_checkpoint(parts: list[pd.DataFrame]) -> None:
    if not parts:
        return
    df = pd.concat(parts, ignore_index=True)
    df.to_csv(CHECKPOINT_PATH, index=False)


def load_existing_csv() -> pd.DataFrame:
    if OUTPUT_PATH.exists():
        df = pd.read_csv(OUTPUT_PATH)
        print(f"Loaded existing CSV: {OUTPUT_PATH}")
        return df

    print("languages_by_country.csv not found. Creating a new one.")
    return pd.DataFrame(columns=["country", "countryLabel", "language", "languageLabel", "type", "count"])


def merge_into_dashboard(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    target_uris = {c["uri"] for c in COUNTRIES}

    if "country" not in existing_df.columns:
        existing_df["country"] = ""

    cleaned_existing = existing_df[~existing_df["country"].isin(target_uris)].copy()

    final_df = pd.concat([cleaned_existing, new_df], ignore_index=True)

    cols = ["country", "countryLabel", "language", "languageLabel", "type", "count"]
    for col in cols:
        if col not in final_df.columns:
            final_df[col] = ""

    final_df = final_df[cols]
    final_df["count"] = pd.to_numeric(final_df["count"], errors="coerce").fillna(0).astype(int)

    final_df = (
        final_df.groupby(["country", "countryLabel", "language", "languageLabel", "type"], as_index=False)["count"]
        .sum()
        .sort_values(["countryLabel", "type", "count"], ascending=[True, True, False])
        .reset_index(drop=True)
    )

    return final_df


def main():
    print("Querying Wikidata: person-based languages for United States and Germany")
    print("Using same properties as main script, with 5-year windows and second-pass English label resolution")

    all_country_frames = []

    for idx, country in enumerate(COUNTRIES, start=1):
        print(f"[{idx}/{len(COUNTRIES)}] {country['label']} ({country['qid']})")
        df_country = fetch_country_languages(country, window_size=5)

        if df_country.empty:
            print("  no rows fetched")
        else:
            print(f"  total raw aggregated rows: {len(df_country)}")
            all_country_frames.append(df_country)

        time.sleep(2)

    if not all_country_frames:
        print("No data fetched for United States or Germany.")
        return

    new_df = pd.concat(all_country_frames, ignore_index=True)

    print("Resolving English labels for all fetched language URIs...")
    new_df = apply_language_labels(new_df)

    cols = ["country", "countryLabel", "language", "languageLabel", "type", "count"]
    new_df = new_df[cols]

    existing_df = load_existing_csv()
    final_df = merge_into_dashboard(existing_df, new_df)

    final_df.to_csv(OUTPUT_PATH, index=False)

    print(f"\nSaved updated dashboard CSV to: {OUTPUT_PATH}")
    print(f"Checkpoint saved to: {CHECKPOINT_PATH}")

    preview = final_df[final_df["country"].isin([c["uri"] for c in COUNTRIES])]
    print("\nPreview of inserted US/Germany rows:")
    print(preview.head(100).to_string(index=False))


if __name__ == "__main__":
    main()