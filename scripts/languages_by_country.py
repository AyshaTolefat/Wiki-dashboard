from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ALLOWED_COUNTRIES_PATH = DATA_DIR / "allowed_countries_qids.csv"
OUTPUT_PATH = DATA_DIR / "languages_by_country.csv"

# Native language: P103
# Spoken/written language: P1412
LANGUAGES_COUNTRY_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?country ?countryLabel ?language ?languageLabel ?type (COUNT(?person) AS ?count)
WHERE {{
  VALUES ?country {{ wd:{country_qid} }}

  ?person wdt:P31 wd:Q5 .
  ?person wdt:P19 / wdt:P17 ?country .

  # Keep consistent with your other queries: only "modern-ish" people
  OPTIONAL {{ ?person wdt:P569 ?dob . }}
  FILTER( !BOUND(?dob) || ?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime )

  {{
    ?person wdt:P103 ?language .
    BIND("native" AS ?type)
  }}
  UNION
  {{
    ?person wdt:P1412 ?language .
    BIND("spoken" AS ?type)
  }}

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
GROUP BY ?country ?countryLabel ?language ?languageLabel ?type
ORDER BY ?country ?type DESC(?count)
"""

def run_sparql(query: str, retries: int = 3, sleep_between: int = 10) -> pd.DataFrame:
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
            if attempt == retries:
                print("  giving up on this query")
                raise
            else:
                print("  retrying")
                time.sleep(sleep_between)

def load_allowed_countries() -> pd.DataFrame:
    if not ALLOWED_COUNTRIES_PATH.exists():
        raise SystemExit(f"Expected {ALLOWED_COUNTRIES_PATH} to exist")

    df = pd.read_csv(ALLOWED_COUNTRIES_PATH)

    # Your file has: country (URI), countryLabel (label)
    # Convert URI -> QID
    def to_qid(val: str) -> str:
        if isinstance(val, str) and val.startswith("http"):
            return val.rsplit("/", 1)[-1]
        return str(val)

    df["country_qid"] = df["country"].apply(to_qid)
    return df[["country", "country_qid", "countryLabel"]]

def fetch_languages_for_country(country_qid: str) -> pd.DataFrame | None:
    query = LANGUAGES_COUNTRY_QUERY.format(country_qid=country_qid)
    try:
        df = run_sparql(query)
    except Exception:
        return None

    if df.empty:
        return None

    if "count" in df.columns:
        df["count"] = df["count"].astype(int)

    # Keep only expected columns (consistent + tidy)
    cols = ["country", "countryLabel", "language", "languageLabel", "type", "count"]
    return df[cols]

def fetch_languages_by_country_all() -> pd.DataFrame | None:
    countries_df = load_allowed_countries()
    total = len(countries_df)

    print("Querying Wikidata: languages (native/spoken) by country")

    all_rows: list[pd.DataFrame] = []

    for idx, row in countries_df.iterrows():
        country_uri = row["country"]
        country_qid = row["country_qid"]
        label = row["countryLabel"]

        print(f"[{idx + 1}/{total}] {label} ({country_qid})")

        df_country = fetch_languages_for_country(country_qid)

        if df_country is None:
            print("  no results / error")
            time.sleep(1.0)
            continue

        # Ensure label + country uri are correct even if WDQS label service returns something odd
        df_country["country"] = country_uri
        df_country["countryLabel"] = label

        all_rows.append(df_country)
        time.sleep(1.0)

    if not all_rows:
        print("No data fetched for any country.")
        return None

    df_all = pd.concat(all_rows, ignore_index=True)
    df_all.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved languages-by-country data to: {OUTPUT_PATH}")
    print("Preview:")
    print(df_all.head())

    return df_all

if __name__ == "__main__":
    fetch_languages_by_country_all()
