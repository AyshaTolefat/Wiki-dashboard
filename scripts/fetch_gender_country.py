from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
from io import StringIO
import os

os.makedirs("../data", exist_ok=True)

ENDPOINT = "https://query.wikidata.org/sparql"

PEOPLE_QUERY = """
SELECT ?person ?country ?genderCategory WHERE {
    ?person wdt:P31 wd:Q5 . #human
    OPTIONAL { ?person wdt:P21 ?gender . } #sex or gender
    ?person wdt:P19 / wdt:P17 ?country . #country of citizenship
    OPTIONAL { ?person wdt:P569 ?dob . } #date of birth

    FILTER( !BOUND(?dob) || ?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime)

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
}
LIMIT 1000000
"""

def run_sparql(query:str) -> pd.DataFrame:
    """Helper: run a SPARQL query and returns a pandas dataframe."""
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

def fetch_country_labels(country_ids):
    """Given a list of country URIs, fetch english labels."""
    qids = [cid.rsplit("/", 1)[-1] for cid in country_ids]
    values_block = " ".join(f"wd:{qid}" for qid in qids)
    label_query = f"""
    SELECT ?country ?countryLabel WHERE {{
        VALUES ?country {{ {values_block} }}
        SERVICE wikibase:label {{
            bd:serviceParam wikibase:language "en" .
        }}
    }}
    """
    df_labels = run_sparql(label_query)
    return df_labels

def fetch_gender_by_country():
    print("Querying wikidata for gender and country")
    df_raw = run_sparql(PEOPLE_QUERY)
    print("Raw sample rows:", len(df_raw))
    print(df_raw.head())
    df_agg = (
        df_raw
        .groupby(["country", "genderCategory"])
        .size()
        .reset_index(name="count")
    )
    unique_countries = df_agg["country"].unique().tolist()
    print("Fetching labels for", len(unique_countries), "countries...")
    df_labels = fetch_country_labels(unique_countries)
    df_final = df_agg.merge(df_labels, on="country", how="left")
    raw_path = "../data/gender_country_people_sample.csv"
    agg_path = "../data/gender_country.csv"
    df_raw.to_csv(raw_path, index=False)
    df_final.to_csv(agg_path, index=False)
    print(f"saved raw sample to {raw_path} ({len(df_raw)} rows)")
    print(f"saved aggregated counts with labels to {agg_path} ({len(df_final)} rows)")
    return df_final

if __name__ == "__main__":
    df = fetch_gender_by_country()
    print("Aggregated preview: ")
    print(df.head())