from datetime import datetime
import pandas as pd
from fetch_gender_country import run_sparql, fetch_country_labels

PEOPLE_QUERY_DECADES = """
SELECT ?person ?country ?genderCategory ?dob WHERE {
    ?person wdt:P31 wd:Q5 .
    OPTIONAL { ?person wdt:P21 ?gender . }
    ?person wdt:P19 / wdt:P17 ?country .
    OPTIONAL { ?person wdt:P569 ?dob . }
    FILTER( !BOUND(?dob) || ?dob >= "1900-01-01T00:00:00Z"^^xsd:dateTime )

    BIND(
        IF(
            !BOUND(?gender),
            "Unknown/ not stated",
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

def fetch_gender_decades_by_country():
    print("Querying wikidata for gender + country + dob")
    df_raw = run_sparql(PEOPLE_QUERY_DECADES)
    print("Raw rows:", len(df_raw))
    print(df_raw.head())
    df_raw["dob"] = pd.to_datetime(df_raw.get("dob"), errors="coerce")
    df_raw["year"] = df_raw["dob"].dt.year
    df_clean = df_raw.dropna(subset=["year"]).copy()
    df_clean["year"] = df_clean["year"].astype(int)
    current_year = datetime.now().year
    df_clean = df_clean[(df_clean["year"] >= 1900) & (df_clean["year"] <= current_year)]
    df_clean["decade"] = (df_clean["year"] // 10) * 10
    df_agg = (
        df_clean.groupby(["country", "decade", "genderCategory"])
        .size()
        .reset_index(name="count")
    )
    unique_countries = df_agg["country"].unique().tolist()
    print("Fetching labels for", len(unique_countries), "countries..")
    df_labels = fetch_country_labels(unique_countries)
    df_final = df_agg.merge(df_labels, on="country", how="left")
    raw_path = "../data/gender_decades_people_sample.csv"
    agg_path = "../data/gender_decades_by_country.csv"
    df_clean.to_csv(raw_path, index=False)
    df_final.to_csv(agg_path, index=False)
    print(f"Saved cleaned people sample with decades")
    print("Saved aggregate decade counts")
    return df_final

if __name__ == "__main__":
    df = fetch_gender_decades_by_country()
    print("Aggregated preview:")
    print(df.head())