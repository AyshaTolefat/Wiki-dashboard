from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time 
import os

os.makedirs("../data", exist_ok=True)

ENDPOINT = "https://query.wikidata.org/sparql"

ALLOWED_COUNTRIES = {
    "Afghanistan", "Albania", "Algeria", "Andorra", "Angola",
    "Antigua and Barbuda", "Argentina", "Armenia", "Australia",
    "Austria", "Azerbaijan", "The Bahamas", "Bahrain", "Bangladesh",
    "Barbados", "Belarus", "Belgium", "Belize", "Benin",
    "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana",
    "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi",
    "Cape Verde", "Cambodia", "Cameroon", "Canada",
    "Central African Republic", "Chad", "Chile", "People's Republic of China",
    "Colombia", "Comoros", "Democratic Republic of the Congo",
    "Republic of the Congo", "Costa Rica", "Ivory Coast",
    "Croatia", "Cuba", "Cyprus", "Czech Republic", "Denmark",
    "Djibouti", "Dominica", "Dominican Republic", "Ecuador",
    "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea",
    "Estonia", "Eswatini", "Ethiopia", "Fiji", "Finland",
    "France", "Gabon", "The Gambia", "Georgia", "Germany", "Ghana",
    "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau",
    "Guyana", "Haiti", "Honduras", "Hungary", "Iceland", "India",
    "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Italy",
    "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya",
    "Kiribati", "North Korea", "South Korea", "Kuwait",
    "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho",
    "Liberia", "Libya", "Liechtenstein", "Lithuania",
    "Luxembourg", "Madagascar", "Malawi", "Malaysia",
    "Maldives", "Mali", "Malta", "Marshall Islands",
    "Mauritania", "Mauritius", "Mexico", "Federated States of Micronesia",
    "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco",
    "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal",
    "Kingdom of the Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria",
    "North Macedonia", "Norway", "Oman", "Pakistan", "Palau",
    "Panama", "Papua New Guinea", "Paraguay", "Peru",
    "Philippines", "Poland", "Portugal", "Qatar", "Romania",
    "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia",
    "Saint Vincent and the Grenadines", "Samoa", "San Marino",
    "São Tomé and Príncipe", "Saudi Arabia", "Senegal",
    "Serbia", "Seychelles", "Sierra Leone", "Singapore",
    "Slovakia", "Slovenia", "Solomon Islands", "Somalia",
    "South Africa", "South Sudan", "Spain", "Sri Lanka",
    "Sudan", "Suriname", "Sweden", "Switzerland", "Syria",
    "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Togo",
    "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey",
    "Turkmenistan", "Tuvalu", "Uganda", "Ukraine",
    "United Arab Emirates", "United Kingdom", "United States",
    "Uruguay", "Uzbekistan", "Vanuatu", "Vatican City",
    "Venezuela", "Vietnam", "Yemen", "Zambia", "Zimbabwe",
    "Palestine"
}

def run_sparql(query: str) -> pd.DataFrame:
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader(
        "User-Agent",
        "Wiki-dashboard/1,0 (https://github.com/AyshaTolefat)"
    )
    results = sparql.query().convert()
    rows = []
    for binding in results["results"]["bindings"]:
        row = {}
        for var, value_dict in binding.items():
            row[var] = value_dict.get("value")
        rows.append(row)
    return pd.DataFrame(rows)

def fetch_allowed_countries() -> pd.DataFrame:
    values_block = "\n        ".join(
        f"\"{label}\"@en" for label in ALLOWED_COUNTRIES
    )
    query = f"""
    PREFIX wd:   <http://www.wikidata.org/entity/>
    PREFIX wdt:  <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?country ?countryLabel WHERE {{
      ?country wdt:P31/wdt:P279* wd:Q3624078 .   
      ?country rdfs:label ?countryLabel .
      FILTER (LANG(?countryLabel) = "en")

      VALUES ?countryLabel {{
        {values_block}
      }}
    }}
    """
    print("Fetching only modern countries from wikidata")
    df = run_sparql(query).drop_duplicates(subset=["country"])
    df = df.sort_values("countryLabel").reset_index(drop=True)
    out_path = "../data/allowed_countries_qids.csv"
    df.to_csv(out_path, index=False)
    print("Got countries, saved mapping.")

GENDER_COUNTRY_QUERY = """
PREFIX wd:  <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?genderCategory (COUNT(?person) AS ?count) WHERE {{
  ?person wdt:P31 wd:Q5 .      
  OPTIONAL {{ ?person wdt:P21 ?gender . }}   
  ?person wdt:P19 / wdt:P17 ?country .       
  VALUES ?country {{ wd:{country_qid} }}     

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
}}
GROUP BY ?genderCategory
"""
def fetch_gender_for_country(country_qid: str, country_label:str) -> pd.DataFrame | None:
    query = GENDER_COUNTRY_QUERY.format(country_qid=country_qid)
    try:
        df = run_sparql(query)
    except Exception as e:
        print ("Error")
        return None
    df["count"] = df["count"].astype(int)
    df["country"] = f"http://www.wikidata.org/entity/{country_qid}"
    df["countryLabel"] = country_label
    return df[["country", "countryLabel", "genderCategory", "count"]]

def fetch_gender_by_country_all() -> pd.DataFrame | None:
    df_countries = fetch_allowed_countries()
    all_rows = []
    total = len(df_countries)
    print("Query gender distribution for countries 1900-present")
    for idx, row in df_countries.iterrows():
        uri = row["country"]
        label = row["countryLabel"]
        qid = uri.rsplit("/", 1)[-1]
        print(f"[{idx+1}/{total}] {label} ({qid})")
        df_c = fetch_gender_for_country(qid, label)
        if df_c is not None:
            all_rows.append(df_c)
        time.sleep(1.5)
    df_final = pd.concat(all_rows, ignore_index=True)
    out_path = "../data/gender_country_1900_present_per_country.csv"
    df_final.to_csv(out_path, index=False)
    print("Saved combined gender-country data")
    return df_final

if __name__ == "__main__":
    df = fetch_gender_by_country_all()
    if df is not None:
        print("preview data: ")
        print(df.head())






