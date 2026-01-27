import pandas as pd
from clean_gender_country import ALLOWED_COUNTRIES

def main():
    df = pd.read_csv("../data/gender_decades_by_country.csv")
    print("original rows:", len(df))
    print("original unique countries:", df["countryLabel"].nunique())
    df_modern = df[df["countryLabel"].isin(ALLOWED_COUNTRIES)].copy()
    df_modern["genderCategory"] = df_modern["genderCategory"].replace({"Unknown/ not stated": "Unknown / not stated"})
    
    print("filtered rows:", len(df_modern))
    print("filtered unique countries:", df_modern["countryLabel"].nunique())
    print("modern country list:")
    print(sorted(df_modern["countryLabel"].unique()))
    out_path = "../data/gender_decades_modern.csv"
    df_modern.to_csv(out_path, index=False)
    print("saced cleaned modern decades dataset")

if __name__ == "__main__":
    main()