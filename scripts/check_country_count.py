import pandas as pd

df = pd.read_csv("../data/gender_country.csv")
print("Number of unique countries: ", df['countryLabel'].nunique())
print(df['countryLabel'].unique())