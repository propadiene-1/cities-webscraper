import pandas as pd
df = pd.read_csv("/Users/propadiene/cloned-repos/cities-webscraper/RP1968-2022_csv/Ficdep22.csv", sep=";", encoding="utf-8", nrows=5)
print(df.columns.tolist())
print(df.head())