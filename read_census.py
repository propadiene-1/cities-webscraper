import pandas as pd
df = pd.read_csv("./france_census/dossier_complet.csv", sep=";", nrows=0)
prefixes = set(c[:4] for c in df.columns if c[0] == "P" and c[1:3].isdigit())
print(sorted(prefixes))

p06 = [c for c in df.columns if c.startswith("P06_NSCOL")]
p11 = [c for c in df.columns if c.startswith("P11_NSCOL")]
p16 = [c for c in df.columns if c.startswith("P16_NSCOL")]
print("2006:", p06)
print("2011:", p11)
print("2016:", p16)