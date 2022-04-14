import pandas as pd

a = pd.read_csv("/home/kgwiazda/movies.csv")
a = a.iloc[800:1600, [1,2,4]]
a.to_csv("mov.csv")

