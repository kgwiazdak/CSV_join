import numpy as np
import pandas as pd
import os

BIG_NUMBER = 100
nr = 0
column_name = "title"
mode = "inner"


# get headers from csv
with open("movie.csv", "r") as movies:
    movies_header = movies.readline().split(",")
    if movies_header[0]=="": del movies_header[0]
    if movies_header[-1]=="": del movies_header[-1]
    if movies_header[-1].endswith("\n"): movies_header[-1]=movies_header[-1][:-1]

with open("film.csv", "r") as movies:
    films_header = movies.readline().split(",")
    if films_header[0]=="": del films_header[0]
    if films_header[-1]=="": del films_header[-1]
    if films_header[-1].endswith("\n"): films_header[-1]=films_header[-1][:-1]

# set indexes in inner headers
arr1 =[]; arr2 = []
for i, x in enumerate(movies_header):
    if x in films_header:
        arr1.append(i)
        arr2.append(films_header.index(x))


# which index in headers is the column_name
m_index = movies_header.index(column_name)+1
f_index = films_header.index(column_name)+1


# which indexes of column names from second header are not in first header
data= []
if mode=="inner":
    left = [i for i in range(len(films_header)) if i not in arr2]
    headers_names = movies_header+[films_header[i] for i in left]

if mode == "inner":
    for movies in pd.read_csv("movie.csv", chunksize=BIG_NUMBER):
        data1 = movies.iloc[:, m_index].values
        data1_set = set(movies.iloc[:, m_index].values)
        for films in pd.read_csv("film.csv", chunksize=BIG_NUMBER):
            data2_set = set(films.iloc[:, f_index].values)
            inner_data_set = data1_set & data2_set
            data2_set.clear()
            data2 = films.iloc[:, f_index].values

            for element in inner_data_set:
                data1_set.remove(element)
                for ind, movie in enumerate(data1):
                    if movie == element:
                        i = ind
                        break
                for ind, film in enumerate(data2):
                    if film == element:
                        j = ind
                        break
                np.delete(data1, i)

                row1 = movies.iloc[i, 1:].values
                row2 = films.iloc[j, left].values

                new_data = np.concatenate((row1, row2))
                data.append(new_data)
        if len(data) > BIG_NUMBER:
            df = pd.DataFrame(data=data, columns=headers_names)
            df.to_csv("help_file" + str(nr) + ".csv")
            nr += 1

import os

files = os.listdir()
for file in files:
    if file.startswith("help_file"): os.remove(file)