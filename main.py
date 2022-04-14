import numpy as np
import pandas as pd
import os
from sys import argv

class Join_csv:
    def __init__(self, filename1, filename2, column_name, join_type):
        self.filename1 = filename1
        self.filename2 = filename2
        self.column_name = column_name
        self.join_type = join_type

        # nr will be used to make files and join them
        self.nr=0

        self.header_first, self.header_second = self.get_headers()

        self.BIG_NUMBER = 100

    # return header from first and second file
    def get_headers(self):
        with open(self.filename1) as first:
            first_header = first.readline().split(",")
            if first_header[0] == "": del first_header[0]
            if first_header[-1] == "": del first_header[-1]
            if first_header[-1].endswith("\n"): first_header[-1] = first_header[-1][:-1]

        with open(self.filename2) as second:
            second_header = second.readline().split(",")
            if second_header[0] == "": del second_header[0]
            if second_header[-1] == "": del second_header[-1]
            if second_header[-1].endswith("\n"): second_header[-1] = second_header[-1][:-1]
        return first_header, second_header

    # return index of column name in first and second file
    def index_column_name(self):
        first_index = self.header_first.index(self.column_name) + 1
        second_index = self.header_second.index(self.column_name) + 1
        return first_index, second_index

    # return two arrays of indexes that are common to both files
    def inner_header_indexes(self):
        inner_index_first = [];inner_index_second = []
        for i, x in enumerate(self.header_first):
            if x in self.header_second:
                inner_index_first.append(i)
                inner_index_second.append(self.header_second.index(x))
        return inner_index_first, inner_index_second

    # construct final header and find indexes that are missing from header_second
    def header_and_left(self, iif, iis):
        if self.join_type == "inner":
            left = [i for i in range(len(self.header_second)) if i not in iis]
            headers_names = [self.header_first + [self.header_second[i]] for i in left]
        return headers_names, left

    def join_files(self, arr):
        with open("merged_csv.csv", "a") as merged_files:
            merged_files.write(",".join(self.header_names)+"\n")
            for file in arr:
                with open(file) as f:
                    f.readline()
                    merged_files.write(f.read())
                os.remove(file)


    def main_function(self):
        data = []
        arr= []
        nr = 0
        first_index, second_index = self.index_column_name()
        inner_index_first, inner_index_second = self.inner_header_indexes()
        self.header_names, left = self.header_and_left(inner_index_first, inner_index_second)
        self.header_names= self.header_names[0]

        for first_file in pd.read_csv(self.filename1, chunksize=self.BIG_NUMBER):
            data1 = first_file.iloc[:, first_index].values
            data1_set = set(first_file.iloc[:, first_index].values)
            for second_file in pd.read_csv(self.filename2, chunksize=self.BIG_NUMBER):
                data2_set = set(second_file.iloc[:, second_index].values)
                inner_data_set = data1_set & data2_set
                data2_set.clear()
                data2 = second_file.iloc[:, second_index].values

                i=-1;j=-1
                for element in inner_data_set:
                    data1_set.remove(element)
                    for ind, f1 in enumerate(data1):
                        if f1 == element:
                            i = ind
                            break
                    for ind, f2 in enumerate(data2):
                        if f2 == element:
                            j = ind
                            break
                    np.delete(data1, i)

                    if self.join_type=="inner":
                        row1 = first_file.iloc[i, 1:].values
                        row2 = second_file.iloc[j, left].values

                        new_data = np.concatenate((row1, row2))
                        data.append(new_data)

            if len(data) > self.BIG_NUMBER:
                df = pd.DataFrame(data=data, columns=self.header_names)
                file = "help_file" + str(nr) + ".csv"
                df.to_csv(file)
                arr.append(file)
                nr += 1
                data.clear()
        if len(data)!=0:
            df = pd.DataFrame(data=data, columns=self.header_names)
            df.to_csv("help_file" + str(nr) + ".csv")
        self.join_files(arr)
        return "merged_files.csv"


if __name__ == '__main__':
    jcsv = Join_csv(argv[1], argv[2], argv[3], argv[4])
    jcsv.main_function()


