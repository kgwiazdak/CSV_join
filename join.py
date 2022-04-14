import numpy as np
import pandas as pd
import os
from sys import argv


class Join_csv:
    def __init__(self, filename1, filename2, column_name, join_type):
        assert join_type in ["inner", "rightjoin", "leftjoin"]
        self.filename1 = filename1
        self.filename2 = filename2
        self.column_name = column_name
        self.join_type = join_type

        # headers of the csv files
        self.header_first, self.header_second = self.get_headers()

        self.BIG_NUMBER = 1000000

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
        inner_index_first = []
        inner_index_second = []
        for i, x in enumerate(self.header_first):
            if x in self.header_second:
                inner_index_first.append(i)
                inner_index_second.append(self.header_second.index(x))
        return inner_index_first, inner_index_second

    # construct final header and find indexes that are missing from header_second, also check if column name is valid
    def header_and_left(self, iif, iis):
        left = [i-1 for i in range(len(self.header_second)) if i not in iis]
        headers_names = [self.header_first + [self.header_second[i+1]] for i in left][0]
        assert self.column_name in headers_names
        return headers_names, left

    # join all help files to one big file "merged_csv.csv" and delete all help files
    def join_files(self, arr):
        with open("merged_csv.csv", "a") as merged_files:
            merged_files.write(",".join(self.header_names) + "\n")
            for file in arr:
                with open(file) as f:
                    f.readline()
                    merged_files.write(f.read())
                os.remove(file)

    # create small files including merged data, returns name of final big file
    def main_function(self):
        data = []
        arr = []
        nr = 0

        # if join type is right join, the solution of the task is very similar to leftjoin, to solve it
        # we can just swap first file with second one
        if self.join_type == "rightjoin":
            self.filename1, self.filename2 = self.filename2, self.filename1

        first_index, second_index = self.index_column_name()
        inner_index_first, inner_index_second = self.inner_header_indexes()
        self.header_names, left = self.header_and_left(inner_index_first, inner_index_second)

        # file can be big so I am considering data partially
        # when I consider one part from first file, I iterate via all parts of second file
        for first_file in pd.read_csv(self.filename1, chunksize=self.BIG_NUMBER):
            data1 = first_file.iloc[:, first_index].values
            data1_set = set(first_file.iloc[:, first_index].values)
            for second_file in pd.read_csv(self.filename2, chunksize=self.BIG_NUMBER):
                data2_set = set(second_file.iloc[:, second_index].values)

                # this set includes all data from column name that is present in both files in this column
                inner_data_set = data1_set & data2_set
                data1_set.clear()
                data2_set.clear()

                # in data1 and data2 there are values from "the column"
                # I used sets to accelerate this task, lists are needed due to need of order in the next part
                data2 = second_file.iloc[:, second_index].values


                # if join type is not inner we are doing the same for both cases, leftjoin and rightjoin due to possible swap in 76 line
                # new entries created by joining rows in first and second file are saved to array (lower explanation extended)
                if self.join_type != "inner":
                    ll = len(left)
                    # we iterate through all the values from one file and if row is not in second file we fill missing columns with "None"
                    for i,element in enumerate(first_file.iloc[:, 1:].values):
                        el = element[first_index-1]
                        if el not in inner_data_set:
                            data11 = ["None" for _ in range(ll)]
                            new_data = np.concatenate((element, data11))
                            data.append(new_data)
                        else:
                            # if the column is common if both files we concatenate data from first and second file
                            for ind, f2 in enumerate(data2):
                                if f2 == el:
                                    j = ind
                                    break
                            np.delete(data1, i)
                            row2 = second_file.iloc[j, left].values
                            new_data = np.concatenate((element, row2))
                            data.append(new_data)
                else:
                    # if join type is inner we interate via common inner values, find indexes of rows where are that values in
                    # files and join rows
                    i = -1
                    j = -1
                    for element in inner_data_set:
                        for ind, f1 in enumerate(data1):
                            if f1 == element:
                                i = ind
                                break
                        for ind, f2 in enumerate(data2):
                            if f2 == element:
                                j = ind
                                break
                        np.delete(data1, i)

                        row1 = first_file.iloc[i, 1:].values
                        row2 = second_file.iloc[j, left].values

                        new_data = np.concatenate((row1, row2))
                        data.append(new_data)
                del data2
            del data1

            # every entry of new data is saved in array, if the length of array is bigger than specified number, the memory becomes
            # free by saving data to new file in computer
            if len(data) > self.BIG_NUMBER:
                df = pd.DataFrame(data=data, columns=self.header_names)
                file = "help_file" + str(nr) + ".csv"
                df.to_csv(file)
                arr.append(file)
                nr += 1
                data.clear()
        if len(data) != 0:
            df = pd.DataFrame(data=data, columns=self.header_names)
            file="help_file" + str(nr) + ".csv"
            df.to_csv(file)
            arr.append(file)
            data.clear()
        self.join_files(arr)
        return "merged_files.csv"


if __name__ == '__main__':
    jcsv = Join_csv(argv[1], argv[2], argv[3], argv[4])
    jcsv.main_function()