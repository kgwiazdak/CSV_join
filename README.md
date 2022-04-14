# CSV_join

### What it does
A program that merges 2 large csv files into one. The program takes 2 paths to the files, the name of the column against which it merges the files and the join_type. 
### How to run it <br>
join.py filename1 filename2 column_name join_type
<br>Allowed join_type types are inner, leftjoin and rightjoin.

### Technologies I used
The most important libraries I used were numpy and pandas

### Challenges I ran into
Because the files can be very large I used the chunksize option in pandas.read_csv. The merged data was not stored in one place which would overflow the RAM. Above a certain number the data is written to help-files, which are automatically deleted at the end of the program. In order not to overflow the memory also sets and arrays were deleted in several places in the program. 

<br>Finding common data in files make me to consider several ways. The problem of this issue is based on lack of knowledge how much data from the first and second file is common. The first way considered was based on a naive interaction after both arrays which would provide O(n^2) complexity. I then considered merging the files and sorting the records, which would provide O(nlogn) time. In the end I decided to use sets, which made finding common records and then performing the "in" operation fast. Unfortunately, the memory complexity suffered. This solution works best especially when there are little common data.

