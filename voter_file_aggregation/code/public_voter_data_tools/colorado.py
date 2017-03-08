from util import *

def file_reader(fil):
    return file_reader_simple_csv(fil)

def line_reader(row):

    address = " ".join(row[11:18])
    if len(row[17]):
        address += ", " + " ".join(row[18:20])
    city = row[21]

    return [row[0], row[4], row[5], row[3], row[29], row[30], "", "", "", "", "", "", "", "",
            row[34], address, city, row[23], row[2], '','']
