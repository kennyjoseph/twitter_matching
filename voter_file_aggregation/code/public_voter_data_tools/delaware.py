from util import *

def file_reader(fil):
    return file_reader_simple_csv(fil)


def line_reader(row):
    address = " ".join([row[6],row[8]])
    if len(row[7]):
        address += ", " + row[7]
    city = row[10]
    votes = row[27:32]
    if row[12] == 'S':
        county = 'Sussex'
    if row[12] == 'K':
        county = 'Kent'
    if row[12] == 'N':
        county = 'New Castle'
    return [row[0], row[2], row[3], row[1], row[5], "",
        1 if '2008' in votes else 0, 1 if '2010' in votes else 0,
        1 if '2012' in votes else 0, 1 if '2014' in votes else 0,
        "", "", "", "", row[20], address,city, row[11],county,'','']
