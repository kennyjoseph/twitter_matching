from util import *


def file_reader(fil):
    return file_reader_simple_csv(fil)


def line_reader(row):
    address = " ".join([row[20], row[21], row[22], row[23]])
    if len(row[24]):
        address += ", " + row[24]
    return [row[34], row[12], row[13], row[14], row[18], row[16],
            "", "", "", "", "", "", "", "", row[17], address, row[25], row[27], "Clark", "", ""]
