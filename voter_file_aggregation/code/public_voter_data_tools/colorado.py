from util import *

party_map = {
    "ACN":"O",
    "DEM":"D",
    "GRN":"G",
    "LBR":"L",
    "REP":"R",
    "UAF":"U",
    "UNI":"O",
}
def file_reader(fil):
    return file_reader_simple_csv(fil)

def line_reader(row):

    address = " ".join(row[11:18])
    if len(row[17]):
        address += ", " + " ".join(row[18:20])
    city = row[21]

    gender = row[30]
    if gender == 'Male':
        gender = "M"
    elif gender == 'Female':
        gender = "F"
    elif gender == "Unknown":
        gender = "U"

    party = get_party(row[34],party_map)
    return [row[0], row[4], row[5], row[3], row[29], gender , "", "", "", "", "", "", "", "",
            party, address, city, row[23], row[2], '','']
