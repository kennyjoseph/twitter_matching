from util import *

def file_reader(fil):
    f = file_reader_simple_csv(fil,"|")
    f.next()
    return f


def line_reader(row):
    address = " ".join([row[7], row[8]])
    if len(row[9]):
        address += ", " + row[9]


    county = towns[row[12]]
    return [row[0], row[3], row[4], row[2], row[37], row[34],
        "", "", "", "", "", "", "", "",row[30], address, row[12], row[10],
        county, '','']


towns = {}
towns["BARRINGTON"] = "BRISTOL"
towns["BRISTOL"] = "BRISTOL"
towns["BURRILLVILLE"] = "PROVIDENCE"
towns["CENTRAL FALLS"] = "PROVIDENCE"
towns["CHARLESTOWN"] = "WASHINGTON"
towns["COVENTRY"] = "KENT"
towns["CRANSTON"] = "PROVIDENCE"
towns["CUMBERLAND"] = "PROVIDENCE"
towns["EAST GREENWICH"] = "KENT"
towns["EAST PROVIDENCE"] = "PROVIDENCE"
towns["EXETER"] = "WASHINGTON"
towns["FOSTER"] = "PROVIDENCE"
towns["GLOCESTER"] = "PROVIDENCE"
towns["HOPKINTON"] = "WASHINGTON"
towns["JAMESTOWN"] = "NEWPORT"
towns["JOHNSTON"] = "PROVIDENCE"
towns["LINCOLN"] = "PROVIDENCE"
towns["LITTLE COMPTON"] = "NEWPORT"
towns["MIDDLETOWN"] = "NEWPORT"
towns["NARRAGANSETT"] = "WASHINGTON"
towns["NEW SHOREHAM"] = "WASHINGTON"
towns["NEWPORT"] = "NEWPORT"
towns["NORTH KINGSTOWN"] = "WASHINGTON"
towns["NORTH PROVIDENCE"] = "PROVIDENCE"
towns["NORTH SMITHFIELD"] = "PROVIDENCE"
towns["PAWTUCKET"] = "PROVIDENCE"
towns["PORTSMOUTH"] = "NEWPORT"
towns["PROVIDENCE"] = "PROVIDENCE"
towns["RICHMOND"] = "WASHINGTON"
towns["SCITUATE"] = "PROVIDENCE"
towns["SMITHFIELD"] = "PROVIDENCE"
towns["SOUTH KINGSTOWN"] = "WASHINGTON"
towns["TIVERTON"] = "NEWPORT"
towns["WARREN"] = "BRISTOL"
towns["WARWICK"] = "KENT"
towns["WEST GREENWICH"] = "KENT"
towns["WEST WARWICK"] = "KENT"
towns["WESTERLY"] = "WASHINGTON"
towns["WOONSOCKET"] = "PROVIDENCE"