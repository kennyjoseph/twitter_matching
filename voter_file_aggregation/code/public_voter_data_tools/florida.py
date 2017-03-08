from util import *
from collections import defaultdict
import os

voter_history = defaultdict(set)
curr_county = "COUNTY"

def file_reader(fil):
    global curr_county
    global voter_history
    voter_history = defaultdict(set)
    print 'FL reading voter history from: ',
    history_file = fil.replace("20151130_voters","VH20151031").replace("20151217","H_20151109")
    print os.path.basename(history_file)

    curr_county = county_keys[os.path.basename(fil)[0:3]]
    reader = csv.reader(open(history_file), delimiter="\t")
    for row in reader:
        if row[4] is not 'N':
            voter_history[row[1]].add(row[2])

    return file_reader_simple_csv(fil,"\t")


def line_reader(row):
    votes = list(voter_history.get(row[1],[]))
    turnout2008 = 1 if '11/04/2008' in votes else 0
    turnout2010 = 1 if '11/02/2010' in votes else 0
    turnout2012 = 1 if '11/06/2012' in votes else 0
    turnout2014 = 1 if '11/04/2014' in votes else 0
    address = row[7].strip()
    if len(row[8].strip()):
        address += ", " + row[8].strip()
    return [row[1], row[4], row[5], row[2], row[21], row[19],
            turnout2008, turnout2010, turnout2012, turnout2014,
            "", "", "", "", row[23].strip(), address,row[9].strip(), row[11],  curr_county, row[20],'']

county_keys = {}
county_keys["ALA"] = "Alachua"
county_keys["BAK"] = "Baker"
county_keys["BAY"] = "Bay"
county_keys["BRA"] = "Bradford"
county_keys["BRE"] = "Brevard"
county_keys["BRO"] = "Broward"
county_keys["CAL"] = "Calhoun"
county_keys["CHA"] = "Charlotte"
county_keys["CIT"] = "Citrus"
county_keys["CLA"] = "Clay"
county_keys["CLL"] = "Collier"
county_keys["CLM"] = "Columbia"
county_keys["DAD"] = "Miami-Dade"
county_keys["DES"] = "Desoto"
county_keys["DIX"] = "Dixie"
county_keys["DUV"] = "Duval"
county_keys["ESC"] = "Escambia"
county_keys["FLA"] = "Flagler"
county_keys["FRA"] = "Franklin"
county_keys["GAD"] = "Gadsden"
county_keys["GIL"] = "Gilchrist"
county_keys["GLA"] = "Glades"
county_keys["GUL"] = "Gulf"
county_keys["HAM"] = "Hamilton"
county_keys["HAR"] = "Hardee"
county_keys["HEN"] = "Hendry"
county_keys["HER"] = "Hernando"
county_keys["HIG"] = "Highlands"
county_keys["HIL"] = "Hillsborough"
county_keys["HOL"] = "Holmes"
county_keys["IND"] = "Indian River"
county_keys["JAC"] = "Jackson"
county_keys["JEF"] = "Jefferson"
county_keys["LAF"] = "Lafayette"
county_keys["LAK"] = "Lake"
county_keys["LEE"] = "Lee"
county_keys["LEO"] = "Leon"
county_keys["LEV"] = "Levy"
county_keys["LIB"] = "Liberty"
county_keys["MAD"] = "Madison"
county_keys["MAN"] = "Manatee"
county_keys["MRN"] = "Marion"
county_keys["MRT"] = "Martin"
county_keys["MON"] = "Monroe"
county_keys["NAS"] = "Nassau"
county_keys["OKA"] = "Okaloosa"
county_keys["OKE"] = "Okeechobee"
county_keys["ORA"] = "Orange"
county_keys["OSC"] = "Osceola"
county_keys["PAL"] = "Palm Beach"
county_keys["PAS"] = "Pasco"
county_keys["PIN"] = "Pinellas"
county_keys["POL"] = "Polk"
county_keys["PUT"] = "Putnam"
county_keys["SAN"] = "Santa Rosa"
county_keys["SAR"] = "Sarasota"
county_keys["SEM"] = "Seminole"
county_keys["STJ"] = "St. Johns"
county_keys["STL"] = "St. Lucie"
county_keys["SUM"] = "Sumter"
county_keys["SUW"] = "Suwannee"
county_keys["TAY"] = "Taylor"
county_keys["UNI"] = "Union"
county_keys["VOL"] = "Volusia"
county_keys["WAK"] = "Wakulla"
county_keys["WAL"] = "Walton"
county_keys["WAS"] = "Washington"