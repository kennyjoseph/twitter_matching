from util import *
from collections import defaultdict
voters = defaultdict(set)

def file_reader(fil):
    global voters
    history_file = fil.replace("voting_data","../voting_history")
    print 'HISTORY FILE WASHINGTON: ', history_file
    reader = csv.reader(open(history_file), delimiter="\t")
    for row in reader:
        elec = row[2]
        voterid = row[1]
        voters[voterid].add(elec)

    return file_reader_simple_csv(fil,"\t")


def line_reader(row):
    county = county_keys[row[20]]
    voterid = row[0]
    votes = voters.get(voterid,set())
    turnout2008 = 1 if '11/04/2008' in votes else 0
    turnout2010 = 1 if '11/02/2010' in votes else 0
    turnout2012 = 1 if '11/06/2012' in votes else 0
    turnout2014 = 1 if '11/04/2014' in votes else 0
    address = " ".join([row[9],row[10],row[14],row[11],row[15],row[12]])
    if len(row[16].strip()):
        address += ", " + row[13] + " " + row[16]
    return [voterid, row[3], row[4], row[5], row[7], row[8],
            turnout2008, turnout2010, turnout2012, turnout2014,
            "", "", "", "", "", address, row[17], row[19], county, '', '']



county_keys = {}
county_keys["AD"] = "Adams"
county_keys["AS"] = "Asotin"
county_keys["BE"] = "Benton"
county_keys["CH"] = "Chelan"
county_keys["CM"] = "Clallam"
county_keys["CR"] = "Clark"
county_keys["CU"] = "Columbia"
county_keys["CZ"] = "Cowlitz"
county_keys["DG"] = "Douglas"
county_keys["FE"] = "Ferry"
county_keys["FR"] = "Franklin"
county_keys["GA"] = "Garfield"
county_keys["GR"] = "Grant"
county_keys["GY"] = "Grays Harbor"
county_keys["IS"] = "Island"
county_keys["JE"] = "Jefferson"
county_keys["KI"] = "King"
county_keys["KP"] = "Kitsap"
county_keys["KS"] = "Kittitas"
county_keys["KT"] = "Klickitat"
county_keys["LE"] = "Lewis"
county_keys["LI"] = "Lincoln"
county_keys["MA"] = "Mason"
county_keys["OK"] = "Okanogan"
county_keys["PA"] = "Pacific"
county_keys["PE"] = "Pend Oreille"
county_keys["PI"] = "Pierce"
county_keys["SJ"] = "San Juan"
county_keys["SK"] = "Skagit"
county_keys["SM"] = "Skamania"
county_keys["SN"] = "Snohomish"
county_keys["SP"] = "Spokane"
county_keys["ST"] = "Stevens"
county_keys["TH"] = "Thurston"
county_keys["WK"] = "Wahkiakum"
county_keys["WL"] = "Walla Walla"
county_keys["WM"] = "Whatcom"
county_keys["WT"] = "Whitman"
county_keys["YA"] = "Yakima"
