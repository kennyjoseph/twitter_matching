from util import *
import os
from collections import defaultdict
county_keys = {}
voters = defaultdict(set)



def file_reader(fil):
    global county_keys
    global voters

    print 'getting michigan voting records, gimme a minute'
    f = open(os.path.join(os.path.dirname(fil),"../countycd.lst"))
    for line in f:
        county_keys[line[0:2]] = line[2:(len(line)-2)].strip()

    f = open(os.path.join(os.path.dirname(fil),"../entire_state_h.lst"))
    for line in f:
        voterid = ext(line, 0, 13).strip()
        elec = ext(line, 25, 13).strip()
        if elec in ['102000017', '102000638', '102000648', '102000665']:
            voters[voterid].add(elec)

    return open(fil)


# function to extract fwf
def ext(line, init, length):
    return(line[init:(init+length)])


def line_reader(line):
    voterid = ext(line, 448, 13).strip()
    votes = voters.get(voterid,set())
    turnout2008 = 1 if '102000017' in votes else 0
    turnout2010 = 1 if '102000638' in votes else 0
    turnout2012 = 1 if '102000648' in votes else 0
    turnout2014 = 1 if '102000665' in votes else 0
    address = ext(line, 92, 7).strip() + ' ' + ext(line, 101, 2).strip() + \
        ' ' + ext(line, 105, 30).strip() + ' ' + ext(line, 135, 6).strip() + \
        ' ' + ext(line, 141, 2).strip()
    county = ext(line, 461, 2)
    county = county_keys[county]
    return [voterid, ext(line, 35, 20).strip(), ext(line, 55, 20).strip(),
        ext(line, 0, 35).strip(), ext(line, 78, 4).strip(), ext(line, 82, 1),
        turnout2008, turnout2010, turnout2012, turnout2014,
        "", "", "", "", "", address, ext(line, 156, 35).strip(), ext(line, 193, 5),county,'','']
