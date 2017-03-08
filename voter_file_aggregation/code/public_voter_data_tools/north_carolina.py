from util import *
from collections import defaultdict
import os
voters = defaultdict(set)

def file_reader(fil):
    reader = csv.reader(open(os.path.join(os.path.dirname(fil),"../ncvhis92.txt")),delimiter="\t")
    vars = reader.next()
    for row in reader:
        voterid = row[10]
        elec = row[3]
        if elec in ['11/04/2008', '11/02/2010', '11/06/2012','11/04/2014']:
            voters[voterid].add(elec)
    return file_reader_simple_csv(fil,"\t",is_unicode=True)


def line_reader(row):
    voterid = row[67]
    votes = voters.get(voterid,[])
    county = row[1]
    turnout2008 = 1 if '11/04/2008' in votes else 0
    turnout2010 = 1 if '11/02/2010' in votes else 0
    turnout2012 = 1 if '11/06/2012' in votes else 0
    turnout2014 = 1 if '11/04/2014' in votes else 0
    address = row[13].strip()
    return [voterid, row[10].strip(), row[11].strip(), row[9].strip(),
            str(2013 - int(row[29])),
            row[28], turnout2008, turnout2010, turnout2012, turnout2014,
            "", "", "", "", row[27], address, row[14].strip(),row[16],county,
            row[25].strip(), row[26].strip()]
