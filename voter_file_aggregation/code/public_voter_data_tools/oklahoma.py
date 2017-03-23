from util import *

party_map = {
    "DEM":"D",
    "IND":"I",
    "REP": "R"
}


def file_reader(fil):
    return file_reader_simple_csv(fil)


def line_reader(row):
    county = county_keys[row[0][0:2]]
    address = " ".join([row[8], row[9], row[10], row[11]])
    if len(row[12]):
        address += ", " + row[12]

    votes = row[22:len(row)]
    party = get_party(row[6], party_map)
    return [row[5], row[2], row[3], row[1], row[15], '',
            1 if '20081104' in votes else 0, 1 if '20101102' in votes else 0,
            1 if '20121106' in votes else 0, '',
            '', '', '', '', party, address,row[13], row[14],county,'','']


county_keys = {}

county_keys["01"] = "Adair"
county_keys["02"] = "Alfalfa"
county_keys["03"] = "Atoka"
county_keys["04"] = "Beaver"
county_keys["05"] = "Beckham"
county_keys["06"] = "Blaine"
county_keys["07"] = "Bryan"
county_keys["08"] = "Caddo"
county_keys["09"] = "Canadian"
county_keys["10"] = "Carter"
county_keys["11"] = "Cherokee"
county_keys["12"] = "Choctaw"
county_keys["13"] = "Cimarron"
county_keys["14"] = "Cleveland"
county_keys["15"] = "Coal"
county_keys["16"] = "Comanche"
county_keys["17"] = "Cotton"
county_keys["18"] = "Craig"
county_keys["19"] = "Creek"
county_keys["20"] = "Custer"
county_keys["21"] = "Delaware"
county_keys["22"] = "Dewey"
county_keys["23"] = "Ellis"
county_keys["24"] = "Garfield"
county_keys["25"] = "Garvin"
county_keys["26"] = "Grady"
county_keys["27"] = "Grant"
county_keys["28"] = "Greer"
county_keys["29"] = "Harmon"
county_keys["30"] = "Harper"
county_keys["31"] = "Haskell"
county_keys["32"] = "Hughes"
county_keys["33"] = "Jackson"
county_keys["34"] = "Jefferson"
county_keys["35"] = "Johnston"
county_keys["36"] = "Kay"
county_keys["37"] = "Kingfisher"
county_keys["38"] = "Kiowa"
county_keys["39"] = "Latimer"
county_keys["40"] = "LeFlore"
county_keys["41"] = "Lincoln"
county_keys["42"] = "Logan"
county_keys["43"] = "Love"
county_keys["44"] = "McClain"
county_keys["45"] = "McCurtain"
county_keys["46"] = "McIntosh"
county_keys["47"] = "Major"
county_keys["48"] = "Marshall"
county_keys["49"] = "Mayes"
county_keys["50"] = "Murray"
county_keys["51"] = "Muskogee"
county_keys["52"] = "Noble"
county_keys["53"] = "Nowata"
county_keys["54"] = "Okfuskee"
county_keys["55"] = "Oklahoma"
county_keys["56"] = "Okmulgee"
county_keys["57"] = "Osage"
county_keys["58"] = "Ottawa"
county_keys["59"] = "Pawnee"
county_keys["60"] = "Payne"
county_keys["61"] = "Pittsburg"
county_keys["62"] = "Pontotoc"
county_keys["63"] = "Pottawatomie"
county_keys["64"] = "Pushmataha"
county_keys["65"] = "Roger Mills"
county_keys["66"] = "Rogers"
county_keys["67"] = "Seminole"
county_keys["68"] = "Sequoyah"
county_keys["69"] = "Stephens"
county_keys["70"] = "Texas"
county_keys["71"] = "Tillman"
county_keys["72"] = "Tulsa"
county_keys["73"] = "Wagoner"
county_keys["74"] = "Washington"
county_keys["75"] = "Washita"
county_keys["76"] = "Woods"
county_keys["77"] = "Woodwar"