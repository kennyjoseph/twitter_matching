from util import *

party_map = {
    "D": "D",
    "R" : "R"
}

def file_reader(fil):
    return file_reader_simple_csv(fil)


def line_reader(row):
    address = row[11]
    if len(row[12].strip()):
        address += ", " + row[12]

    # doesn't fit the established pattern, so do it a different way
    birth_year = row[7][:4]
    party = get_party(row[10],party_map)
    return [row[0], row[4], row[5], row[3], birth_year, "",
        1 if row[70] is 'X' else 0, 1 if row[80] is 'X' else 0,
        1 if row[85] is 'X' else 0, 1 if row[91] is 'X' else 0,
        row[68], row[77], row[84], row[90], party,
        address, row[13], row[15],county_keys[row[1]],'','']


county_keys = {}

county_keys["01"] ="ADAMS"
county_keys["02"] ="ALLEN"
county_keys["03"] ="ASHLAND"
county_keys["04"] ="ASHTABULA"
county_keys["05"] ="ATHENS"
county_keys["06"] ="AUGLAIZE"
county_keys["07"] ="BELMONT"
county_keys["08"] ="BROWN"
county_keys["09"] ="BUTLER"
county_keys["10"] = "CARROLL"
county_keys["11"] = "CHAMPAIGN"
county_keys["12"] = "CLARK"
county_keys["13"] = "CLERMONT"
county_keys["14"] = "CLINTON"
county_keys["15"] = "COLUMBIANA"
county_keys["16"] = "COSHOCTON"
county_keys["17"] = "CRAWFORD"
county_keys["18"] = "CUYAHOGA"
county_keys["19"] = "DARKE"
county_keys["20"] = "DEFIANCE"
county_keys["21"] = "DELAWARE"
county_keys["22"] = "ERIE"
county_keys["23"] = "FAIRFIELD"
county_keys["24"] = "FAYETTE"
county_keys["25"] = "FRANKLIN"
county_keys["26"] = "FULTON"
county_keys["27"] = "GALLIA"
county_keys["28"] = "GEAUGA"
county_keys["29"] = "GREENE"
county_keys["30"] = "GUERNSEY"
county_keys["31"] = "HAMILTON"
county_keys["32"] = "HANCOCK"
county_keys["33"] = "HARDIN"
county_keys["34"] = "HARRISON"
county_keys["35"] = "HENRY"
county_keys["36"] = "HIGHLAND"
county_keys["37"] = "HOCKING"
county_keys["38"] = "HOLMES"
county_keys["39"] = "HURON"
county_keys["40"] = "JACKSON"
county_keys["41"] = "JEFFERSON"
county_keys["42"] = "KNOX"
county_keys["43"] = "LAKE"
county_keys["44"] = "LAWRENCE"
county_keys["45"] = "LICKING"
county_keys["46"] = "LOGAN"
county_keys["47"] = "LORAIN"
county_keys["48"] = "LUCAS"
county_keys["49"] = "MADISON"
county_keys["50"] = "MAHONING"
county_keys["51"] = "MARION"
county_keys["52"] = "MEDINA"
county_keys["53"] = "MEIGS"
county_keys["54"] = "MERCER"
county_keys["55"] = "MIAMI"
county_keys["56"] = "MONROE"
county_keys["57"] = "MONTGOMERY"
county_keys["58"] = "MORGAN"
county_keys["59"] = "MORROW"
county_keys["60"] = "MUSKINGUM"
county_keys["61"] = "NOBLE"
county_keys["62"] = "OTTAWA"
county_keys["63"] = "PAULDING"
county_keys["64"] = "PERRY"
county_keys["65"] = "PICKAWAY"
county_keys["66"] = "PIKE"
county_keys["67"] = "PORTAGE"
county_keys["68"] = "PREBLE"
county_keys["69"] = "PUTNAM"
county_keys["70"] = "RICHLAND"
county_keys["71"] = "ROSS"
county_keys["72"] = "SANDUSKY"
county_keys["73"] = "SCIOTO"
county_keys["74"] = "SENECA"
county_keys["75"] = "SHELBY"
county_keys["76"] = "STARK"
county_keys["77"] = "SUMMIT"
county_keys["78"] = "TRUMBULL"
county_keys["79"] = "TUSCARAWAS"
county_keys["80"] = "UNION"
county_keys["81"] = "VANWERT"
county_keys["82"] = "VINTON"
county_keys["83"] = "WARREN"
county_keys["84"] = "WASHINGTON"
county_keys["85"] = "WAYNE"
county_keys["86"] = "WILLIAMS"
county_keys["87"] = "WOOD"
county_keys["88"] = "WYANDOT"
