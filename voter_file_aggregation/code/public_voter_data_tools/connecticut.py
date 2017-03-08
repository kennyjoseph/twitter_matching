import csv
import glob


def file_reader(fil):
    return open(fil)

def line_reader(line):

    try:
        county = towns[line[236:254].strip()]
    except:
        return None
    votes = line[470:len(line)]
    votes = votes.split(',')
    turnout2008 = 1 if '11/04/2008' in votes else 0
    turnout2010 = 1 if '11/02/2010' in votes else 0
    turnout2012 = 1 if '11/06/2012' in votes else 0
    turnout2014 = 1 if '11/04/2014' in votes else 0
    address = line[179:185].strip() + ' ' + line[195:235].strip()
    city = line[236:254].strip()
    values = [line[4:13], line[50:70].strip(), line[71:86].strip(),
        line[14:49].strip(), line[423:433], line[457:458],
        turnout2008, turnout2010, turnout2012, turnout2014,
        "NA", "NA", "NA", "NA", line[445:450].strip(), address,city,
        line[258:263].strip(),county, '','']
    return values



# list of towns and counties
# NOTE: voter file does not contain county names, so we identify these by
# town names
towns = {}

for t in ["Bethel", "Bridgeport", "Brookfield", "Danbury", "Darien", "Easton", "Fairfield",
          "Greenwich", "Monroe", "New Canaan", "New Fairfield", "Newtown", "Norwalk",
          "Shelton", "Sherman", "Stamford", "Stratford", "Redding", "Ridgefield", "Trumbull",
          "Weston", "Westport", "Wilton"]:
    towns[t] = 'Fairfield'

for x in ["Avon", "Berlin", "Bloomfield", "Bristol", "Burlington", "Canton", "East Granby", "East",
          "Hartford", "East Windsor", "Enfield", "Farmington", "Glastonbury", "Granby", "Hartford",
          "Hartland", "Manchester", "Marlborough", "New Britain", "Newington", "Plainville", "Rocky  Hill",
          "Simsbury", "Southington", "South Windsor", "Suffield", "West Hartford", "Wethersfield", "Windsor",
          "Windsor Locks"]:
    towns[x] = 'Hartford'

for x in ["Barkhamsted", "Bethlehem", "Bridgewater", "Canaan", "Colebrook", "Cornwall", "Goshen", "Harwinton",
          "Kent", "Litchfield", "Morris", "New Hartford", "New Milford", "Norfolk", "North Canaan", "Plymouth",
          "Roxbury", "Salisbury", "Sharon", "Thomaston", "Torrington", "Warren", "Washington", "Watertown",
          "Winchester", "Woodbury"]:
    towns[x] = 'Litchfield'

for x in ["Chester", "Clinton", "Cromwell", "Deep River", "Durham", "East Haddam", "East Hampton", "Essex",
          "Haddam", "Killingworth", "Middlefield", "Middletown", "Old Saybrook", "Portland", "Westbrook"]:
    towns[x] = 'Middlesex'

for x in ["Ansonia", "Beacon Falls", "Bethany", "Branford", "Cheshire", "Derby", "East Haven", "Guilford",
          "Hamden", "Madison", "Meriden", "Middlebury", "Milford", "Naugatuck", "New Haven", "North Branford",
          "North Haven", "Orange", "Oxford", "Prospect", "Seymour", "Southbury", "Wallingford", "Waterbury",
          "West Haven", "Wolcott", "Woodbridge"]:
    towns[x] = 'New Haven'

for x in ["Bozrah", "Colchester", "East Lyme", "Franklin", "Griswold", "Groton", "Lebanon", "Ledyard",
          "Lisbon", "Lyme", "Montville", "New London", "North Stonington", "Norwich", "Old Lyme", "Preston",
          "Salem", "Sprague", "Stonington", "Voluntown", "Waterford"]:
    towns[x] = 'New London'

for x in ["Andover", "Bolton", "Columbia", "Coventry", "Ellington", "Hebron", "Mansfield", "Somers",
          "Stafford", "Tolland", "Union", "Vernon", "Willington"
]:
    towns[x] = 'Tolland'

for x in ["Ashford", "Brooklyn", "Canterbury", "Chaplin", "Eastford", "Hampton", "Killingly", "Plainfield",
          "Pomfret", "Putnam", "Scotland", "Sterling", "Thompson", "Windham", "Woodstock"]:
    towns[x] = 'Windham'