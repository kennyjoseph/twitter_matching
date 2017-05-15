
import colorado
import connecticut
import delaware
import florida
import north_carolina
import oklahoma
import rhode_island
import ohio
import washington
import michigan
import nevada

file_readers = {
    "CO" : colorado.file_reader,
    "CT" : connecticut.file_reader,
    "DE" : delaware.file_reader,
    "FL" : florida.file_reader,
    "MI" : michigan.file_reader,
    "NC" : north_carolina.file_reader,
    "OK" : oklahoma.file_reader,
    "RI" : rhode_island.file_reader,
    "OH" : ohio.file_reader,
    "WA" : washington.file_reader,
    "NV" : nevada.file_reader
}


line_readers = {
    "CO" : colorado.line_reader,
    "CT" : connecticut.line_reader,
    "DE" : delaware.line_reader,
    "FL" : florida.line_reader,
    "MI" : michigan.line_reader,
    "NC" : north_carolina.line_reader,
    "OK" : oklahoma.line_reader,
    "RI" : rhode_island.line_reader,
    "OH" : ohio.line_reader,
    "WA" : washington.line_reader,
    "NV" : nevada.line_reader
}

state_names = {
    "CO" : "colorado",
    "CT" : "connecticut",
    "DE" : "delaware",
    "FL" : "florida",
    "MI" : "michigan",
    "NC" : "north_carolina",
    "OK" : "oklahoma",
    "RI" : "rhode_island",
    "OH" : "ohio",
    "WA" : "washington",
    "NV" : "nevada"
}

state_to_voter_data_file = {
    "CO" : "colorado",
    "CT" : "connecticut",
    "DE" : "delaware",
    "FL" : "florida/voting_records",
    "MI" : "michigan/voting_records",
    "NC" : "north_carolina/voting_records",
    "OK" : "oklahoma",
    "RI" : "rhode_island",
    "OH" : "ohio",
    "WA" : "washington/voting_records",
    "NV" : "nevada"
}


def get_party_affiliation_dnc_data(row):
    if row['reg_party_dem'] == 1:
        return 'D'
    if row['reg_party_rep'] == 1:
        return 'R'
    if row['reg_party_oth'] == 1:
        return 'O'
    if row['reg_party_lib'] == 1:
        return 'O'
    if row['reg_party_grn'] == 1:
        return 'O'
    if row['reg_party_ind'] == 1:
        return 'I'
    return ''


def get_targetsmart_party_affil(x):
    if x == 'Conservative':
        return 'O'
    if x == 'Democrat':
        return 'D'
    if x == 'Green':
        return 'O'
    if x == 'Independent':
        return 'I'
    if x == 'Libertarian':
        return 'O'
    if x == 'No Party':
        return 'N'
    if x == 'Other':
        return 'O'
    if x == 'Republican':
        return 'R'
    if x == 'Unaffiliated':
        return 'N'
    if x == 'Unknown':
        return ''
    if x == 'Working Fam':
        return 'O'