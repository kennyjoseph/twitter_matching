
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
    "FL" : "florida/20151130_voters",
    "MI" : "michigan/voting_records",
    "NC" : "north_carolina_wake_county_only/voting_records",
    "OK" : "oklahoma/voting_records",
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
        return 'L'
    if row['reg_party_grn'] == 1:
        return 'G'
    if row['reg_party_ind'] == 1:
        return 'I'
    return ''
