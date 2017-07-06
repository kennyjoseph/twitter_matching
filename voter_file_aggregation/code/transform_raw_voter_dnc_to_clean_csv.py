"""
This file parses raw data from voter registration files available at the set of sites
[state_name]voters.info (e.g. michiganvoters.info)

It basically steals the processing code from Pablo Barbera's awesome github repo for processing these files
(https://github.com/pablobarbera/voter-files) and puts in a slightly more reusable, functional format

To add a new state, go get the data, make a new [state_name].py file and take a look at the examples, all you
should really have to do is some preprocessing of history files and then a function to take a row and transform it into
the necessary output structure. Most of this code should be available in Pablo's library, just be careful you're working
on the same file as he is.

"""

import os
import glob
from public_voter_data_tools import *
from util import *
import sys
import pandas as pd
from collections import defaultdict
from copy import copy
from multiprocessing import Pool, cpu_count
from functools import partial

import time

DATA_DIR = "../data/"
VOTER_FILE_DATA_DIR = os.path.join(DATA_DIR, "raw_voter", "public_voter_files")
DNC_DATA_DIR = os.path.join(DATA_DIR, "raw_voter","dnc_voter_files")
OUTPUT_DIR = os.path.join(DATA_DIR,"cleaned_voter_files")
TODAYS_DATE = time.strftime("%d/%m/%Y")

STATE_COUNTY_FILE = os.path.join(DATA_DIR, "used_by_model", "per_county_state.csv")

COUNTY_FILE = os.path.join(DATA_DIR, "used_by_model", "national_county.txt")

county_fips = pd.read_csv(COUNTY_FILE, header=None, names=['state_code', 'state_fips',
                                                          'reg_address_countyfips', 'county', 'extra'])
county_fips = county_fips[['state_code', 'reg_address_countyfips', 'county']]
county_fips.county = county_fips.county.str.replace(" County", "")

ALL_STATES = ['CO','OH','MI','NC','WA','CT','RI','OK','DE','FL']

STATE_TO_REGISTRY_DATE = {
     'NC': "2017-03-18",
     'OH': "2017-03-18",
     'MI': "2016-09-01",
     'CO': "2016-12-01",
     'WA': "2017-02-01",
     'CT': "2017-02-03",
     'RI': "2015-01-01",
     'OK': "2016-12-05",
     'DE': "2015-05-21",
     'FL': "2017-02-28"
}

all_var_names = ["voter_id", "first_name", "middle_name",
                 "last_name", "birth_date", "gender", "turnout_2008", "turnout_2010",
                 "turnout_2012", "turnout_2014", "party_affiliation2008",
                 "party_affiliation2010", "party_affiliation2012", "party_affiliation2014",
                 "party_affiliation", "address", "city", "zipcode", "county", "race", 'ethnicity', 'state',
                 'from','birth_year']

varnames_to_keep =copy(all_var_names)
varnames_to_keep.remove("birth_date")

varnames_to_keep_len = len(varnames_to_keep)

def get_data_for_state_from_voter_file(input_dir, output_filename, state,
                                       get_state_file_reader, get_line_function):
    """
    :param input_dir: directory of input files, should only contain voting records files (i.e. put history
    files somewhere else)
    :param output_filename: output file name, usually just the state name
    :param variable_names: the variables in the output file
    :param varnames_to_keep: only the variables you want to keep (vs all those that should be captured)
    :param state: state name (put into the output file)
    :param get_state_file_reader: This is a function that returns an iterable for each voting file for a given state.
    See the [state_name].py files for examples. This function also should do any necessary preprocessing, i.e. reading
    in voter history files
    :param get_line_function: This is a function that takes in a row of a voting records file and transforms it into
    the necessary structure required for the output files
    :return: a set of counties for this state that are in the voter file
    """
    print "STARTING STATE: ", state, output_filename
    all_counties_in_state = set()
    out = io.open(output_filename.replace(".csv",".tsv"), 'w')
    out.write(tsn(varnames_to_keep))
    n_voters = 0
    # reading file
    for fil in glob.glob(input_dir + "/*"):
        for i, line in enumerate(get_state_file_reader(fil)):
            if i % 200000 == 0:
                print str(i) + '/' + str(n_voters)
            try:
                if not len(line):
                    continue
                values = get_line_function(line)

                if values:
                    # make sure that the state data is returning the correct number of rows
                    assert (len(values) == len(all_var_names) - 3)

                    values.append(state)

                    # do any post processing consistent across all states here
                    dat = dict(zip(all_var_names, values))

                    dat['voter_id'] = state + "_" + STATE_TO_REGISTRY_DATE[state] +"_" + dat['voter_id']

                    # if no birthdate, continue
                    if not len(dat['birth_date']):
                        continue

                    # all of these are either years or dd/mm/yyyy format, so this is safe for most
                    # if it isn't safe, then fix it in the state-specific file (e.g. see ohio)
                    birth_year = dat['birth_date'][-4:]

                    dat['from'] = 'public_'+ state + "_" + STATE_TO_REGISTRY_DATE[state]

                    # Make sure its a valid row
                    if not (len(dat['zipcode']) and
                                len(dat['first_name']) and
                                len(dat['last_name'])):
                        # print 'NOT VALID!!!'
                        # print age <= 100, age, dat['birth_date']
                        # print len(dat['first_name'])
                        # print len(dat['last_name'])
                        # print len(dat['zipcode'])
                        continue

                    dat['party_affiliation'] = dat['party_affiliation'] if len(dat['party_affiliation']) else ''
                    dat['gender'] = dat['gender'] if len(dat['gender']) else ''
                    dat['address'] = dat['address'].replace("\xbd"," 1/2").replace("\xbc"," 1/4")
                    dat['first_name'] = clean_name_text(dat['first_name'])
                    dat['last_name'] = clean_name_text(dat['last_name'])
                    dat['county'] = dat['county'].lower()
                    dat['city'] = dat['city'].lower()

                    # just focus on birth year
                    dat['birth_year'] = birth_year
                    n_voters += 1
                    towrite = [dat[x] for x in varnames_to_keep]

                    try:
                        #tab-separate, remove quote chars
                        output_line = tsn(towrite).replace("\"","")
                        if len(output_line.split("\t")) != varnames_to_keep_len:
                            print 'skipping line, extra tab somewhere'
                        else:
                            out.write(output_line)
                    except UnicodeDecodeError:
                        print 'unicode error for a line'
                        print dat

                    all_counties_in_state.add(dat['county'])
                elif state != 'CT':
                    print 'missed line!'
            except IndexError:
                print 'row failed', line
            except ValueError:
                print 'row failed value error', line
    out.close()
    return set(all_counties_in_state)


def clean_dnc_address_data(data,addr_type):
    data = pd.merge(data, county_fips, left_on=['state_code', addr_type+'_address_countyfips'],
                right_on=['state_code', 'reg_address_countyfips'])
    data = data.rename(columns={"sex": "gender",
                                "ethnicity_code": "race",
                                "ethnicity_subgroup": "ethnicity",
                                addr_type+"_address_street1": "address",
                                addr_type+"_address_city": "city",
                                addr_type+"_address_zip5": "zipcode",
                                addr_type+"_address_state": "state",
                                "personid": "voter_id"
                                })
    data['county'] = data.county.str.lower()
    data['city']= data.city.str.lower()
    return data[varnames_to_keep]

def transform_dnc_data(filename):
    data = pd.read_csv(filename,encoding='utf8')
    data = data.fillna("")
    data['middle_name'] = ''

    data['first_name'] = data.first_name.astype("unicode").apply(clean_name_text)
    data['last_name'] = data.last_name.astype("unicode").apply(clean_name_text)

    data['party_affiliation'] = data.apply(get_party_affiliation_dnc_data, axis=1)
    data["party_affiliation2008"] = ''
    data["party_affiliation2010"] = ''
    data["party_affiliation2012"] = ''
    data["party_affiliation2014"] = ''

    file_id = os.path.basename(filename).replace("voterfile_unique_","").replace(".csv","") +"_2015"
    data['from'] = "dnc_"+file_id

    data['birth_year'] = data.birth_date.str.slice(0, 4)

    # split data into those we should extract reg_address from and those
    # for whom there isn't a reg address so we turn to their mailing address
    reg_address_data = clean_dnc_address_data(data[data.reg_address_city != ""],"reg")
    mailing_address_data = clean_dnc_address_data(data[data.reg_address_city == ""],"mailing")

    data = pd.concat((reg_address_data, mailing_address_data), axis=0)

    data['voter_id'] = data.voter_id.apply(lambda x: file_id +"_" + str(x))
    data['zipcode'] = data.zipcode.apply(lambda x: str(int(x)) if x != '' else x)

    data = data.fillna("")
    return data

def get_data_for_state_from_dnc_data(filename, output_dir):
    print filename
    output_filename = os.path.join(output_dir, os.path.basename(filename))

    data = transform_dnc_data(filename)
    state_county_res = defaultdict(set)
    for r in data[['state', 'county']].drop_duplicates().itertuples():
        state_county_res[r[1]].add(r[2])

    write_file(data,output_filename.replace(".csv",".tsv"))
    return state_county_res


def get_data_voter_file_helper(state):
    full_name = state_names[state]
    file_reader = file_readers[state]
    line_reader = line_readers[state]
    counties = get_data_for_state_from_voter_file(os.path.join(VOTER_FILE_DATA_DIR, state_to_voter_data_file[state]),
                                                             os.path.join(OUTPUT_DIR, full_name+".csv"),
                                                             state,
                                                             file_reader,
                                                             line_reader)
    return state, counties


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print 'USAGE: transform_raw_voter_dnc_to_clean_csv.py [path_to_raw_data_files -> e.g. ../data/]'

    try:
        os.mkdir(OUTPUT_DIR)
    except:
        print 'didnt make output dir, exists already'

    state_to_county_data = defaultdict(set)

    # read in the existing state -> county mappings, so we don't have to rerun everything
    # every time just to generate this file
    if os.path.exists(STATE_COUNTY_FILE):
        state_county_out_fil = open(STATE_COUNTY_FILE)
        for line in state_county_out_fil:
            state, county = line.strip().split("\t")
            state_to_county_data[state].add(county)

    # generate results in parallel for state voter files
    n_cpu = min(len(ALL_STATES), cpu_count()/float(2))

    #get_data_voter_file_helper('WA')
    print 'N CPUS: ', n_cpu
    pool = Pool(int(n_cpu))
    results = pool.map(get_data_voter_file_helper,ALL_STATES)
    for result in results:
         state, counties = result
         state_to_county_data[state] = state_to_county_data[state] | counties
    pool.close()
    pool.terminate()

    # generate results for the DNC data
    dnc_partial = partial(get_data_for_state_from_dnc_data, output_dir = OUTPUT_DIR)
    pool = Pool(2)
    results = pool.map(dnc_partial,glob.glob(DNC_DATA_DIR + "/*"))
    for result in results:
        for state,counties in result.items():
            state_to_county_data[state] = state_to_county_data[state] | counties
    pool.close()
    pool.terminate()

    # write out county/state file
    state_county_out_fil = open(STATE_COUNTY_FILE, "w")
    for k, v in state_to_county_data.items():
        for county in v:
            state_county_out_fil.write(k + "\t" + str(county) + "\n")
    state_county_out_fil.close()


