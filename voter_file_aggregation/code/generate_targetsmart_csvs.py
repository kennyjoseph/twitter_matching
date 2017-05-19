from __future__ import print_function

"""
This file parses raw data specifically from targetsmart voter registration files

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

sys.argv = ['', "../data/targetsmart/", "../data/targetsmart_cleaned_csvs/"]
if len(sys.argv) != 3:
    print('USAGE: generate_targetsmart_csvs.py [path_to_targetsmart_data] [path_to_output_data]')
    sys.exit(-1)

VOTER_FILE_DATA_DIR = sys.argv[1]
OUTPUT_DIR = sys.argv[2]
TODAYS_DATE = time.strftime("%d/%m/%Y")
try:
    os.mkdir(OUTPUT_DIR)
except:
    print('didnt make output dir, exists already')


all_var_names = ["voter_id", "first_name", "middle_name",
                 "last_name", "birth_date", "gender", "turnout_2008", "turnout_2010",
                 "turnout_2012", "turnout_2014", "party_affiliation2008",
                 "party_affiliation2010", "party_affiliation2012", "party_affiliation2014",
                 "party_affiliation", "address", "city", "zipcode", "county", "race", 'ethnicity', 'state',
                 'from','birth_year']

varnames_to_keep =copy(all_var_names)
varnames_to_keep.remove("birth_date")
varnames_to_keep_len = len(varnames_to_keep)


def get_data_for_state_from_targetsmart(filename, output_dir):
    print(filename)
    output_filename = os.path.join(output_dir, os.path.basename(filename))
    data = pd.read_csv(filename,sep="\t")
    data = data.fillna("")
    data['middle_name'] = data.tsmart_middle_name.apply(clean_name_text)
    data['first_name'] = data.tsmart_first_name.apply(clean_name_text)
    data['last_name'] = data.tsmart_last_name.apply(clean_name_text)

    data['party_affiliation'] = data.vf_party.apply(get_targetsmart_party_affil)
    data["party_affiliation2008"] = ''
    data["party_affiliation2010"] = ''
    data["party_affiliation2012"] = ''
    data["party_affiliation2014"] = ''
    data["turnout_2008"] = ''
    data["turnout_2010"] = ''
    data["turnout_2012"] = ''
    data["turnout_2014"] = ''

    file_id = os.path.basename(filename).replace("_northeastern_install_file", "").replace(".csv", "")
    data['from'] =file_id

    data['birth_year'] = data.voterbase_dob.astype(str).str.slice(0, 4)

    data['ethnicity'] = ''
    data = data.rename(columns={"voterbase_gender": "gender",
                                "voterbase_race": "race",
                                "tsmart_full_address": "address",
                                "tsmart_city": "city",
                                "tsmart_zip": "zipcode",
                                "tsmart_state": "state",
                                "voterbase_id": "voter_id",
                                "tsmart_county_name": "county"
                                })

    data['voter_id'] = data.voter_id.apply(lambda x: "ts_" + str(x))
    data['zipcode'] = data.zipcode.apply(lambda x: str(int(x)) if x != '' else x)

    data = data.fillna("")
    data = data[varnames_to_keep]

    for x in ['county', 'city', 'state', 'zipcode']:
        print("\t", file_id, x)
        if x != 'state':
            gvs = [x, 'state']
        else:
            gvs = ['state']
        group_vars = ['first_name', 'last_name'] + gvs
        v = pd.DataFrame(data.groupby(group_vars).size()).reset_index()
        v.columns = group_vars + [x + "_count"]
        data = pd.merge(data, v, on=group_vars)
        
    data = data.fillna("")

    write_file(data,output_filename.replace(".csv",".tsv"))
    return filename


files = glob.glob(os.path.join(VOTER_FILE_DATA_DIR,"*"))

# generate results in parallel for state voter files
n_cpu = 4

#get_data_voter_file_helper('WA')
print('N CPUS: ', n_cpu)

get_data_for_state_from_targetsmart(files[0],OUTPUT_DIR)

# pool = Pool(int(n_cpu))
# results = pool.map(get_data_voter_file_helper,ALL_STATES)
# for result in results:
#      print result
# pool.close()
# pool.terminate()
