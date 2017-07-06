"""
A simple, special processing script for the allusable data lisa sent from the 100K
"""

from transform_raw_voter_dnc_to_clean_csv import *

varnames_to_keep += ['voters_in_state','voters_in_zip5']
varnames_to_keep_len = len(varnames_to_keep)

def DNCV(filename, output_dir):
    print filename
    output_filename = os.path.join(output_dir, os.path.basename(filename))
    data = transform_dnc_data(filename)
    data['county_count'] = data.voters_in_state
    data['city_count'] = data.voters_in_state
    data['zipcode_count'] = data.voters_in_zip5
    data['state_count'] = data.voters_in_state

    data.drop("voters_in_state", axis=1, inplace=True)
    data.drop("voters_in_zip5", axis=1, inplace=True)
    write_file(data,output_filename.replace(".csv",".tsv"))


DNCV("../voterfile_sample100k_allUsable.csv","../")

