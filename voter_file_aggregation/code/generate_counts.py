from multiprocessing import Pool, cpu_count
import glob
import os
import sys
import pandas as pd
from util import write_file

sys.argv = ['', '../data/cleaned_voter_files']

if len(sys.argv) != 2:
    print 'Usage: generate_counts.py [path_to_cleaned_voter_files]'
    sys.exit(-1)

VOTER_FILE_LOC = sys.argv[1]
OUTPUT_DIR = VOTER_FILE_LOC +"_with_counts" if VOTER_FILE_LOC[-1] != "/" else VOTER_FILE_LOC[-1] +"_with_counts"

try:
    os.mkdir(OUTPUT_DIR)
    print 'output dir created: ', OUTPUT_DIR
except:
    print 'output dir already exists: ', OUTPUT_DIR


def gen_count_data(filename):
    basefn =  os.path.basename(filename)
    print 'starting: ', basefn
    output_filename = os.path.join(OUTPUT_DIR,basefn)

    d = pd.read_csv(filename,sep="\t")
    for x in ['county', 'city', 'state', 'zipcode']:
        print "\t", basefn, x
        v = pd.DataFrame(d.groupby(['first_name', 'last_name', x]).size()).reset_index()
        v.columns = ['first_name', 'last_name', x, x + '_count']
        d = pd.merge(d, v, on=['first_name', 'last_name', x])
    d = d.fillna("")
    write_file(d, output_filename)

all_cleaned_voter_files = glob.glob(os.path.join(VOTER_FILE_LOC,"*"))
n_cpu = min(len(all_cleaned_voter_files), cpu_count()/float(2))

#gen_count_data(all_cleaned_voter_files[0])
print 'N CPUS: ', n_cpu
pool = Pool(int(n_cpu))
results = pool.map(gen_count_data,all_cleaned_voter_files)
