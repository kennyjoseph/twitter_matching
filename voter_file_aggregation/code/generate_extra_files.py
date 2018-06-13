import io
import glob
import sys
import os

# states for which we'll produce 'extra' files
states = ['WY', 'WV', 'WI', 'WA', 'VT', 'VA', 'UT', 'TX', 'TN', 'SD', 'SC', 
          'RI', 'PA', 'OR', 'OK', 'OH', 'NY', 'NV', 'NM', 'NJ', 'NH', 'NE',
          'ND', 'NC', 'MT', 'MS', 'MO', 'MN', 'MI', 'ME', 'MD', 'MA', 'LA', 
          'KY', 'KS', 'IN', 'IL', 'ID', 'IA', 'HI', 'GA', 'FL', 'DE', 'DC', 
          'CT', 'CO', 'AZ', 'AR', 'AL', 'AK']
#states = ['CA']  	# (CA is present sometimes)
states.append('CA')

if len(sys.argv) != 3:
    print('USAGE: generate_extra_files.py [path_to_cleaned_data] [path_to_extra_files]')
    sys.exit(-1)

CLEANED_DATA_DIR = sys.argv[1]
OUTPUT_DIR = sys.argv[2]

try:
    os.mkdir(OUTPUT_DIR)
except:
    print('didn\'t make output dir, exists already')



state_map = {s : io.open(os.path.join(OUTPUT_DIR, s+"_extra.tsv"), "w") for s in states}

for fil in glob.glob(os.path.join(CLEANED_DATA_DIR,"*")):
    fil_state = fil[-6:-4].upper()	# (sometimes filenames have lowercase state)
    print fil, fil_state
    infil = io.open(fil)
    #print infil.readline()
    for i, line in enumerate(infil):
        x = line.split("\t")
        if x[20] == fil_state:
            continue
        elif x[20] in state_map:
            state_map[x[20]].write(line)

    infil.close()

y = [x.close() for x in state_map.values()]
