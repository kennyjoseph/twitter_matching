import io
import glob
states = ['WY', 'WV', 'WI', 'WA', 'VT', 'VA', 'UT', 'TX', 'TN', 'SD', 'SC', 
          'RI', 'PA', 'OR', 'OK', 'OH', 'NY', 'NV', 'NM', 'NJ', 'NH', 'NE',
          'ND', 'NC', 'MT', 'MS', 'MO', 'MN', 'MI', 'ME', 'MD', 'MA', 'LA', 
          'KY', 'KS', 'IN', 'IL', 'ID', 'IA', 'HI', 'GA', 'FL', 'DE', 'DC', 
          'CT', 'CO', 'AZ', 'AR', 'AL', 'AK']

state_map = {s : io.open("/net/data/twitter-voters/voter-data/extra_state_files/"+s+"_extra.tsv","w") for s in states}

for fil in glob.glob("/net/data/twitter-voters/voter-data/ts_cleaned/*"):
    fil_state = fil[-6:-4]
    print fil, fil_state
    infil = io.open(fil)
    print infil.readline()
    for i, line in enumerate(infil):
        x = line.split("\t")
        if x[20] == fil_state:
            continue
        elif x[20] in state_map:
            state_map[x[20]].write(line)

    infil.close()

y = [x.close() for x in state_map.values()]
