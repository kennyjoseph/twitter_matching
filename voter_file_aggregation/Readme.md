# Env setup

python libraries required (aside from standard anaconda distro):
```
pip install nameparser
```

Also, step 03 currently requires access to the deceptions spark cluster

# Raw data info

### Most
Most data is from the public database at, e.g., connvoters.com.  

### The rest:

- OH - http://www6.sos.state.oh.us/ords/f?p=111:1:0::NO:RP:P1_TYPE:STATE
- DC: https://github.com/ajschumacher/dc_voter_reg
- WA: https://www.sos.wa.gov/elections/vrdb-download-form.aspx
- NV (Clark County only): http://www.clarkcountynv.gov/election/Pages/VoterDataFiles.aspx

### For purchase, if we want:
- PA - https://www.pavoterservices.state.pa.us/pages/PurchasePAFULLVoterExport.aspx
-  National - http://nationbuilder.com/voterfiled
-  IL - https://www.elections.il.gov/votinginformation/computerizedvoterdata.aspx
-  LA - http://www.sos.la.gov/ElectionsAndVoting/BecomeACandidate/PurchaseVoterLists/Pages/default.aspx
-  GA - http://sos.ga.gov/index.php/elections/order_voter_registration_lists_and_files
-  CA - Will email
- NC full - http://www.ncsbe.gov/data-statistics
  
### Weird restrictions, but probably could be collected:
-  TX http://www.txdemocrats.org/act/van
-  VA http://elections.virginia.gov/candidatepac-info/client-services/
-  NV http://nvsos.gov/index.aspx?page=332
-  NY http://www.elections.ny.gov/FoilRequests.html

# Process

1. download voter data - this step has to be automated, sorry. 
Right now the data is at ``` .... insert data loc ... ``` on achtung

2.  Run ```transform_raw_voter_dnc_to_clean_csv.py```, passing in the location of the data.
This file generates one output file for each of the public voting record files, as well as for the dnc data.
It does some removal of incomplete records (no birthdate, no first name, no last name, no zipcode).
It is also structured so that adding new states/data should be relatively formulaic, just follow
the pattern of the already existing states and then rerun the script.
The data will be output in ```[path_to_data_dir]/raw_voter/cleaned_data/```.


# Other Notes

```data/national_county.txt``` is from https://www.census.gov/geo/reference/codes/cou.html
```rsync -avzP barbera_data/ kjoseph@achtung.ccs.neu.edu:barbera_data```

