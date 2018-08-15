# Env setup

python libraries required (aside from standard anaconda distro):
```
pip install nameparser
```


# Raw data info

To download the raw data, run ```download_public_voter_data.sh```. This will, I think, download all
the data used.  

Note that in ```transform_raw_voter_dnc_to_clean_csv.py``` there is some information about when the data
was downloaded. If you re-download new data, probably best to update these values.

### Most
Most data is from the public database at, e.g., connvoters.com.  

### The rest:

- OH - http://www6.sos.state.oh.us/ords/f?p=111:1:0::NO:RP:P1_TYPE:STATE
- DC: https://github.com/ajschumacher/dc_voter_reg
- WA: https://www.sos.wa.gov/elections/vrdb-download-form.aspx
- NC: http://dl.ncsbe.gov/data/

### For purchase, if we want:
- PA - https://www.pavoterservices.state.pa.us/pages/PurchasePAFULLVoterExport.aspx
-  IL - https://www.elections.il.gov/votinginformation/computerizedvoterdata.aspx
-  LA - http://www.sos.la.gov/ElectionsAndVoting/BecomeACandidate/PurchaseVoterLists/Pages/default.aspx
-  GA - http://sos.ga.gov/index.php/elections/order_voter_registration_lists_and_files
-  CA - Will email
  
### Weird restrictions, but probably could be collected:
-  TX http://www.txdemocrats.org/act/van
-  VA http://elections.virginia.gov/candidatepac-info/client-services/
-  NV http://nvsos.gov/index.aspx?page=332
-  NY http://www.elections.ny.gov/FoilRequests.html



# Ignore below here....

# Generating Clean Voter Files

1. download voter data - this step has to be automated, sorry. 
Right now the data is at ``` .... insert data loc ... ``` on achtung

2.  Run ```transform_raw_voter_dnc_to_clean_csv.py```, passing in the location of the data.
This file generates one output file for each of the public voting record files, as well as for the dnc data.
It does some removal of incomplete records (no birthdate, no first name, no last name, no zipcode).
It is also structured so that adding new states/data should be relatively formulaic, just follow
the pattern of the already existing states and then rerun the script.
The data will be output in ```[path_to_data_dir]/cleaned_voter_files```.




# Loading data into BigQuery

## Step 1
 Follow (this)[https://cloud.google.com/bigquery/quickstart-web-ui] tutorial to install and initialize the cloud sdk, set up the project for BigQuery

Step 3 -> Load the data by following this tutorial (https://cloud.google.com/bigquery/quickstart-command-line)

Here are the commands that I had to run, installation included
```
wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-146.0.0-linux-x86_64.tar.gz
tar -xzvf google-cloud-sdk-146.0.0-linux-x86_64.tar.gz 
./google-cloud-sdk/install.sh
gcloud init
```

# Other Notes

```data/national_county.txt``` is from https://www.census.gov/geo/reference/codes/cou.html
```rsync -avzP barbera_data/ kjoseph@achtung.ccs.neu.edu:barbera_data```

