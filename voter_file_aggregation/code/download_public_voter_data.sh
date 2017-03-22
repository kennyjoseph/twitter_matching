#!/usr/bin/env bash

DATA_DIR="../data/raw_voter_2/public_voter_files"

# Colorado
mkdir $DATA_DIR/colorado
cd $DATA_DIR/colorado
for i in {1..8};
do
    wget --no-clobber -O $i.zip http://coloradovoters.info/downloads/20161201/Registered_Voters_List_%20Part$i.zip;
    unzip $i.zip;
done
rm *.zip


# Connecticut
cd ..
mkdir connecticut
cd connecticut
for i in {1..4};
do
    wget --no-clobber -O $i.zip  http://connvoters.com/downloads/20170203/votelct$i.zip;
    unzip $i.zip;
done
rm *.zip
mv SSP/ELCT/VOTER/* .
rm -rf SSP

# Delaware
cd ..
mkdir delaware
cd delaware
wget --no-clobber http://delawarevoters.info/download/20150521/ActiveReg.csv

#Ohio
cd ..
mkdir ohio
cd ohio
wget --no-clobber  ftp://sosftp.sos.state.oh.us/free/Voter/SWVF_1_44.zip
unzip SWVF_1_44.zip
wget --no-clobber ftp://sosftp.sos.state.oh.us/free/Voter/SWVF_45_88.zip
unzip SWVF_45_88.zip
rm *.zip

# Rhode Island
cd ..
mkdir rhode_island
cd rhode_island
wget --no-clobber http://rivoters.com/download/2015-01.txt

# Washington
echo "FOR WASHINGTON, FIRST GO TO https://www.sos.wa.gov/elections/vrdb/ and request approval!"
mkdir washington
cd washington/
wget --no-clobber http://www.sos.wa.gov/_assets/elections/vrdb-current.zip
unzip vrdb-current.zip
mkdir voting_records
mv 201702_VRDB_Extract.txt voting_records/
cat 2015-2016_VotingHistoryExtract.txt 2017-2018_VotingHistoryExtract.txt  > voting_history.txt
rm 201* vrdb-current.zip Extract\ Readme.pdf README.txt
rm VRDB\ Database\ Fields\ -\ 201506.doc

# Florida
cd ..
mkdir florida
cd florida
mkdir voter_history
cd voter_history
wget -r --no-clobber --no-parent --no-host-directories --reject="index.html"  --cut-dirs=3 http://flvoters.com/download/20170228/20170307_VoterHistory/
rm robots.txt
cd ..
mkdir voting_records
cd voting_records
wget -r --no-clobber --no-parent --no-host-directories --cut-dirs=3 --reject="index.html" --directory-prefix=$ST http://flvoters.com/download/20170228/20170307_VoterDetail/
rm robots.txt
cd ..


# Oklahoma
cd ..
mkdir oklahoma
cd oklahoma
wget --no-clobber http://oklavoters.com/downloads/20161205/CDSW_VR_20161208175906.zip
wget --no-clobber http://oklavoters.com/downloads/20161205/CDSW_VH_20161208175746.zip
unzip CDSW_VR_20161208175906.zip
unzip CDSW_VH_20161208175746.zip
rm *.zip precincts.csv readme.pdf
rm *vh.csv

# Michigan
cd ..
mkdir michigan
cd michigan
wget --no-clobber http://michiganvoters.info/download/20160901/FOIA_VOTERS.zip
wget --no-clobber http://michiganvoters.info/download/20160901/countycd.lst
unzip FOIA_VOTERS.zip
rm FOIA_VOTERS.zip
mkdir voting_records
mv entire_state_v.lst voting_records/
wget --no-clobber http://michiganvoters.info/download/20160901/FOIA_HISTORY.zip
unzip FOIA_HISTORY.zip
rm FOIA_HISTORY.zip

# North Carolina
cd ..
mkdir north_carolina
cd north_carolina
wget --no-clobber --directory-prefix=$ST https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip
unzip ncvoter_Statewide.zip
rm ncvoter_Statewide.zip
for i in {1..100};
do
    wget --no-clobber -O $i.zip  http://dl.ncsbe.gov.s3.amazonaws.com/data/ncvhis$i.zip;
    unzip $i.zip;
done
rm *.zip
cat ncvhis*.txt > voter_history.txt
rm ncvhis*.txt
mkdir voting_records
mv ncvoter_Statewide.txt voting_records/