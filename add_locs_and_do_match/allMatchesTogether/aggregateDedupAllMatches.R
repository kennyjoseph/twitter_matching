
source("do_the_matching.R")
voterfileDir = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts"
candMatchesBaseDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve"
locsBaseDir = "/home/lfriedl/twitter_matching/add_locs_and_do_match"


voterfiles = c("rhode_island.tsv", "delaware.tsv", "colorado.tsv", "connecticut.tsv", "north_carolina.tsv",
                "ohio.tsv", "oklahoma.tsv", "washington.tsv", "michigan.tsv", "florida.tsv",
                "voterfile_unique_ia_nh_20150915.tsv", "voterfile_unique_national2m.tsv", "natl100k...")         # <-- note 100k isn't yet in tsv format
datadirs = c("public_voters/RI", "public_voters/DE", "public_voters/CO", "public_voters/CT", "public_voters/NC",
                "public_voters/OH", "public_voters/OK", "public_voters/WA", "public_voters/MI", "public_voters/FL",
                "dnc_voters/IANH", "dnc_voters/natl2M", "dnc_voters/natl100k_all")

# the natl 100k set is slightly different than the others (in format and colnames)
voterfiles100k = file.path("/home/lfriedl/twitterUSVoters/data/voter-data", c("voterfile_sample100k_uniqInState.csv", "voterfile_sample100k_allUsable.csv"))

# 1. Go back rerunning all "best" versions of matches but w/o deduping.
commonOutDir = file.path(locsBaseDir, "allMatchesTogether/sourceMatchFiles")
matchFiles = c()
for (i in 1:length(voterfiles)) {
	voterfileName = voterfiles[i]
	dataDirStem = datadirs[i]
	print(paste("working on", voterfileName))

	voterfileIn = file.path(voterfileDir, voterfileName)
	candMatchLocProbsFile = file.path(locsBaseDir, dataDirStem, "allCandidateMatchLocProbs.csv")

        #filterFieldForMatching = "state_count"
        #filterFieldBase = substr(filterFieldForMatching, 1, nchar(filterFieldForMatching) - 6)
	foreignCutoff = .9

	if (i <= 11) {
		voterfileBase = substr(voterfileName, 1, nchar(voterfileName)-4)
		filterFieldForMatching = "city_count"
		filterFieldBase = substr(filterFieldForMatching, 1, nchar(filterFieldForMatching) - 6)

		matchResultsFile = file.path(commonOutDir, paste0("matches-", voterfileBase, "-uniq", filterFieldBase, "RF", foreignCutoff, "-rule5-wDups.csv"))
		matchResultsOut = file.path(commonOutDir, paste0("matches-", voterfileBase, "-uniq", filterFieldBase, "RF", foreignCutoff, "-rule5-wDups.out"))
		capture.output(matchTheMatches(voterfileIn=voterfileIn, matchFileIn=candMatchLocProbsFile, fullMatchFileOut=matchResultsFile,
			matchRulesVersion = 5, filterField = filterFieldForMatching, filterFieldMax = 1, foreignIfProbScoreAbove=.9, removeDups=FALSE), file=matchResultsOut)

		# notes: RI ends up w/4 fewer state-level now -- I think it's that I didn't originally rm city-level Twitter accts when proceeding to state matching.
		# ...as do almost all the calls. I do think this is the right (algorithmic) thing to do, though: give those city-level Twitter candidates higher precedence.

	} else if (i == 12) { # natl2M
                voterfileBase = "natl2m"
                voterfileIn="~/twitterUSVoters-0/voters/voterfile_unique_national2m.csv"        # changed back to this because KJ's version lost some lines that get matched
		filterFieldBase = "state"

                matchResultsFile = file.path(commonOutDir, paste0("matches-", voterfileBase, "-uniq", filterFieldBase, "RF", foreignCutoff, "-rule3-wDups.csv"))
                matchResultsOut = file.path(commonOutDir, paste0("matches-", voterfileBase, "-uniq", filterFieldBase, "RF", foreignCutoff, "-rule3-wDups.out"))
                capture.output(matchTheMatches(voterfileIn=voterfileIn, matchFileIn=candMatchLocProbsFile, fullMatchFileOut=matchResultsFile, voterfileOldFormat=T,
                        foreignIfProbScoreAbove=.9, removeDups=FALSE), file=matchResultsOut)

		# Actually, let's get both versions of the natl2M: one for matching with others' fields, the other for matching with natl100k
		voterfileName = voterfiles[i]
		voterfileIn = file.path(voterfileDir, voterfileName)
		voterIDPrefix = "national2m_2015_"
                matchResultsFile = file.path(commonOutDir, paste0("matches-", voterfileBase, "-uniq", filterFieldBase, "RF", foreignCutoff, "-rule5-wDups.csv"))
                matchResultsOut = file.path(commonOutDir, paste0("matches-", voterfileBase, "-uniq", filterFieldBase, "RF", foreignCutoff, "-rule5-wDups.out"))
                capture.output(matchTheMatches(voterfileIn=voterfileIn, matchFileIn=candMatchLocProbsFile, fullMatchFileOut=matchResultsFile, 
                        matchRulesVersion = 5, foreignIfProbScoreAbove=.9, removeDups=FALSE, voterIDPrefix=voterIDPrefix), file=matchResultsOut)

	} else if (i == 13) { # natl100k_all
		voterfileIn = voterfiles100k[2]
		voterfileBase = "natl100k_allUsable"
		filterFieldForMatching = "voters_in_state"
		filterFieldBase = "state"

		matchResultsFile = file.path(commonOutDir, paste0("matches-", voterfileBase, "-uniq", filterFieldBase, "RF", foreignCutoff, "-rule5-wDups.csv"))
		matchResultsOut = file.path(commonOutDir, paste0("matches-", voterfileBase, "-uniq", filterFieldBase, "RF", foreignCutoff, "-rule5-wDups.out"))

		capture.output(matchTheMatches(voterfileIn=voterfileIn, matchFileIn=candMatchLocProbsFile, fullMatchFileOut=matchResultsFile, voterfileOldFormat=T,
			matchRulesVersion = 5, filterField = filterFieldForMatching, filterFieldMax = 1, foreignIfProbScoreAbove=.9, removeDups=FALSE), file=matchResultsOut)
	}
	matchFiles = c(matchFiles, matchResultsFile)
}



}

# 2. Do a massive dedup (of pairs sharing the same Twitter account) instead, saving pairs to another file for inspection.

# Where this gets tricky will be where national-sample files match to the same Twitter accounts as state-sample files. These could be the same exact voter record.
# Within a given state, or from one state to another, we could possibly be seeing the same voter, but it would have to be an error (duplication) in the voter DB.

# Another consideration: the national samples are precisely those that we've used old voter file format for, and they have different fields than the others.

# Outline v1 for how to handle:
# a. rbind all state-level files. Detect duplicates and save them to another file, and keep a list of those Twitter accounts.
# b. rbind the national-level files. If any voter/Twitter pair is seen twice, no problem! If a voter is seen twice with different Twitter accts, that's a problem.
# c. taking just the columns common to all files, rbind what remains.
#    -if a Twitter account is now seen twice, is it to natl + the same state?  --> 1 group: maybe ok
#						to natl + a different state (or possibly multiple matches) --> messy ones
#    -if see a Twitter account previously seen more than once --> messy ones
# Everything that's left after that should appear just once in the big list.

# Outline v2 (4/30/17):
# a. rbind all state-level files. Detect duplicates and save them to another file, and keep a list of those Twitter accounts.
#  (any dups count as case "(a)" from my notebook.)
# b. rbind national-level files using old format.
#    -Check and remove (only the dup versions of) voters occurring twice. (b1)
#    -Remaining Twitter dups: handle as (a) (including IDs on kill list from earlier).
# c. combine natl with states
#    -First, switch to the new format for natl 2M: read it in and remove killed rows.
#    -foreach of natl2m, natl100k x states:
#	-if same twitter ID AND same state, probably ok(?) (b2)
# 	-else bad; handle as (a): add to dup list, and remove from all data


# a. read state data
stateMatchFiles = matchFiles[1:11]
stateData = vector(mode="list", length=11)
for (i in 1:11) {
	stateData[[i]] = fread(stateMatchFiles[i])
}
allStateData = rbindlist(stateData)	# dim: 456554 x 31

# find dups within states (a)
twitterAcctFreqs = table(allStateData$twProfileID)		# 454766 IDs total
dupTwitterIDs = names(twitterAcctFreqs)[twitterAcctFreqs > 1]	# 1772 occur > once
stateDupRowIDs = which(as.character(allStateData$twProfileID) %in% dupTwitterIDs)
dupsInStates = allStateData[stateDupRowIDs,]
dupsInStates = dupsInStates[order(twProfileID),]
fwrite(dupsInStates, file="dupsInStates.csv")		# spot checked: correct to throw these out. saw 1-2 that are voter dups, but most are erroneous/ambiguous matches.
allStateData2 = allStateData[!stateDupRowIDs,]	# 452994 rows = 454766 - 1772
		

# b. read natl data (old version)
natlMatchFiles = matchFiles[12:13] 	# natl files in old column format
natlData = vector(mode="list", length=2)
natlData[[1]] = fread(natlMatchFiles[1])
natlData[[2]] = fread(natlMatchFiles[2])
# remove extra cols from 100k data:
natlData[[2]][, c("zip5_for_counts","voters_in_country","voters_in_state","voters_in_zip5") := NULL]
allNatlData = rbindlist(natlData, use.names=T)	# 29790 x 97



## save simple version (containing dups) of everything
fwrite(allStateData, "simple-allStates.csv")
fwrite(allNatlData, "simple-allNatl.csv")
t1 = unique(c(allStateData$twProfileID,allNatlData$twProfileID))
write.table(t1, file="simple-allTwitterIDs.txt", row.names=F, col.names="twitterProfileID")
## simple, non-deduped versions give 486344 rows containing 476938 twitter IDs

# (b1): find voters occurring twice
# manually checked: length(unique(allNatlData$voter_id)) == 29776 == length(unique(paste(allNatlData$voter_id, allNatlData$twProfileID))).
# that is, voters appearing twice are always matched to same Twitter ID. good.

	# not needed...
	#voterAcctFreqs = table(allNatlData$voter_id)
	#dupVoterIDs = names(voterAcctFreqs)[voterAcctFreqs > 1]
	#natlDupRowIDs = which(allNatlData$voter_id %in% dupVoterIDs)
# which row to keep of each duplicated pair? The one from the natl2M file, which was produced later. That's the earlier row in allNatlData.
dupNatlRow = duplicated(paste(allNatlData$voter_id, allNatlData$twProfileID))
#allNatlData2 = allNatlData[!dupNatlRow,]
# Actually, what we'll do is keep all of 2M (will paste the other version to the states later), and keep what's left of 100k in a separate table.
natl100kData = natlData[[2]]
natl100kDupRows = dupNatlRow[(nrow(natlData[[1]])+1):nrow(allNatlData)]
natl100kData = natl100kData[!natl100kDupRows,]

# record and kill any dup twitter accts found within natl data (a)
twitterAcctFreqs = table(c(natlData[[1]]$twProfileID, natl100kData$twProfileID))
newDupTwitterIDs = names(twitterAcctFreqs)[twitterAcctFreqs > 1]	# just 5 new ids
# append new list to old kill list ... and we actually need to remove the union from both new and old data.
dupTwitterIDs = c(dupTwitterIDs, newDupTwitterIDs)
# find all natl rows with these
natl2MDupRowIDs = which(as.character(natlData[[1]]$twProfileID) %in% dupTwitterIDs)	# 61 rows, most from old ids
natl100kDupRowIDs = which(as.character(natl100kData$twProfileID) %in% dupTwitterIDs)	# only 2 rows (and they happen to be from new dup ids)
natlDupRows = rbind(natlData[[1]][natl2MDupRowIDs,], natl100kData[natl100kDupRowIDs,])
fwrite(natlDupRows[order(twProfileID),], file="dupsInNatl.csv")
natl100kData = natl100kData[!natl100kDupRowIDs,]					# now 790 rows, down from orig 806
# no need to modify natl2M right now; just keep track of the dup twitter IDs
# find state rows with these, save, and delete
stateDupRowIDs2 = which(as.character(allStateData2$twProfileID) %in% dupTwitterIDs)	# only 2 new ones
dupsInStates = allStateData2[stateDupRowIDs2,]
fwrite(dupsInStates, file="dupsInStates.csv", append=T)		
allStateData2 = allStateData2[!stateDupRowIDs2,]


# c. combine state and national
# -First, switch to new version of 2m
newNatl2MFile = "/home/lfriedl/twitter_matching/add_locs_and_do_match/allMatchesTogether/sourceMatchFiles/matches-natl2m-uniqstateRF0.9-rule5-wDups.csv"
natl2mData = fread(newNatl2MFile)
# and remove Twitter IDs we've already killed
natlRowsToRm = which(as.character(natl2mData$twProfileID) %in% dupTwitterIDs)
# noted: this contains 2 fewer dups than other file format did above, but that's probably b/c it contained 2 fewer rows to begin with. Identical after fixing that.
natl2mData = natl2mData[!natlRowsToRm,]		# 28923 rows
# odd quirks to clean:
natl2mData[, middle_name := as.character(middle_name)]
natl2mData[, middle_name := '']		# was always NA
natl2mData[, party_affiliation2008 := as.character(party_affiliation2008)]
natl2mData[, party_affiliation2008 := '']		# was always NA
natl2mData[, party_affiliation2010 := as.character(party_affiliation2010)]
natl2mData[, party_affiliation2010 := '']		# was always NA
natl2mData[, party_affiliation2012 := as.character(party_affiliation2012)]
natl2mData[, party_affiliation2012 := '']		# was always NA
natl2mData[, party_affiliation2014 := as.character(party_affiliation2014)]
natl2mData[, party_affiliation2014 := '']		# was always NA
natl2mData[, zipcode := as.character(zipcode)]		# this is annoying, but because other data wasn't clean enough to stay integer

# combine with states, then look for dups
stateAndNatl2M = rbind(allStateData2, natl2mData)	# 481915 x 31
#stateAnd100k = rbind(allStateData2[, 1:2], natl100kData[, 1:2])   # todo: come back and get colnames that are needed

twitterAcctFreqs = table(stateAndNatl2M$twProfileID)
newDupTwitterIDs = names(twitterAcctFreqs)[twitterAcctFreqs > 1]	# 7338 of them
# need to pull rows separately from state vs. natl source data
stateRowIDsWithDups = which(as.character(allStateData2$twProfileID) %in% newDupTwitterIDs)
natlRowIDsWithDups = which(as.character(natl2mData$twProfileID) %in% newDupTwitterIDs)
dupsMerged = merge(allStateData2[stateRowIDsWithDups,], natl2mData[natlRowIDsWithDups,], by=c("twProfileID"))

# Potentially good ones have same: state, first & last names, birthyear
NCHasWrongBirthyear = T
if (NCHasWrongBirthyear) {
	# a change: assume NC is off by 3 to 4 (NC's birth year is lower). Gives 7238 IDs this way.
	potentiallyOk = which(dupsMerged$state.x == dupsMerged$state.y & dupsMerged$first_name.x == dupsMerged$first_name.y & 
				dupsMerged$last_name.x == dupsMerged$last_name.y & 
				((dupsMerged$birth_year.x == dupsMerged$birth_year.y & dupsMerged$state.x != "NC") | 
				 (dupsMerged$state.x == "NC" & (dupsMerged$birth_year.y - dupsMerged$birth_year.x %in% c(3,4))))  )
} else {
	# (original way)
	potentiallyOk = which(dupsMerged$state.x == dupsMerged$state.y & dupsMerged$first_name.x == dupsMerged$first_name.y & 
				dupsMerged$last_name.x == dupsMerged$last_name.y & dupsMerged$birth_year.x == dupsMerged$birth_year.y)
}
potentiallyOkTwIDs = unique(dupsMerged[potentiallyOk, twProfileID])	# 6483 IDs
badTwIDs = dupsMerged[!potentiallyOk, twProfileID]			# 855 --> 100 w/NC
# (b2) Save potentially ok dups for inspection and retain the state versions of them in our data
potentiallyOkRows = stateAndNatl2M[twProfileID %in% potentiallyOkTwIDs,]
potentiallyOkRows = potentiallyOkRows[order(twProfileID),]
fwrite(potentiallyOkRows, "dupsProbablyOkSameVoter.csv")
# Remove those potentially-ok dups from natl2M, then re-rbind with state
natl2mData2 = natl2mData[!(twProfileID %in% potentiallyOkTwIDs),]
stateAndNatl2M2 = rbind(allStateData2, natl2mData2)			# now 475432 = 481915 - 6483

# (a) Kill the twitter IDs we didn't judge potentially ok. From all data (after saving a copy).
dupTwitterIDs = c(dupTwitterIDs, as.character(badTwIDs))
stateNatlDupRowIDs = which(as.character(stateAndNatl2M2$twProfileID) %in% dupTwitterIDs)	# 1710 of them == 855 * 2
dupsInStateNatl = stateAndNatl2M2[stateNatlDupRowIDs,]
dupsInStateNatl = dupsInStateNatl[order(twProfileID),]
fwrite(dupsInStateNatl, "dupsAcrossStateNatl.csv")
stateAndNatl2M3 = stateAndNatl2M2[!stateNatlDupRowIDs,]			# now 473722 = 475432 - 1710

# yay! 
# Just, haven't done part c with 100k yet.
# meanwhile, write out anyway.
fwrite(stateAndNatl2M3, "allMatches-inprogress.csv")
fwrite(natl100kData, "matches100k-inprogress.csv")		# 206 of the 790 are duplicated with state data
write.table(dupTwitterIDs, "dup-twitter-ids-in-progress.txt", row.names=F)

# 100k: combine with state-level. Using just a subset of columns.
natl100kSlim = natl100kData[, .(voter_id,twProfileID,twProfileName,twProfileHandle,twProfileLoc,first_name, last_name, birth_date, state_code, reg_address_city, reg_address_zip5)]
natl100kSlim[, birth_year := substr(birth_date, 1, 4)]
natl100kSlim[, birth_date := NULL]
setnames(natl100kSlim, c("state_code", "reg_address_city", "reg_address_zip5"), c("state", "city", "zipcode"))
# combine with states, then look for dups. (copying & modifying from ~45 lines earlier)
# (or...can I avoid combining by just seeing if there are any dup twitter IDs? no, b/c there are 207 dup IDs (from hacky file comparisons). Do need to check them further.)
# note: unlike before, this time we have the natl2M attached to states.
stateAndNatlSlim = stateAndNatl2M3[, .(voter_id,twProfileID,twProfileName,twProfileHandle,twProfileLoc, first_name, last_name, birth_year, state, city, zipcode)]
stateAndNatl100k = rbind(stateAndNatlSlim, natl100kSlim)

twitterAcctFreqs = table(stateAndNatl100k$twProfileID)
newDupTwitterIDs = names(twitterAcctFreqs)[twitterAcctFreqs > 1]        # 206, to be precise
# need to pull rows separately from state vs. natl source data
stateRowIDsWithDups = which(as.character(natl100kSlim$twProfileID) %in% newDupTwitterIDs)
natlRowIDsWithDups = which(as.character(stateAndNatlSlim$twProfileID) %in% newDupTwitterIDs)
dupsMerged = merge(natl100kSlim[stateRowIDsWithDups,], stateAndNatlSlim[natlRowIDsWithDups,], by=c("twProfileID"))
# Decide which are potentially the same voter. (b2)
# This time, I don't know if names will match precisely, but city and zipcode should. 
# Ok: it turns out all that match on the other criteria also match on names. Keep them all in the code.
if (NCHasWrongBirthyear) {
	# a change: assume NC is off by 3 to 4 (NC's birth year is lower). Gives 7238 IDs this way.
	potentiallyOk = which(dupsMerged$state.x == dupsMerged$state.y & tolower(dupsMerged$city.x) == tolower(dupsMerged$city.y) &
				dupsMerged$zipcode.x == dupsMerged$zipcode.y & 
				tolower(dupsMerged$first_name.x) == tolower(dupsMerged$first_name.y) & tolower(dupsMerged$last_name.x) == tolower(dupsMerged$last_name.y) &
				((dupsMerged$birth_year.x == dupsMerged$birth_year.y & dupsMerged$state.x != "NC") | 
				 (dupsMerged$state.x == "NC" & (dupsMerged$birth_year.y - dupsMerged$birth_year.x %in% c(3,4))))  )
} else {
	potentiallyOk = which(dupsMerged$state.x == dupsMerged$state.y & tolower(dupsMerged$city.x) == tolower(dupsMerged$city.y) & 
				dupsMerged$zipcode.x == dupsMerged$zipcode.y & dupsMerged$birth_year.x == dupsMerged$birth_year.y 
				& tolower(dupsMerged$first_name.x) == tolower(dupsMerged$first_name.y) & tolower(dupsMerged$last_name.x) == tolower(dupsMerged$last_name.y))
}
potentiallyOkTwIDs = unique(dupsMerged[potentiallyOk, twProfileID])	# 137 IDs --> 161 w/NC fix
badTwIDs = dupsMerged[!potentiallyOk, twProfileID]			# 69 --> 45 w/NC
potentiallyOkRows = stateAndNatl100k[twProfileID %in% potentiallyOkTwIDs,]
potentiallyOkRows = potentiallyOkRows[order(twProfileID),]
fwrite(potentiallyOkRows, "dupsProbablyOkSameVoter2.csv")
# Remove those potentially-ok dups from natl100k (orig + slim), then re-rbind with state
natl100kData2 = natl100kData[!(twProfileID %in% potentiallyOkTwIDs),]
natl100kSlim2 = natl100kSlim[!(twProfileID %in% potentiallyOkTwIDs),]
stateAndNatl100k2 = rbind(stateAndNatlSlim, natl100kSlim2)			# now 474375 = 473722 + 790 - 137

# (a) Kill the twitter IDs we didn't judge potentially ok. From all data (after saving a copy).
dupTwitterIDs = c(dupTwitterIDs, as.character(badTwIDs))
stateNatl100kDupRowIDs = which(as.character(stateAndNatl100k2$twProfileID) %in% dupTwitterIDs)	# 138 of them == 69 * 2
dupsInStateNatl = stateAndNatl100k2[stateNatl100kDupRowIDs,]
dupsInStateNatl = dupsInStateNatl[order(twProfileID),]
fwrite(dupsInStateNatl, "dupsAcrossStateNatl2.csv")
# remove them from slim version (stateAndNatl100k2), plus the separate fat versions (natl100kData and stateAndNatl2M3)
stateAndNatlSlimFinal = stateAndNatl100k2[!stateNatl100kDupRowIDs,]			# now 474237 = 474375 - 138
stateAndNatl2MFat = stateAndNatl2M3[!(as.character(twProfileID) %in% dupTwitterIDs),]   # 473653 x 31
natl100kFinal = natl100kData2[!(as.character(twProfileID) %in% dupTwitterIDs),] 	# 584 x 97. 
# Note: 584 + 473653 = 474237. That is, we have the full data either in 1 skinny table with all matches, or 2 separate wide ones (with different columns).
fwrite(stateAndNatlSlimFinal, "allMatches-skinny.csv")
fwrite(stateAndNatl2MFat, "allMatches-wide1.csv")
fwrite(natl100kFinal, "allMatches-wide2.csv")
