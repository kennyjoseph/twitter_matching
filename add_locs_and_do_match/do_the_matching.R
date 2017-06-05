
library(data.table)
source("randomForest.R")	# for printConfusionMatrix

# File to take preliminary matching results (from querying voters against the twitter DB) and convert them to 
# full-fledged matches, where each row = 1 voter + 1 twitter account.

# Notice: only rules 3 and 5 are currently enabled.

# matchFileIn: has one row per potential match, must contain the columns inferredLoc, personid/voter_id, and twProfileID. Probably also has [voter's] state_code, twProfileName, twProfileHandle, twProfileLoc.
# voterfileIn: assumed to be one of the new tsv files (unless use flag voterfileOldFormat), and it has to contain "personid"/voter_id. Can be NULL, in which case we don't merge in voter data.
# fullMatchFileOut: will be a .csv file.
# filterField: e.g., state_count, city_count
# filterFieldMax: e.g., 1 for "only allow voters having state_count <= 1"
# foreignIfProbScoreAbove: e.g., .9 for "rule it out as foreign if probMetaForeign > .9"
# voterIDPrefix: hack to make IDs compatible between personid & voter_id versions. 
# voterfileOldFormat: another hack for using original csv file.
# In terms of required colnames, location cols can now be changed in arguments, and others (voter_id, city, state) are the same as used earlier in the pipeline.
matchTheMatches = function(voterfileIn, matchFileIn, fullMatchFileOut,
                           matchRulesVersion = 3, numBlanksAllowed = 0,		# N.B. code treats 3 == 4; only difference is in numBlanksAllowed.
                            # match rules:  [1: match iff there's only 1 candidate match and it's to the right state]
                            #               [2: match iff there's only 1 candidate match (after excluding matches to other states)]
                            #               3: match iff there's only 1 candidate match (after excluding matches to "foreign" and other states)
                            #                   [requires format >= 3, which marks "foreign"]
                            #               4: exclude foreign and other states, and further, allow there to be up to N blank or unparsed records 
                            #                   as long as there's only 1 in the right state
                            #               5: recreate the procedure KJ used (using either the naive location parse or newInferredLoc)
						   filterField = NULL, filterFieldMax = 0, foreignIfProbScoreAbove=1, 
							voterIDPrefix="", voterfileOldFormat = F, removeDups = TRUE,
							locationColName = "inferredLoc", cityMatchColumn = "locContainsCity") {
    
	# normally only need 1 of the voter columns returned, personid, since going to merge with voter data anyway.
    matchingColsToKeep = c("voter_id", "twProfileID", "twProfileName", "twProfileHandle", "twProfileLoc")
	# But for testing/inspection purposes, may want others:
	#matchingColsToKeep = c("voter_id", "first_name", "last_name", "city", "state", "twProfileID", "twProfileName", "twProfileHandle", "twProfileLoc", "locContainsCity")
	#locationColName = "inferredLoc"

	# 0. Read data and preprocess if needed
	print("Reading and preprocessing candidate matches")
    matchCountsData = setDF(fread(matchFileIn))

	# update column names to look like voter file format 2:
	if ("personid" %in% colnames(matchCountsData)) {
			setnames(matchCountsData, "personid", "voter_id")
		if (nchar(voterIDPrefix)) {
			matchCountsData$voter_id = paste0(voterIDPrefix, matchCountsData$voter_id)
		}
	}
	if ("reg_address_city" %in% colnames(matchCountsData)) {
			setnames(matchCountsData, "reg_address_city", "city")
	}
	if ("state_code" %in% colnames(matchCountsData)) {
			setnames(matchCountsData, "state_code", "state")
	}

	# Fix column names/formatting right from the start (after which we can call summarizeRemainder any time)
	matchCountsData$newInferredLoc = as.character(matchCountsData[[locationColName]])
	matchCountsData$state = as.character(matchCountsData$state)
	if (matchRulesVersion == 5) {
		matchCountsData$cityMatchColumn = matchCountsData[[cityMatchColumn]]
	}

    # If input data includes voters with blank cities, drop them. (Try to prevent this at earlier steps?)
    if ("city" %in% colnames(matchCountsData) & sum(matchCountsData$city == '') > 0) {
        matchCountsData = matchCountsData[matchCountsData$city != '',]
    }

	# Filter to voters who are unique enough for us to look at
	# (with public data, I pulled candidate matches for many voters, planning to filter to a subset here)
	if (!is.null(filterField) && filterFieldMax > 0) {
		print("Entire candidate matches file:")
		counts = summarizeRemainder(matchCountsData)
		print("Filtering according to uniqueness of voters")
		print(paste("Before:", length(unique(matchCountsData$voter_id)), "voters in file"))
		if (filterField %in% colnames(matchCountsData)) {
			matchCountsData = matchCountsData[matchCountsData[[filterField]] <= filterFieldMax,]
		} else {
			stop(paste("matchTheMatches: filterField=", filterField, "specified, but not present in input data"))
		}
		print(paste("After:", length(unique(matchCountsData$voter_id)), "voters"))
	}



    
    # 1. Decide which matches are reliable (depending on the matching rules to use).
    
    # Do the matching! (Exciting part.)
	# 	matchRulesVersion 1 was: conservative / original: only allow the match when there's exactly 1 match on twitter and it's to the right state
	# 	matchRulesVersion 2 was: Ignore any candidate matches for which we know the location and it's a different US state
    if (matchRulesVersion == 3 || matchRulesVersion == 4) {
        # Additionally exclude "foreign" locs (requires more recent loc computation).
		#twMatches = matching3ExcludeForeign(matchCountsData, locationColName)		# rule 3 is just rule 4 with numBlanksAllowed = 0
		twMatches = matching4AllowBlanks_b(matchCountsData, numBlanksAllowed, foreignIfProbScoreAbove) 
	} else if (matchRulesVersion == 5) {
		twMatches = matching5CityThenState_b(matchCountsData, numBlanksAllowed, foreignIfProbScoreAbove) 
	}

    fieldsWanted = (colnames(twMatches) %in% matchingColsToKeep)
    twMatches = twMatches[, fieldsWanted]
    
    # 2. Ensure uniqueness: if >= 2 voters match to the same twitter account, drop these rows.
	if (removeDups) {
		print("Removing any duplicate matches to the same Twitter account")
		twitterAcctFreqs = table(twMatches$twProfileID)
		rows_twitterOccurringJustOnce = (as.character(twMatches$twProfileID) %in% names(twitterAcctFreqs)[twitterAcctFreqs==1])
		twMatches = twMatches[rows_twitterOccurringJustOnce,]
	}
    
    # 3. Merge with the original voter data
	if (! is.null(voterfileIn)) {
		voterData = fread(voterfileIn)
		#voterData = read.csv(file=voterfileIn, header=T) #, stringsAsFactors=F) #colClasses = "character")
		if (voterfileOldFormat) {
			allDataMatched = merge(setDT(twMatches), voterData, by.x="voter_id", by.y="personid", all.x=T)
		} else {
			allDataMatched = merge(setDT(twMatches), voterData, by.x="voter_id", by.y="voter_id", all.x=T)
		}
		if ("from" %in% colnames(allDataMatched)) {
				setnames(allDataMatched, "from", "voterdata_from")
		}
		
		# sanity check: specifying all.x shouldn't have created any blank fields
		if (sum(is.na(allDataMatched$last_name))) {
			warning("Hrmm. Some records were not properly matched to the voter data.")
		}
		
		# 3b. If voter data includes "nameChanged" column, remove rows for which it's true, then remove the column.
		if ("nameChanged" %in% colnames(voterData)) {
			allDataMatched = allDataMatched[allDataMatched$nameChanged==0,
											!(colnames(allDataMatched) %in% "nameChanged")]
		}
	} else {
		allDataMatched = twMatches
	}
        
        
    
    # 4. Write out
    print(paste("Used matching rules", matchRulesVersion, "and got", nrow(allDataMatched), "matches."))
    #write.csv(allDataMatched, file=fullMatchFileOut, row.names=F, na="")
    fwrite(allDataMatched, file=fullMatchFileOut)
    print(paste("Output saved as", fullMatchFileOut))
}

# Utility code for tallying how many candidate matches remain under various criteria
howManyCandidates = function(matchCountsSubset) {
    nameFreqs = table(matchCountsSubset$voter_id)
	t = table(nameFreqs)
	t2 = t[sort(as.numeric(names(t)))]
	print(t2)
	return(t2)
}

removeOtherLocs = function(matchCountsData) {
    # remove rows with a newInferredLoc of "foreign"
    matchCountsData = matchCountsData[matchCountsData$newInferredLoc != "foreign",]

    # remove matchesOtherState rows
    newLocLength = nchar(matchCountsData$newInferredLoc)   # only states have length 2
    differentState = (newLocLength == 2 & matchCountsData$newInferredLoc != matchCountsData$state)
    matchCountsData = matchCountsData[!differentState,]

	return(matchCountsData) 
}


# Cross-tabs: number of voters with {0, 1, >1} candidates of the correct state x {0, 1, 2, 3, >=4} other candidates (not to the correct state)
summarizeRemainder = function(matchCountsData, matchLevel = "state") {
	allVoters = unique(matchCountsData$voter_id)
	print(paste("Total # voters:", length(allVoters), "; total # candidate rows:", nrow(matchCountsData)))
	print(paste("rows with blank loc:", sum(matchCountsData$newInferredLoc == ""), "; with 'unparsed':", sum(matchCountsData$newInferredLoc == "unparsed")))
	print(paste("rows with other state:", sum(matchCountsData$newInferredLoc != matchCountsData$state & nchar(matchCountsData$newInferredLoc)==2),
		"; with correct state:", sum(matchCountsData$newInferredLoc == matchCountsData$state), "; with both city and state:", 
		sum(matchCountsData$newInferredLoc == matchCountsData$state & matchCountsData$cityMatchColumn)))

	# for each voter, how many candidates have the right state?
	if (matchLevel == "state") {
		rightStateFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc == matchCountsData$state])
	} else if (matchLevel == "city") {
		rightStateFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc == matchCountsData$state & matchCountsData$cityMatchColumn])
	}

	# fix rightStateFreq to include all voters
    entriesToAdd = setdiff(allVoters, names(rightStateFreq))
    vectorToAdd = rep(0, length(entriesToAdd))
    names(vectorToAdd) = entriesToAdd
                      
	rightStateFreq = c(rightStateFreq, vectorToAdd)
   
   # for each voter, how many candidates don't have the right state?
	if (matchLevel == "state") {
	   wrongLocFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc != matchCountsData$state])
	} else if (matchLevel == "city") {
	   wrongLocFreq = table(matchCountsData$voter_id[!(matchCountsData$newInferredLoc == matchCountsData$state & matchCountsData$cityMatchColumn)])
	}

	# fix wrongStateFreq to include all voters
    entriesToAdd = setdiff(allVoters, names(wrongLocFreq))
    vectorToAdd = rep(0, length(entriesToAdd))
    names(vectorToAdd) = entriesToAdd
	wrongLocFreq = c(wrongLocFreq, vectorToAdd)

	# Now, cross-tab. (How?)
	rightStateFreq = rightStateFreq[order(names(rightStateFreq))]
	wrongLocFreq = wrongLocFreq[order(names(wrongLocFreq))]
	both = as.data.frame(cbind(rightStateFreq, wrongLocFreq))
	
	crosstab = table(both$rightStateFreq, both$wrongLocFreq)
	rowsum = rowSums(crosstab)
	if (matchLevel == "state") {
		print("Num in-state matches (rows) x Num extra matches (cols) [boxes count voters]")
	} else if (matchLevel == "city") {
		print("Num in-state+city matches (rows) x Num extra matches (cols) [boxes count voters]")
	}
	print(cbind(crosstab, rowsum=rowsum))

	return(both)
                      
}

checkClassifierGroundTruth = function(matchCountsData, foreignIfProbScoreAbove=1) {
	hasUsefulLoc = !(matchCountsData$newInferredLoc %in% c("", "unparsed"))
	print(paste("Checking classifier's AUC for labeled data (", sum(hasUsefulLoc), "instances)"))

	predObj = prediction(matchCountsData$probMetaForeign[hasUsefulLoc], matchCountsData$newInferredLoc[hasUsefulLoc] == "foreign")
	print(paste("AUC for labeled data:", performance(predObj, "auc")@y.values[[1]]))

	if (foreignIfProbScoreAbove < 1) {
		print(paste("Checking classifier's confusion matrix for cutoff", foreignIfProbScoreAbove ))
		printConfusionMatrix(matchCountsData$probMetaForeign[hasUsefulLoc], matchCountsData$newInferredLoc[hasUsefulLoc] == "foreign", cutoff=foreignIfProbScoreAbove)
	}

}

# Same effect as matching4AllowBlanks, different code/print-outs
matching4AllowBlanks_b = function(matchCountsData, numBlanksAllowed, foreignIfProbScoreAbove=1) {
    
	print("Initial candidate matches...")
	counts = summarizeRemainder(matchCountsData)

	if (foreignIfProbScoreAbove < 1 && "probMetaForeign" %in% colnames(matchCountsData)) {
		checkClassifierGroundTruth(matchCountsData, foreignIfProbScoreAbove)
	}

	#matchCountsData = prepMatchDataRmExcluded(matchCountsData, locationColName)
	matchCountsData = removeOtherLocs(matchCountsData)

	print("After removing matches to other locations...")
	counts = summarizeRemainder(matchCountsData)

	if (foreignIfProbScoreAbove < 1 && "probMetaForeign" %in% colnames(matchCountsData)) {
		# (can't check classifier against ground truth after removing foreign rows)

		print(paste0("Removing matches with a high (>", foreignIfProbScoreAbove, ") meta-data probability of being foreign [and unknown loc]..."))
		matchCountsData = matchCountsData[matchCountsData$probMetaForeign <= foreignIfProbScoreAbove | matchCountsData$newInferredLoc == matchCountsData$state,]

		counts = summarizeRemainder(matchCountsData)
	}

    
	votersWanted = rownames(counts[counts$rightStateFreq == 1 & counts$wrongLocFreq <= numBlanksAllowed,])
    print(paste("[rule 3-4 matching] ", length(votersWanted), "voters satisfied both criteria"))

    # identify the matching row for each of those voters
    rowsWanted = (matchCountsData$voter_id %in% votersWanted & matchCountsData$newInferredLoc == matchCountsData$state)
    
    matches = matchCountsData[rowsWanted, ]
	return(matches)
}

# Much like matching4AllowBlanks_b, with an added step to make correct-city matches (iff voter is unique in city) before correct-state matches (iff voter is unique in state).
# N.B. Input data could be unique at any level (zip, city, state). We do uniqueness checks listed above.
# Note effects of different initial filters: we do city, then state. County and state are more stringent than city, so shrink the input set. Zip is more relaxed, so upon using 
# city filter, it's equivalent. All of these, when we do the 2nd part (state only), filter to the same, most restricted set of voters.
matching5CityThenState_b = function(matchCountsData, numBlanksAllowed, foreignIfProbScoreAbove=1) {

	print("Initial candidate matches...")
	counts = summarizeRemainder(matchCountsData)

	if (foreignIfProbScoreAbove < 1 && "probMetaForeign" %in% colnames(matchCountsData)) {
		checkClassifierGroundTruth(matchCountsData, foreignIfProbScoreAbove)
	}

	# Remove foreign/wrong-state candidates
	matchCountsData = removeOtherLocs(matchCountsData)

	print("After removing matches to other locations...")
	counts = summarizeRemainder(matchCountsData)

	# Remove probMetaForeign is high
	if (foreignIfProbScoreAbove < 1 && "probMetaForeign" %in% colnames(matchCountsData)) {
		# (can't check classifier against ground truth after removing foreign rows)

		print(paste0("Removing matches with a high (>", foreignIfProbScoreAbove, ") meta-data probability of being foreign [and unknown loc]..."))
		matchCountsData = matchCountsData[matchCountsData$probMetaForeign <= foreignIfProbScoreAbove | matchCountsData$newInferredLoc == matchCountsData$state,]

		counts = summarizeRemainder(matchCountsData)
	}

	print("Looking at city-level matches")
	# From here on, only consider matches unique at city level
	if ("city_count" %in% colnames(matchCountsData)) {
		matchCountsData = matchCountsData[matchCountsData$city_count == 1,]
	} else {
		print("Didn't find a city_count field, so assuming voters are already unique at city level")
	}
	counts = summarizeRemainder(matchCountsData, matchLevel = "city")

	# Get voters who are unique in city, have exactly one match that's to both the city and state, and have allowed number of blanks.
	# I.e., from counts, start with allowed number of blanks and at least 1 state match, then filter further.
	cityMatchVoters = rownames(counts[counts$rightStateFreq == 1 & counts$wrongLocFreq <= numBlanksAllowed,])
	cityMatchRows = matchCountsData[(matchCountsData$voter_id %in% cityMatchVoters & matchCountsData$newInferredLoc == matchCountsData$state & matchCountsData$cityMatchColumn == 1),]
	print(paste("[rule 5 matching] ", length(cityMatchVoters), "voters matched at both city and state level"))

	# Now remove these voters AND twitter acounts, and continue only with voters unique at state level
	print("Now filtering to voters having state_count == 1")
	if ("state_count" %in% colnames(matchCountsData)) {
		matchCountsData = matchCountsData[matchCountsData$state_count == 1,]
	} else {
		print("Didn't find a state_count field, so assuming voters are already unique at state level")
	}
	matchCountsData = matchCountsData[!(matchCountsData$voter_id %in% cityMatchVoters) & !(matchCountsData$twProfileID %in% cityMatchRows$twProfileID),]
	counts = summarizeRemainder(matchCountsData)

	votersWanted = rownames(counts[counts$rightStateFreq == 1 & counts$wrongLocFreq <= numBlanksAllowed,])
	print(paste("[rule 5 matching] ", length(votersWanted), "voters matched only at state level"))

	# identify the matching row for each of those voters
	rowsWanted = (matchCountsData$voter_id %in% votersWanted & matchCountsData$newInferredLoc == matchCountsData$state)
	
	matches = rbind(cityMatchRows, matchCountsData[rowsWanted, ])
	return(matches)

}

# Some steps common to several matching variants:
# (Making locationColName an arg only to make function easier to call from elsewhere.)
prepMatchDataRmExcluded = function(matchCountsData, locationColName="inferredLoc") {
    # make sure some columns are characters
    matchCountsData$newInferredLoc = as.character(matchCountsData[[locationColName]])
    matchCountsData$state = as.character(matchCountsData$state)
    
    # remove rows with a newInferredLoc of "foreign"
    matchCountsData = matchCountsData[matchCountsData$newInferredLoc != "foreign",]

    # remove matchesOtherState rows
    newLocLength = nchar(matchCountsData$newInferredLoc)   # only states have length 2
    differentState = (newLocLength == 2 & matchCountsData$newInferredLoc != matchCountsData$state)
    matchCountsData = matchCountsData[!differentState,]

	return(matchCountsData) 
}

# Exclude potential matches that are foreign or in a different state
matching3ExcludeForeign = function(matchCountsData, locationColName) {
    
	matchCountsData = prepMatchDataRmExcluded(matchCountsData, locationColName)
        
    # now, see which voters have exactly 1 matching row
    nameFreqs = table(matchCountsData$voter_id)
    rows_votersOccurringJustOnce = (matchCountsData$voter_id %in% names(nameFreqs)[nameFreqs==1])
    votersWithOneMatch = matchCountsData[rows_votersOccurringJustOnce,]

    # accept the match only if it's to the right state
    acceptThisMatch = which(votersWithOneMatch$state == votersWithOneMatch$newInferredLoc)
    
    matches = votersWithOneMatch[acceptThisMatch, ]
    return(matches)
    
}

# Like rule 3, use newInferredLoc and exclude foreign and other-state matches. Allow only 1 correct-state match.
# But this time, allow numBlanksAllowed confounders: records for which the location is blank, USA or unparsed.
matching4AllowBlanks = function(matchCountsData, numBlanksAllowed, locationColName) {
    
	matchCountsData = prepMatchDataRmExcluded(matchCountsData, locationColName)

    # get list of voters who have exactly 1 match to correct state
    rightStateFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc == matchCountsData$state])
    votersWithOneGoodMatch = names(rightStateFreq[rightStateFreq==1])
    print(paste("[rule4 matching] Found", length(votersWithOneGoodMatch), "voters who had exactly 1 correct-state match"))
    
    # get list of voters who have <= numBlanksAllowed matches to blank, USA or unparsed. 
    # (That's all that should be left after removing foreign and otherUS and not counting correct state.)
    unknownLocFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc == "USA" | 
                                                        matchCountsData$newInferredLoc == "" |
                                                        matchCountsData$newInferredLoc == "unparsed"])
    # This table missed voters who had 0 blank matches. Put in entries for them.
    entriesToAdd = setdiff(votersWithOneGoodMatch, names(unknownLocFreq))
    vectorToAdd = rep(0, length(entriesToAdd))
    names(vectorToAdd) = entriesToAdd
                      
    unknownAugmented = c(unknownLocFreq, vectorToAdd)
    votersWithAllowedBlankMatches = names(unknownAugmented[unknownAugmented <= numBlanksAllowed])
    print(paste("[rule4 matching] Found", length(votersWithAllowedBlankMatches), "voters with <=", numBlanksAllowed, "unknown loc matches"))
    
    okBoth = intersect(votersWithOneGoodMatch, votersWithAllowedBlankMatches)
    print(paste("[rule4 matching] ", length(okBoth), "voters satisfied both criteria"))
    
    # identify the matching row for each of those voters
    rowsWanted = (matchCountsData$voter_id %in% okBoth & matchCountsData$newInferredLoc == matchCountsData$state)
    
    matches = matchCountsData[rowsWanted, ]
    return(matches)
    
}

rule5CityLevelThenState = function(matchCountsData, numBlanksAllowed, locationColName) {
    # start out like rules 3-4, removing foreign and otherUS
	matchCountsData = prepMatchDataRmExcluded(matchCountsData, locationColName)

    # compute which loc fields also contain the correct city    
    locStrings = as.character(matchCountsData$twProfileLoc)
    citiesAsPatterns = paste0("\\b", matchCountsData$city, "\\b")
    locStringContainsCity = mapply(grepl, citiesAsPatterns, locStrings, MoreArgs=list(ignore.case=T, perl=T))
    # note: sometimes registered city is blank. Don't count those as matches:
    locStringContainsCity = locStringContainsCity & (mapply(nchar, matchCountsData$city) > 0)
    
    ### Match any rows in which state matches && locStringContainsCity && within numBlanksAllowed
    # (code modified from rule4)
    rightCityAndStateFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc == matchCountsData$state 
                                                    & locStringContainsCity])
    votersWithOneGoodMatch = names(rightCityAndStateFreq[rightCityAndStateFreq==1])
    print(paste("[rule5 matching] Found", length(votersWithOneGoodMatch), "voters who had exactly 1 correct-state + city match"))
    
    # get list of voters who have <= numBlanksAllowed matches to blank, USA or unparsed. 
    # (That's all that should be left after removing foreign and otherUS and not counting correct state.)
    unknownLocFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc == "USA" | 
                                                        matchCountsData$newInferredLoc == "" |
                                                        matchCountsData$newInferredLoc == "unparsed"])
    # This table missed voters who had 0 blank matches. Put in entries for them.
    entriesToAdd = setdiff(votersWithOneGoodMatch, names(unknownLocFreq))
    vectorToAdd = rep(0, length(entriesToAdd))
    names(vectorToAdd) = entriesToAdd
    
    unknownAugmented = c(unknownLocFreq, vectorToAdd)
    votersWithAllowedBlankMatches = names(unknownAugmented[unknownAugmented <= numBlanksAllowed])
    print(paste("[rule5 matching] Found", length(votersWithAllowedBlankMatches), "voters with <=", numBlanksAllowed, "unknown loc matches"))
    
    okBoth = intersect(votersWithOneGoodMatch, votersWithAllowedBlankMatches)
    print(paste("[rule5 matching] ", length(okBoth), "voters satisfied both criteria"))
    
    rowsWantedCityState = (matchCountsData$voter_id %in% okBoth & matchCountsData$newInferredLoc == matchCountsData$state & locStringContainsCity)
    cityStateMatches = matchCountsData[rowsWantedCityState,]
    rowsFromCSMatchPeople = (matchCountsData$voter_id %in% okBoth)
    matchCountsData = matchCountsData[!rowsFromCSMatchPeople,]
    
    ### Match any rows in which state matches && within numBlanksAllowed
    # (copied from rule4)
    # get list of voters who have exactly 1 match to correct state
    rightStateFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc == matchCountsData$state])
    votersWithOneGoodMatch = names(rightStateFreq[rightStateFreq==1])
    print(paste("[rule5 matching] Found", length(votersWithOneGoodMatch), "voters who had exactly 1 correct-state match"))
    
    # get list of voters who have <= numBlanksAllowed matches to blank, USA or unparsed. 
    # (That's all that should be left after removing foreign and otherUS and not counting correct state.)
    unknownLocFreq = table(matchCountsData$voter_id[matchCountsData$newInferredLoc == "USA" | 
                                                        matchCountsData$newInferredLoc == "" |
                                                        matchCountsData$newInferredLoc == "unparsed"])
    # This table missed voters who had 0 blank matches. Put in entries for them.
    entriesToAdd = setdiff(votersWithOneGoodMatch, names(unknownLocFreq))
    vectorToAdd = rep(0, length(entriesToAdd))
    names(vectorToAdd) = entriesToAdd
    
    unknownAugmented = c(unknownLocFreq, vectorToAdd)
    votersWithAllowedBlankMatches = names(unknownAugmented[unknownAugmented <= numBlanksAllowed])
    print(paste("[rule5 matching] Found", length(votersWithAllowedBlankMatches), "voters with <=", numBlanksAllowed, "unknown loc matches"))
    
    okBoth = intersect(votersWithOneGoodMatch, votersWithAllowedBlankMatches)
    print(paste("[rule5 matching] ", length(okBoth), "voters satisfied both criteria"))
    
    # identify the matching row for each of those voters
    rowsWantedState = (matchCountsData$voter_id %in% okBoth & matchCountsData$newInferredLoc == matchCountsData$state)
    
    matches = rbind(cityStateMatches, matchCountsData[rowsWantedState,])
    print(paste("[rule5 matching] ", nrow(matches), "matches total"))
    return(matches)
    
}

