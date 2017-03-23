
library(data.table)

# File to take preliminary matching results (from querying voters against the twitter DB) and convert them to 
# full-fledged matches, where each row = 1 voter + 1 twitter account.

# Notice: matchTheMatches only enables rule3 right now.

# matchFileIn: has one row per potential match, must contain the columns inferredLoc, personid, and twProfileID. Probably also has [voter's] state_code, twProfileName, twProfileHandle, twProfileLoc.
# voterfileIn: assumed to be one of the csv files, and it has to contain "personid".
# fullMatchFileOut: will be a .csv file.
matchTheMatches = function(voterfileIn, matchFileIn, fullMatchFileOut,
                           matchRulesVersion = 3) {
                            # match rules:  [1: match iff there's only 1 candidate match and it's to the right state]
                            #               [2: match iff there's only 1 candidate match (after excluding matches to other states)]
                            #               3: match iff there's only 1 candidate match (after excluding matches to "foreign" and other states)
                            #                   [requires format >= 3, which marks "foreign"]
                            #               4: exclude foreign and other states, and further, allow there to be up to N blank or unparsed records 
                            #                   as long as there's only 1 in the right state
                            #               5: recreate the procedure KJ used (using either the naive location parse or newInferredLoc)
    
	# normally only need 1 of the voter columns returned, personid, since going to merge with voter data anyway.
    matchingColsToKeep = c("personid", "twProfileID", "twProfileName", "twProfileHandle", "twProfileLoc")
	# But for testing/inspection purposes, may want others:
	#matchingColsToKeep = c("personid", "first_name", "last_name", "reg_address_city", "state_code", "twProfileID", "twProfileName", "twProfileHandle", "twProfileLoc")
	locationColName = "inferredLoc"

	# 0. Read data and preprocess if needed
    matchCountsData = as.data.frame(fread(matchFileIn))

    # If input data includes voters with blank cities, drop them. (Will try to prevent this at earlier steps.)
    if ("reg_address_city" %in% colnames(matchCountsData) & sum(matchCountsData$reg_address_city == '') > 0) {
        matchCountsData = matchCountsData[matchCountsData$reg_address_city != '',]
    }

    
    # 1. Decide which matches are reliable (depending on the matching rules to use).
    
    # Do the matching! (Exciting part.)
	# 	matchRulesVersion 1 was: conservative / original: only allow the match when there's exactly 1 match on twitter and it's to the right state
	# 	matchRulesVersion 2 was: Ignore any candidate matches for which we know the location and it's a different US state
    if (matchRulesVersion == 3) {
        # Additionally exclude "foreign" locs (requires more recent loc computation).
		twMatches = matching3ExcludeForeign(matchCountsData, locationColName, matchingColsToKeep)
    }
    
    # 2. Ensure uniqueness: if >= 2 voters match to the same twitter account, drop these rows.
    twitterAcctFreqs = table(twMatches$twProfileID)
    rows_twitterOccurringJustOnce = (as.character(twMatches$twProfileID) %in% names(twitterAcctFreqs)[twitterAcctFreqs==1])
    twMatches = twMatches[rows_twitterOccurringJustOnce,]
    
    # 3. Merge with the original voter data
    voterData = as.data.frame(fread(voterfileIn))
    #voterData = read.csv(file=voterfileIn, header=T) #, stringsAsFactors=F) #colClasses = "character")
    allDataMatched = merge(twMatches, voterData, by.x="personid", by.y="personid", all.x=T)
    
    # sanity check: specifying all.x shouldn't have created any blank fields
    if (sum(is.na(allDataMatched$last_name))) {
        warning("Hrmm. Some records were not properly matched to the voter data.")
    }
    
    # 3b. If voter data includes "nameChanged" column, remove rows for which it's true, then remove the column.
    if ("nameChanged" %in% colnames(voterData)) {
        allDataMatched = allDataMatched[allDataMatched$nameChanged==0,
                                        !(colnames(allDataMatched) %in% "nameChanged")]
    }
        
        
    
    # 4. Write out
    print(paste("Used matching rules", matchRulesVersion, "and got", nrow(allDataMatched), "matches."))
    write.csv(allDataMatched, file=fullMatchFileOut, row.names=F, na="")
    print(paste("Output saved as", fullMatchFileOut))
}

# Utility code for tallying how many candidate matches remain under various criteria
howManyCandidates = function(matchCountsSubset) {
    nameFreqs = table(matchCountsSubset$personid)
	t = table(nameFreqs)
	t2 = t[sort(as.numeric(names(t)))]
	print(t2)
	return(t2)
}


# Some steps common to several matching variants:
prepMatchDataRmExcluded = function(matchCountsData, locationColName="inferredLoc") {
    # make sure some columns are characters
    matchCountsData$newInferredLoc = as.character(matchCountsData[[locationColName]])
    matchCountsData$state_code = as.character(matchCountsData$state_code)
    
    # remove rows with a newInferredLoc of "foreign"
    matchCountsData = matchCountsData[matchCountsData$newInferredLoc != "foreign",]

    # remove matchesOtherState rows
    newLocLength = nchar(matchCountsData$newInferredLoc)   # only states have length 2
    differentState = (newLocLength == 2 & matchCountsData$newInferredLoc != matchCountsData$state_code)
    matchCountsData = matchCountsData[!differentState,]

	return(matchCountsData) 
}

# Exclude potential matches that are foreign or in a different state
matching3ExcludeForeign = function(matchCountsData, locationColName, fieldNamesToKeep) {
    
	matchCountsData = prepMatchDataRmExcluded(matchCountsData, locationColName)
        
    # now, see which voters have exactly 1 matching row
    nameFreqs = table(matchCountsData$personid)
    rows_votersOccurringJustOnce = (matchCountsData$personid %in% names(nameFreqs)[nameFreqs==1])
    votersWithOneMatch = matchCountsData[rows_votersOccurringJustOnce,]

    # accept the match only if it's to the right state
    acceptThisMatch = which(votersWithOneMatch$state_code == votersWithOneMatch$newInferredLoc)
    
    fieldsWanted = (colnames(votersWithOneMatch) %in% fieldNamesToKeep)
    matches = votersWithOneMatch[acceptThisMatch, fieldsWanted]
    return(matches)
    
}

# Like rule 3, use newInferredLoc and exclude foreign and other-state matches. Allow only 1 correct-state match.
# But this time, allow numBlanksAllowed confounders: records for which the location is blank, USA or unparsed.
matching4AllowBlanks = function(matchCountsData, numBlanksAllowed, locationColName, fieldNamesToKeep) {
    
	matchCountsData = prepMatchDataRmExcluded(matchCountsData, locationColName)
    
    # get list of voters who have exactly 1 match to correct state
    rightStateFreq = table(matchCountsData$personid[matchCountsData$newInferredLoc == matchCountsData$state_code])
    votersWithOneGoodMatch = names(rightStateFreq[rightStateFreq==1])
    print(paste("[rule4 matching] Found", length(votersWithOneGoodMatch), "voters who had exactly 1 correct-state match"))
    
    # get list of voters who have <= numBlanksAllowed matches to blank, USA or unparsed. 
    # (That's all that should be left after removing foreign and otherUS and not counting correct state.)
    unknownLocFreq = table(matchCountsData$personid[matchCountsData$newInferredLoc == "USA" | 
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
    rowsWanted = (matchCountsData$personid %in% okBoth & matchCountsData$newInferredLoc == matchCountsData$state_code)
    
    fieldsWanted = (colnames(matchCountsData) %in% fieldNamesToKeep)
    matches = matchCountsData[rowsWanted, fieldsWanted]
    return(matches)
    
}

rule5CityLevelThenState = function(matchCountsData, numBlanksAllowed, locationColName, fieldNamesToKeep) {
    # start out like rules 3-4, removing foreign and otherUS
	matchCountsData = prepMatchDataRmExcluded(matchCountsData, locationColName)

    # compute which loc fields also contain the correct city    
    locStrings = as.character(matchCountsData$twProfileLoc)
    citiesAsPatterns = paste0("\\b", matchCountsData$reg_address_city, "\\b")
    locStringContainsCity = mapply(grepl, citiesAsPatterns, locStrings, MoreArgs=list(ignore.case=T, perl=T))
    # note: sometimes registered city is blank. Don't count those as matches:
    locStringContainsCity = locStringContainsCity & (mapply(nchar, matchCountsData$reg_address_city) > 0)
    
    ### Match any rows in which state matches && locStringContainsCity && within numBlanksAllowed
    # (code modified from rule4)
    rightCityAndStateFreq = table(matchCountsData$personid[matchCountsData$newInferredLoc == matchCountsData$state_code 
                                                    & locStringContainsCity])
    votersWithOneGoodMatch = names(rightCityAndStateFreq[rightCityAndStateFreq==1])
    print(paste("[rule5 matching] Found", length(votersWithOneGoodMatch), "voters who had exactly 1 correct-state + city match"))
    
    # get list of voters who have <= numBlanksAllowed matches to blank, USA or unparsed. 
    # (That's all that should be left after removing foreign and otherUS and not counting correct state.)
    unknownLocFreq = table(matchCountsData$personid[matchCountsData$newInferredLoc == "USA" | 
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
    
    rowsWantedCityState = (matchCountsData$personid %in% okBoth & matchCountsData$newInferredLoc == matchCountsData$state_code & locStringContainsCity)
    cityStateMatches = matchCountsData[rowsWantedCityState,]
    rowsFromCSMatchPeople = (matchCountsData$personid %in% okBoth)
    matchCountsData = matchCountsData[!rowsFromCSMatchPeople,]
    
    ### Match any rows in which state matches && within numBlanksAllowed
    # (copied from rule4)
    # get list of voters who have exactly 1 match to correct state
    rightStateFreq = table(matchCountsData$personid[matchCountsData$newInferredLoc == matchCountsData$state_code])
    votersWithOneGoodMatch = names(rightStateFreq[rightStateFreq==1])
    print(paste("[rule5 matching] Found", length(votersWithOneGoodMatch), "voters who had exactly 1 correct-state match"))
    
    # get list of voters who have <= numBlanksAllowed matches to blank, USA or unparsed. 
    # (That's all that should be left after removing foreign and otherUS and not counting correct state.)
    unknownLocFreq = table(matchCountsData$personid[matchCountsData$newInferredLoc == "USA" | 
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
    rowsWantedState = (matchCountsData$personid %in% okBoth & matchCountsData$newInferredLoc == matchCountsData$state_code)
    
    fieldsWanted = (colnames(matchCountsData) %in% fieldNamesToKeep)
    matches = rbind(cityStateMatches, matchCountsData[rowsWantedState,])[, fieldsWanted]
    print(paste("[rule5 matching] ", nrow(matches), "matches total"))
    return(matches)
    
}

