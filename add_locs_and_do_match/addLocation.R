
library(data.table)
library(bit64)
source("processLocations.R")	# for isLocationInUSA and its helper fns

# Code to add a "inferredLoc" column to candidate matches.
# addLocToFile() runs on one file.
# runDirectory() loops over the output files from mySQL_load_retrieve/countTwitterVoterMatches().
# grabAll() rbinds and saves as a single file.

# to execute: 
# inDir = "~/twitter_matching/mySQL_load_retrieve/data"
# outDir = "~/twitter_matching/add_locs_and_do_match/data/candidateMatchFiles"
# finalOutfile = "~/twitter_matching/add_locs_and_do_match/data/allCandidateMatchLocs2M.csv"
# system.time(runDirectory(inDir, outDir)) 	# quick! Took 55 minutes for all 1000 files (from the 2M voters).
# grabAll(outDir, 1000, finalOutfile)

# and finally, rbind all of them:
grabAll = function(dataDir, numFiles, outFile) {
	listOfDataTables = vector(mode="list", length=numFiles)
	infiles = paste0(dataDir, "/candMatchLocs-", 1:numFiles, ".csv")
	for (i in 1:numFiles) {
		listOfDataTables[[i]] = fread(infiles[i])
		# need to explicitly convert twitterID to integer64 for all tables, else some treat it as numeric
		listOfDataTables[[i]] = listOfDataTables[[i]][, twProfileID := as.integer64(twProfileID)]	# note unfamiliar syntax. Seems to mean "replace one col, while keeping the others"
	}
	allTogether = rbindlist(listOfDataTables)
	fwrite(allTogether, file=outFile)
}

runDirectory = function(inDir, outDir, numFiles=1000) {
	placeListDir = "data/placeLists/"
	inParallel = T
	timeProfiling = T

	initSnowfall()

	for (i in 1:numFiles) {
		infile = file.path(inDir, paste0("candMatches-", i, ".txt"))
		outfile = file.path(outDir, paste0("candMatchLocs-", i, ".csv"))
		print(paste("Starting file", i))
		addLocToFile(infile, outfile, placeListDir, inParallel=inParallel, timeProfiling=T)
	}
	sfStop()

	print("all done!")

}

initSnowfall = function() {
	library(snowfall)
	sfInit(parallel=TRUE,cpus=10)
	# all the components for making isLocationInUSA work:
	sfLibrary(stringi)
	sfExport("isLocationInUSA", "checkForUSA", "checkForUSRegions", "containsNY_SF", "isExactlyCityOrState", "looksLikeCityState", 
		"handleSpecialCases", "hasForeignLoc", "remapCoding")
}

addLocToFile = function(infile, outfile, placeListDir, inParallel=F, timeProfiling=T) {
	if (timeProfiling) {
		startTime = Sys.time()
	}

	places = readKnownPlaceLists(placeListDir)
	# experimental: remove diacritical marks here and now from latin chars
	places$countryList = unique(stri_trans_general(places$countryList, "latin-ascii"))
		
	candMatches = as.data.table(fread(infile, skip=1))		# header doesn't have enough fields; easier to paste on correct names here.
	headerStart = read.table(infile, nrows=1)
	colnames(candMatches)[1:ncol(headerStart)] = sapply(headerStart[1,], as.character)

	print(paste("Read", nrow(candMatches), "lines of matches to process"))
	if (timeProfiling) {
		print(paste("Time since start:", Sys.time()-startTime, units(Sys.time()-startTime)))
	}

	# Infer locations
	locStrings = candMatches$twProfileLoc

	fullInferredLocVector = as.character(locStrings)
	# change input NAs to "" 
	fullInferredLocVector[is.na(fullInferredLocVector)] = rep("", sum(is.na(fullInferredLocVector)))

	# Only need to look at nonBlank loc strings
	hasLocString = (fullInferredLocVector != "")
	nonBlankLocStrings = fullInferredLocVector[hasLocString]
	nonBlankLocMatchCands = candMatches[hasLocString,]

	inferredLocs = computeLoc2Parallel(nonBlankLocStrings, places, inParallel)
	# not actually going to write these out
	#fullInferredLocVector[hasLocString] = inferredLocs
	#candMatches$loc2 = fullInferredLocVector

	if (timeProfiling) {
		print(paste("computed loc2 at", Sys.time()-startTime, units(Sys.time()-startTime)))
	}

	locStringContainsCity = locContainsCity(nonBlankLocMatchCands$reg_address_city, nonBlankLocMatchCands$twProfileLoc, places, inParallel)
	candMatches$locContainsCity = rep(0, nrow(candMatches))
	candMatches$locContainsCity[hasLocString] = as.numeric(locStringContainsCity)

	finalLocs = updateLocInferenceParallel(nonBlankLocMatchCands, inferredLocs, places, inParallel, locStringContainsCity)
	fullInferredLocVector[hasLocString] = finalLocs
	candMatches$inferredLoc = fullInferredLocVector
	if (timeProfiling) {
		print(paste("updated locs for matches at", Sys.time()-startTime, units(Sys.time()-startTime)))
	}

	# Save file for later.
	#outCon = gzfile(outfile, open="wt")
	#write.csv(all, file=outCon, row.names=F)
	#close(outCon)
	write.csv(candMatches, file=outfile, row.names=F)
	print(paste("Wrote matches & inferred locs to", outfile))

}


# For efficiency, only send in non-blank loc strings
computeLoc2Parallel = function(locStrings, places, inParallel = T) {

	if (inParallel) {
		sfExport("places", "locStrings")
		newlyInferredLocs = sfSapply(locStrings, fun=isLocationInUSA, placeLists=places) 
	} else {
		newlyInferredLocs = sapply(locStrings, FUN=isLocationInUSA, placeLists=places)
	}

    # In case this returned NAs, which really mean "" in this context
    newlyInferredLocs[is.na(newlyInferredLocs)] = rep("", sum(is.na(newlyInferredLocs)))

    return(newlyInferredLocs)
}

# Function takes loc2 as input (inferredLocs), updates it as a function of the candidate match.
# Assumes matchData contains columns for city, state and location (should modify function to pass in their colnames--see below).
# For efficiency, we should only send in non-blank loc strings.
#
# Summary of rules for updating location (see below for details):
# Look for voter's city and state within twitter location.
# -If original parse mismatched but city + state are present, it's a match.
# -If twitter_location == city, it's a match.
# -If (original parse was unclear || original parse just noticed NYC) but (city || state) are present, it's a match.
# -If original parse was another state but isn't clear-cut, and city is present, it's a match.
# -If original parse was foreign but (city || state) are present, downgrade location to "unparsed."
#
updateLocInferenceParallel = function(matchData, inferredLocs, places, inParallel=T, locStringContainsCity=NULL) {

	matchData$city = as.character(matchData$reg_address_city)
	matchData$state = as.character(matchData$state_code)
	matchData$location = as.character(matchData$twProfileLoc)

	if (is.null(locStringContainsCity)) {
		locStringContainsCity = locContainsCity(matchData$city, matchData$location, places, inParallel)
		if (F && timeProfiling) {
			print(paste("finished locContainsCity at", Sys.time()-startTime, units(Sys.time()-startTime)))
		}
	}
	locStringContainsState = locContainsState(matchData$state, matchData$location, places, inParallel)
	if (F && timeProfiling) {
		print(paste("finished locContainsState at", Sys.time()-startTime, units(Sys.time()-startTime)))
	}

	newLocs = inferredLocs

    # case 0: if matchesCity and matchesState, it's a match.
    matchesCityAndState = locStringContainsCity & locStringContainsState & (inferredLocs != matchData$state)
    
    # cases 1 and 2 (= by far the biggest source of new matches)
    # 1. if inferredLoc is "unparsed" or "USA", accept the city-match or the state-match (as helping us parse)
    # 2. if locString equals the city (with no other info), use the city-match.
    useCityOrStateMatch = (inferredLocs != matchData$state) & (locStringContainsCity | locStringContainsState) & 
                    (inferredLocs == "unparsed" | inferredLocs == "USA" |                 # case 1
                        tolower(matchData$location) == tolower(matchData$city))    # case 2
	if (F && timeProfiling) {
		print(paste("finished cases 0-2 at", Sys.time()-startTime, units(Sys.time()-startTime)))
	}

    # case 3: if original parse was another state but city is present, 
    # use the city-match UNLESS it parses very clearly as the other state.
    case3 = locStringContainsCity & (inferredLocs != matchData$state) & (nchar(inferredLocs) == 2)  

	# parallelize cityGuess. For efficiency, only call it for case3.
	mat = cbind(matchData$location[case3], inferredLocs[case3])		# matrix of strings
	if (inParallel) {
		sfExport("mat", "getInferredCity2")			# helper fns for getInferredCity already there from top of file
	    cityGuess = sfApply(mat, 1, function(x) getInferredCity2(x[1], x[2], stateAbbrevs=places$stateAbbrevs, cityStateAbbrevs=places$cityStateAbbrevs))
	} else {
	    cityGuess = apply(mat, 1, function(x) getInferredCity2(x[1], x[2], stateAbbrevs=places$stateAbbrevs, cityStateAbbrevs=places$cityStateAbbrevs))

	}

	# cityGuess is the natural parse. We're only interested here in rows containing a match that's not the natural parse.
    case3UseCityMatchRowIDs = (which(case3))[tolower(cityGuess)!=tolower(matchData$city[case3])]
    # change the short vector of row ids into a long vector of bools
    case3UseCityMatch = (1:nrow(matchData)) %in% case3UseCityMatchRowIDs

	if (F && timeProfiling) {
		print(paste("finished case 3 at", Sys.time()-startTime, units(Sys.time()-startTime)))
	}

    # case 4: If inferredState is NY because twProfileLoc contains "NYC", but (city || state) are present, it's a match.
	# I.e., if (original parse was unclear || original parse just noticed NYC) but (city || state) are present, it's a match.
	if (inParallel) {
		locStr = matchData$location
		sfExport("locStr")
		containsNYC = sfSapply(locStr, function(x) grepl("nyc", x, ignore.case=T))
	} else {
		containsNYC = grepl("nyc", matchData$location, ignore.case=T)
	}
    case4 = (inferredLocs == "NY") & containsNYC & (inferredLocs != matchData$state) & (locStringContainsState | locStringContainsCity)
	if (F && timeProfiling) {
		print(paste("finished case 4 at", Sys.time()-startTime, units(Sys.time()-startTime)))
	}

    # Do it!
    itsAMatch = matchesCityAndState | useCityOrStateMatch | case3UseCityMatch | case4
    newLocs[itsAMatch] =  matchData$state[itsAMatch]

    case5 = (locStringContainsCity | locStringContainsState) & (inferredLocs == "foreign") & !itsAMatch
	newLocs[case5] = rep("unparsed", sum(case5))

	return(newLocs)

}

# Modification(!!) of the one in matchFinish: return "" instead of NULL
# carefully walk through preprocessing code that can get us to the city/state call, just as was done in isLocationInUSA.
# We can assume we got some state out of this string last time.
getInferredCity2 = function(locString, inferredStateSeen, stateAbbrevs, cityStateAbbrevs) {
    locString = stri_trans_nfkc(locString)
    locString = stri_replace_all(locString, " ", charclass="[\\p{S}\\p{C}]")
    locString = stri_replace(locString, "", regex="\\p{Space}+$")
    locString = stri_replace(locString, "", regex="[\\p{P}\\p{Z}\\p{M}]+$")
    
    stub = checkForUSA(locString)
    stub = checkForUSRegions(stub$remainingLocString) 
    locString = sub("\\bmetro\\b", "", stub$remainingLocString, ignore.case=T, perl=T)
    locString = sub("\\barea\\b", "", locString, ignore.case=T, perl=T)
    
    locString = sub("^\\s+", "", locString, perl=T)
    locString = sub("\\s+$", "", locString, perl=T)
    locString = stri_replace(locString, "", regex="[\\p{P}\\p{Z}\\p{M}]+$")
    
    stub = isExactlyCityOrState(locString, cityStateAbbrevs, stateAbbrevs)  # if stub$hasLoc, we're back in case 2
    if (!stub$hasLoc && nchar(locString) > 0) {
        stub = looksLikeCityState(locString, stateAbbrevs) 
        if (stub$hasLoc && inferredStateSeen == stub$inferredLoc) {   # if that part doesn't match, we haven't parsed it like the real function does
            return(stub$cityGuess)
        }
    }
    return("")
}

# city and locString must be character class
locContainsCity = function(cityVector, locStringVector, places, inParallel=T) {
    citiesAsPatterns = paste0("\\b", cityVector, "\\b")

	# to parallelize, can't use mapply. Since there are 2 args, try making it into a matrix and using apply.
	mat = cbind(citiesAsPatterns, locStringVector)		# a matrix of strings
	if (inParallel) {
		sfExport("mat")
		locStringContainsCity = sfApply(mat, 1, function(x) grepl(x[1], x[2], ignore.case=T, perl=T))
	} else {
		locStringContainsCity = apply(mat, 1, function(x) grepl(x[1], x[2], ignore.case=T, perl=T))
	}
	return(locStringContainsCity)

}
locContainsState = function(stateVector, locStringVector, places, inParallel=T) {
	# See if the twitter loc contains the voter's state.
	# Each state code can expand to either itself, itself with periods in between, the full state, or any abbrevs 
	# listed in places$stateAbbrevs.
	statesAsPatterns = stateVector

	# easiest to code is as a loop over 50 states
	for (state in unique(places$stateAbbrevs$stateAbbrev)) {
		# grab all the long words
		stateRE1 = paste(places$stateAbbrevs$stateLong[places$stateAbbrevs$stateAbbrev==state], collapse="|")
		# append the two-letter abbrev (with or without periods)
		letter1 = substr(state, 1, 1)
		letter2 = substr(state, 2, 2)
		stateRE2 = paste0("\\b(", letter1, "\\.?", letter2, "|", stateRE1, ")\\b")
		if (state == "IN" || state == "OR" || state == "OH" || state == "DE") {
			# the two-letter abbreviations could be words, so don't match on those
			stateRE2 = paste0("\\b(", stateRE1, ")\\b")
		} else if (state == "WA") {
			# don't allow a match on just "Washington," or we'll get DC places
			stateRE2 = sub("washington", "washington state", stateRE2)
		} else if (state == "VA") {
			# don't allow a match on just "Virginia," or we'll get West Virginia places too. Match is ok at the start of 
			# a string though.
			stateRE2 = sub("virginia", "^virginia", stateRE2)
		}
		
		statesAsPatterns[statesAsPatterns==state] = rep(stateRE2, sum(statesAsPatterns==state))
	}

	mat2 = cbind(statesAsPatterns, locStringVector)		# matrix of strings
	if (inParallel) {
		sfExport("mat2")
		locStringContainsState = sfApply(mat2, 1, function(x) grepl(x[1], x[2], ignore.case=T, perl=T))
	} else {
		locStringContainsState = apply(mat2, 1, function(x) grepl(x[1], x[2], ignore.case=T, perl=T))
	}

	return(locStringContainsState)

}


