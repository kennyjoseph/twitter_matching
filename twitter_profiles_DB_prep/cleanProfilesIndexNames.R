

library(data.table)
source("nameChunking.R")

# for speed:
library(compiler)
enableJIT(3)  # higher numbers for more aggressive JITting
options(warn=1)  # print warnings as they occur


# Code that takes as input a delimited file of Twitter profiles and 
# 1. Drops unambiguously foreign accounts (optionally writing them to a separate file)
# 2. Computes name and handle words to index, as new columns
# See bottom of file for example usage. Main function is preprocessProfileTextFile().


# New! Do the same as before, but (1) parallelized, and (2) adapted to file format of ~kjoseph/profile_study/.
preprocessProfileTextFileParallel = function(inFile, outFile, outFileForDroppedForeign=NULL,
                           appendToOutputFiles=F, inputIsCompressed=T, compressOutput=T,
                           chunkSize=500000, pruneMoreForeign=T, timeProfiling=T, inParallel=T) {

	## Set up parallel stuff ##
	if (inParallel) {
			require(snowfall)
			sfInit(parallel=TRUE,cpus=10)
			# components for making isDefinitelyForeign work:
			sfLibrary(stringi)
			sfLibrary(data.table)
			sfExport("onlyForeignAlphabet", "isDefinitelyForeign")
			# components for making putThemTogether work:
			sfExport("putThemTogether", "getWordsFromTwitterHandle", "splitHandleNative", "getWordsFromTwitterName",
					"getWordsFromTwitterNameForHandleMatching", "processNameText", "getHandleChunksUsingNameWords",
					"assembleAlignment", "getContinguousChunksFromDataStructure", "getMatchedChunksFromDataStructure",
					"itemizeAvailableChunks")
	}


	## Open file connections ##
	
	# Preserve functionality for:
	# -optionally compressed input/output
	# -files that are larger than we want to read into memory at once

    if (inputIsCompressed == "zip") {
        # unz() reads .zip files. We'll assume there's only 1 file inside the archive.
		# caution! This command (via its implementation of unzip) limits file sizes to 4GB, then gives an "end of file."
        con = unz(inFile, file=unzip(inFile, list=T)$Name, open="rt", encoding="UTF-8")
    } else {
        # gzfile can read .gz, others, and uncompressed too
        con = gzfile(inFile, open="rt", encoding="UTF-8")
    }
    
    if (!compressOutput) {
        if (appendToOutputFiles) {
            conOut = file(outFile, open="at")
        } else {
            conOut = file(outFile, open="wt")
        }
    } else {
        if (appendToOutputFiles) {
            conOut = gzfile(outFile, open="at")
        } else {
            conOut = gzfile(outFile, open="wt")
        }
    }
    
    if (!is.null(outFileForDroppedForeign)) {
        # always compressed
        if (appendToOutputFiles) {
            conOut2 = gzfile(outFileForDroppedForeign, open="at")
        } else {
            conOut2 = gzfile(outFileForDroppedForeign, open="wt")
        }
    }
    
	## Which column is which, and which do we keep? (Hard-coded) ##
	# (Note: different columns and order than before, so DB loading will need to adjust)

	# Format is tab-separated (with embedded null characters), with these columns:
	inputColNames = c("id", "name", "handle", "url", "is_protected", "location", "description", "num_followers", "num_following", 
					"date_created", "tz_offset", "tz_name", "num_tweets", "profile_lang", "date_last_seen", "coords", "tweet_lang",
					"pic_url", "is_verified")

    colNamesToPrint = inputColNames[c(1,2,3,6,7,4,  # basics, including location, description and url
									 10,15,			# dates
									 8,9,13,5,19, 	# profile stats
									 11,12,14,17,16)]	# time zone, langs, coords
	colNamesToPrint2 = c(colNamesToPrint, "nameHandleWords") # use this for the new file; above one for foreign lines
	dateColNames = inputColNames[c(10,15)]
	colNamesToQuote = c(inputColNames[c(2,6,7,12,16)], "nameHandleWords")


	## Start reading and processing ## 

	if (timeProfiling) {
			startTime = Sys.time()
	}

	chunkCnt = 1
    numForeignDropped = 0
    numLinesKept = 0
	while (!isIncomplete(con)) {

		if (timeProfiling) {
				print(paste("About to read chunk", chunkCnt, "of profiles at", Sys.time()-startTime, units(Sys.time()-startTime)))
		}

		accountLines = as.data.table(matrix(
										# odd syntax, since I can't figure out how to get scan to put things into a list with length > 1
										scan(con, what=as.list(""), nlines=chunkSize, sep="\t", quiet=T, na.strings=NULL, skipNul=T, quote="\"", multi.line=F)[[1]], 
										byrow=T, nrow=chunkSize))
		colnames(accountLines) = inputColNames

		print(paste("Read", nrow(accountLines), "lines of profiles to process"))
		if (timeProfiling) {
				print(paste("Time since start:", Sys.time()-startTime, units(Sys.time()-startTime)))
		}

		# write headers
		if (chunkCnt == 1) {
			writeLines( paste(colNamesToPrint2, collapse=","), con=conOut)
            if (!is.null(outFileForDroppedForeign)) {
				writeLines( paste(colNamesToPrint, collapse=","), con=conOut2)
			}
		}

        # Change any embedded newlines to spaces (maybe data.table is fast enough)
		colsToFix = c("name", "handle", "location", "description")
		accountLines = cbind(accountLines[, -colsToFix, with=F], accountLines[, lapply(.SD, gsub, pattern="\\n", replacement=" "), .SDcols=colsToFix])

		# Change any occurrences of "None" to "" in fields where it sometimes occurs. (Most other fields can already appear as "".)
		fieldsWithNone = c("url", "tz_offset", "tz_name")
		accountLines = cbind(accountLines[, -fieldsWithNone, with=F], accountLines[, lapply(.SD, gsub, pattern="^None$", replacement=""), .SDcols=fieldsWithNone])
		if (timeProfiling) {
				print(paste("did some gsubs, about to detect foreign profiles at", Sys.time()-startTime, units(Sys.time()-startTime)))
		}

        ## Check for and discard foreign rows ##
		if (inParallel) {
			mat = accountLines[, .(profile_lang, tweet_lang, tz_offset, name, location, tz_name)]
			if (pruneMoreForeign) {
				definitelyForeign = sfApply(mat, 1, function(x) isDefinitelyForeign(x[1], x[2], x[3], x[4], x[5], x[6]))
			} else {
				definitelyForeign = sfApply(mat, 1, function(x) isDefinitelyForeign(x[1], x[2], x[3], x[4], x[5]))
			}
		} else {
			mat = accountLines[, .(profile_lang, tweet_lang, tz_offset, name, location, tz_name)]
			if (pruneMoreForeign) {
				definitelyForeign = apply(mat, 1, function(x) isDefinitelyForeign(x[1], x[2], x[3], x[4], x[5], x[6]))
			} else {
				definitelyForeign = apply(mat, 1, function(x) isDefinitelyForeign(x[1], x[2], x[3], x[4], x[5]))
			}
		}
		if (sum(definitelyForeign) > 0) {
			numForeignDropped = numForeignDropped + sum(definitelyForeign)
            if (!is.null(outFileForDroppedForeign)) {
				# write.table handles embedded quotes (but write.csv doesn't do appending?)
                write.table( accountLines[definitelyForeign, colNamesToPrint, with=F], file=conOut2, quote=which(colNamesToPrint %in% colNamesToQuote),
							sep=",", row.names=F, col.names=F, qmethod="d")
			}
			accountLines = accountLines[!definitelyForeign,]
		}
		if (timeProfiling) {
				print(paste("separated out", sum(definitelyForeign), "foreign profiles at", Sys.time()-startTime, units(Sys.time()-startTime)))
		}

        ## Create index terms ##
		if (inParallel) {
			nameHandleWords = sfApply(accountLines[, .(name, handle)], 1, 
									FUN=function(x) { 
										x1 = putThemTogether(x[1], x[2], printSummary=F); 
										paste(c(x1$nameWords, x1$handleWords), collapse=" ")
									} )
		} else {
			#listOfIndexFields = apply(accountLines[, .(name, handle)], 1, FUN=function(x) putThemTogether(x[1], x[2], printSummary=F))
			#nameHandleWords = t(apply(accountLines[, .(name, handle)], 1, 
			#						FUN=function(x) { 
			#							x1 = putThemTogether(x[1], x[2], printSummary=F); 
			#							c(paste(x1$nameWords, collapse=" "), paste(x1$handleWords, collapse=" ")) 
			#						} ))

			# hey -- actually, no good reason to keep putting the name vs. handle into separate columns
			nameHandleWords = apply(accountLines[, .(name, handle)], 1, 
									FUN=function(x) { 
										x1 = putThemTogether(x[1], x[2], printSummary=F); 
										paste(c(x1$nameWords, x1$handleWords), collapse=" ")
									} )
		}
		accountLines = cbind(accountLines, nameHandleWords=nameHandleWords)
		if (timeProfiling) {
				print(paste("done with index terms at", Sys.time()-startTime, units(Sys.time()-startTime)))
		}

		# Replace True/False with TRUE/FALSE (to make SQL happy later)
		booleanFields = c("is_protected", "is_verified")
        accountLines = cbind(accountLines[, -booleanFields, with=F], accountLines[, lapply(.SD, toupper), .SDcols=booleanFields])

        # Shorten time zone spec (truncate to: Eastern, Central, Mountain, Pacific)
		accountLines = cbind(accountLines[, -"tz_name", with=F], accountLines[, .(tz_name=sub(" Time \\(US \\& Canada\\)", "", tz_name))])

		if (timeProfiling) {
				print(paste("about to fix date and coords at", Sys.time()-startTime, units(Sys.time()-startTime)))
		}
         # Shorten date formatting
        fieldsWithDates = c("date_created", "date_last_seen")
        accountLines = cbind(accountLines[, -fieldsWithDates, with=F], accountLines[, lapply(.SD, makeDate), .SDcols=fieldsWithDates])

        # Fix coords, if present
		accountLines = cbind(accountLines[, -"coords", with=F], accountLines[, .(coords=sapply(coords, fixGPS))]) 

		# (Do I need to check for NAs?)


		## Write out ##

		if (timeProfiling) {
				print(paste("about to write out chunk", chunkCnt, "(", nrow(accountLines), " lines) at", Sys.time()-startTime, units(Sys.time()-startTime)))
		}
		write.table( accountLines[, colNamesToPrint2, with=F], file=conOut, quote=which(colNamesToPrint2 %in% colNamesToQuote), 
						sep=",", row.names=F, col.names=F, qmethod="d")
		flush(conOut)
        numLinesKept = numLinesKept + nrow(accountLines)
		chunkCnt = chunkCnt + 1
	}

    close(con)
    close(conOut)
    if (!is.null(outFileForDroppedForeign)) {
        close(conOut2)
    }
    print(paste("Done! (Kept", numLinesKept, "entries and dropped", numForeignDropped, "foreign ones.)"))

}


# Function used in Feb 2016 to handle text file containing profile info: 
# -discard rows that are definitely foreign
# -add index terms for name words (as 2 columns: nameWords, handleWords)
# Assumes input file is a (compressed) csv containing embedded newlines.
# Notice that column positions are hard-coded inside the function.
preprocessProfileTextFile = function(inFile, outFile, 
                           inputIsCompressed=T, compressOutput=T,
                           appendToOutputFiles=F, outFileForDroppedForeign=NULL,
                           startWhenID=NULL, stopAtID=NULL, pruneMoreForeign=T,
						   goodMatchField=T, onlyDoIDsInHash=NULL) {
    
    if (inputIsCompressed == "zip") {
        # unz() reads .zip files. We'll assume there's only 1 file inside the archive.
	# caution! This command (via its implementation of unzip) limits file sizes to 4GB, then gives an "end of file."
        con = unz(inFile, file=unzip(inFile, list=T)$Name, open="rt", encoding="UTF-8")
    } else {
        # gzfile can read .gz, others, and uncompressed too
        con = gzfile(inFile, open="rt", encoding="UTF-8")
    }
    
    if (!is.null(startWhenID)) {
        okToStart = F
    } else { 
        okToStart = T
    }
    
    if (!compressOutput) {
        if (appendToOutputFiles) {
            conOut = file(outFile, open="at")
        } else {
            conOut = file(outFile, open="wt")
        }
    } else {
        if (appendToOutputFiles) {
            conOut = gzfile(outFile, open="at")
        } else {
            conOut = gzfile(outFile, open="wt")
        }
    }
    
    if (!is.null(outFileForDroppedForeign)) {
        # always compressed
        if (appendToOutputFiles) {
            conOut2 = gzfile(outFileForDroppedForeign, open="at")
        } else {
            conOut2 = gzfile(outFileForDroppedForeign, open="wt")
        }
    }
    
    
    colNumsToKeepUnchanged = c(1:4)    # id, name, handle, location
    numStatusesCol = 11  # not sure if I'll keep this or not
    dateCols = c(8,13)  # for reformatting: date_created, date_last_post
    profileLangCol = 12
    statusLangCol = 15
    timeZoneCol = 10   # text description
    timeZoneUTCCol = 9  # UTC offset (in minutes)
    coordsCol = 14
    maxFieldNumUsed = max(colNumsToKeepUnchanged, numStatusesCol, dateCols, profileLangCol, statusLangCol,
                          timeZoneCol, timeZoneUTCCol, coordsCol)
    # (Due to privacy settings or lack of customization, many people have nothing marked for time zones. 
    # Some have no info for most recent status.)
    colsToPrint = c(colNumsToKeepUnchanged, profileLangCol, statusLangCol, timeZoneUTCCol, timeZoneCol, coordsCol, dateCols)
    
    colsToQuote = c(2,4,5, timeZoneCol, coordsCol)  # adding "description" because we'll still put it in the discard pile
    
    firstLine = T
    numForeignDropped = 0
    numLinesKept = 0
                        
    # automatically strips enclosing quotes, reads all fields as class character
    fields = scan(con, what=as.list(""), nmax=1, sep=",", quiet=T, na.strings=NULL, skipNul=T, quote="\"")[[1]]
    while (length(fields)) {
        
        if (firstLine) {
            firstLine = F
			if (grepl("content|id", fields[1])) {
				if (okToStart) {
				if (goodMatchField) {
					writeLines( paste(c(fields[colsToPrint], "nameWords", "handleWords", "nameMatchesHandle"), collapse=","), con=conOut)
				} else {
					writeLines( paste(c(fields[colsToPrint], "nameWords", "handleWords"), collapse=","), con=conOut)
				}
				numLinesKept = numLinesKept + 1
				}
				fields = scan(con, what=as.list(""), nmax=1, sep=",", quiet=T, na.strings=NULL, skipNul=T, quote="\"")[[1]]
				next
			}
		}
        if (!okToStart) {
            id = fields[1]
            if (as.numeric(id) == startWhenID) {
                okToStart = T
				print("Found starting line!")
            } else {
                fields = scan(con, what=as.list(""), nmax=1, sep=",", quiet=T, na.strings=NULL, skipNul=T, quote="\"")[[1]]
                next
            }
        }
		if (!is.null(stopAtID) && stopAtID == as.numeric(fields[1])) {
			print("Found stopping ID")
			break
		}

		if (!is.null(onlyDoIDsInHash)) {	 				# if using the hash
			if (is.null(onlyDoIDsInHash[[fields[1]]])) {	# and it doesn't contain this id
				# then skip this line
                fields = scan(con, what=as.list(""), nmax=1, sep=",", quiet=T, na.strings=NULL, skipNul=T, quote="\"")[[1]]
				next						
			}
		}

        
        # Change any embedded newlines to spaces
        fields[c(2,4,5)] = mapply(gsub, fields[c(2,4,5)], MoreArgs=list(pattern="\\n", replacement=" "))
        
        # Check for and discard foreign rows 
        if (pruneMoreForeign) {
            definitelyForeign = isDefinitelyForeign(fields[profileLangCol], fields[statusLangCol], 
                                                    fields[timeZoneUTCCol], fields[2], fields[4], fields[timeZoneCol])
        } else {
            definitelyForeign = isDefinitelyForeign(fields[profileLangCol], fields[statusLangCol], 
                                                    fields[timeZoneUTCCol], fields[2], fields[4])
        }
        if (definitelyForeign) {
            numForeignDropped = numForeignDropped + 1
            if (!is.null(outFileForDroppedForeign)) {
                #fields[colsToQuote] = mapply(addQuotes, fields[colsToQuote])
                #writeLines( paste(fields, collapse=","), con=conOut2)		# [bug fix] write.table is safer because it handles embedded quuotes
                write.table( matrix(fields, nrow=1), file=conOut2, quote=colsToQuote, sep=",", row.names=F, col.names=F, qmethod="d")
            }
            fields = scan(con, what=as.list(""), nmax=1, sep=",", quiet=T, na.strings=NULL, skipNul=T, quote="\"")[[1]]
            next
        }
        
        # Create index terms
        name = fields[2]
        handle = fields[3]
        # list with components nameWords, handleWords, goodMatch
        indexResults = putThemTogether(name, handle, printSummary=F)
        
		if (goodMatchField) {
			goodMatch = 0
			if (indexResults$goodMatch) {
				goodMatch = 1
			}
		}
        
         # Shorten date formatting
        fields[dateCols] = mapply(makeDate, fields[dateCols])
        
        # Shorten time zone spec (truncate to: Eastern, Central, Mountain, Pacific)
        fields[timeZoneCol] = sub(" Time \\(US \\& Canada\\)", "", fields[timeZoneCol])
        
        # Change any NAs to ""
        fields = fields[1:maxFieldNumUsed]
        fields[is.na(fields)] = ""

        # Fix coords, if present
        fields[coordsCol] = fixGPS(fields[coordsCol])
        
        if (length(indexResults$nameWords) > 0) {
            indexResults$nameWords = indexResults$nameWords[!is.na(indexResults$nameWords)]
        }
        if (length(indexResults$handleWords) > 0) {
            indexResults$handleWords = indexResults$handleWords[!is.na(indexResults$handleWords)]
        }
        
        # Manually put quotes around text columns <-- no longer, when using write.table
        #fields[colsToQuote] = mapply(addQuotes, fields[colsToQuote])
		withQuotes = rep(F, length(fields))
		withQuotes[colsToQuote] = T
        #procNameWords = addQuotes(paste(indexResults$nameWords, collapse=" "))
        #procHandleWords = addQuotes(paste(indexResults$handleWords, collapse=" "))
        procNameWords = paste(indexResults$nameWords, collapse=" ")
        procHandleWords = paste(indexResults$handleWords, collapse=" ")
        
		if (goodMatchField) {
			#writeLines( paste(c(fields[colsToPrint], procNameWords, procHandleWords, goodMatch),
			#				  collapse=","), con=conOut)
			write.table( matrix(c(fields[colsToPrint], procNameWords, procHandleWords, goodMatch), nrow=1), 
							file=conOut, quote=which(withQuotes[colsToPrint]), sep=",", row.names=F, col.names=F, qmethod="d")
		} else {
			#writeLines( paste(c(fields[colsToPrint], procNameWords, procHandleWords),
			#				  collapse=","), con=conOut)
			write.table( matrix(c(fields[colsToPrint], procNameWords, procHandleWords), nrow=1), 
							file=conOut, quote=which(withQuotes[colsToPrint]), sep=",", row.names=F, col.names=F, qmethod="d")
		}
        numLinesKept = numLinesKept + 1
        if (numLinesKept %% 10000 == 0) {
            print(paste("processed", numLinesKept, "(kept) lines"))
        }
        
        fields = scan(con, what=as.list(""), nmax=1, sep=",", quiet=T, na.strings=NULL, skipNul=T, quote="\"")[[1]]
    }
    close(con)
    close(conOut)
    if (!is.null(outFileForDroppedForeign)) {
        close(conOut2)
    }
    print(paste("Done! (Kept", numLinesKept, "entries and dropped", numForeignDropped, "foreign ones.)"))
}

# Conservative filter: only mark as foreign if 
# 1. no language is marked as "en", and either #2 or #3
# (Profile language seems to be public, and probably defaults to "en", so English-speaking users will generally have that value.)
# 2. timeZone's UTC is non-US 
#    2b. Consider broadening so that any non-US time zone (e.g., Central America) is counted as foreign
# 3. the name and loc are in a non-Latin alphabet
isDefinitelyForeign = function(profileLang, statusLang, timeZoneUTCOffset, name, locString, timeZoneString=NULL) {
    
    # If mentions English, don't call it foreign
    #if ("en" %in% c(profileLang, statusLang)) {
    # technically, language could also look like "en-uk"
    if ("en" %in% mapply(substr, c(profileLang, statusLang), MoreArgs=list(start=1, stop=2))) {
        return(F)
    }
    
    foreignTime = F
    hasTimeZone = F
    if (nchar(timeZoneUTCOffset) > 0 && !is.na(as.numeric(timeZoneUTCOffset))) {
        hasTimeZone = T
        utc = as.numeric(timeZoneUTCOffset)
        # Eastern: -18000; Pacific: -28800; Alaska: -32400; Hawaii: -36000
        if ((utc > -18000 || utc < -28800) && (utc != -32400) && (utc != -36000)) {
            foreignTime = T
        } else if (!is.null(timeZoneString)) {
            if (! (timeZoneString %in% c("Arizona", "Hawaii", "Alaska", "Indiana (East)",
                                         "Eastern Time (US & Canada)", "Central Time (US & Canada)",
                                         "Mountain Time (US & Canada)", "Pacific Time (US & Canada)",
				 # Consulted https://en.wikipedia.org/wiki/List_of_tz_database_time_zones for full list. 
                                         "America/Chicago", "America/Denver", "America/New_York", "America/Los_Angeles",
                                         "America/Boise", "America/Detroit", "America/Anchorage", "America/Phoenix",
										 # Indiana, ugh:
										 "America/Fort_Wayne", "America/Indiana/Indianapolis", "America/Indiana/Knox", "America/Indiana/Marengo",
										 "America/Indiana/Petersburg", "America/Indiana/Tell_City", "America/Indiana/Vevay", "America/Indiana/Vincennes",
										 "America/Indiana/Winamac", "America/Indianapolis", 
										 "America/Juneau", "America/Kentucky/Louisville", "America/Kentucky/Monticello", "America/Knox_IN", "America/Louisville",
										 "America/Menominee", "America/Metlakatla", "America/Nome", "America/Sitka", "America/Yakutat", 
										 "America/North_Dakota/Beulah", "America/North_Dakota/Center", "America/North_Dakota/New_Salem", 
										 "Navajo", "US/Alaska", "US/Aleutian", "US/Arizona", "US/Central", "US/Eastern", "US/East-Indiana", 
										 "US/Hawaii", "US/Indiana-Starke", "US/Michigan", "US/Mountain", "US/Pacific", "US/Pacific-New" 
										 ))) {
                foreignTime = T
            }
        }
    }
    
    if (hasTimeZone) {
        return(foreignTime)
    } else if (onlyForeignAlphabet(name) && (nchar(locString) == 0 || onlyForeignAlphabet(locString))) {
        return(T)
    } else {
        return(F)
    }
}

# Returns true iff there are letters and they are all non-Latin
onlyForeignAlphabet = function(text) {
     numLetters = stri_count_charclass(text, "\\p{Letter}")
     numLatinLetters = stri_count_charclass(text, "\\p{script=latin}")
     if (numLetters > 0 && numLatinLetters == 0) {
         return(T)
     }
     return(F)
}

# returns a date string in format like "2015-10-23". Assumes a very particular input, with length 30.
# Now works on vectors too.
makeDate = function(longDateString) {
    month = substr(longDateString, start=5, stop=7)
    day = substr(longDateString, start=9, stop=10)
    year = substr(longDateString, start=27, stop=30)
    niceDate = as.character(as.Date(strptime(paste(year, month, day), format="%Y %b %d")))
	niceDate[is.na(niceDate)] = ""
	return(niceDate)
}

# Not vectorized
fixGPS = function(coordsTxt) {
    if (nchar(coordsTxt) == 0) {
        return("")
    }
    ms = regexpr("\\[ (.+), (.+) \\]", coordsTxt, perl=T)
    starts = attr(ms, "capture.start")
    lengths = attr(ms, "capture.length")
    lat = substr(coordsTxt, starts[2], starts[2] + lengths[2] - 1)
    long = substr(coordsTxt, starts[1], starts[1] + lengths[1] - 1)
    newTxt = paste0(lat, ",", long)
    return(newTxt)
}

# Example usage
# Sept 2016: same as Feb 2016, but different input file
if (F) {
	inFile = "/home/lfriedl/twitterUSVoters/data/twitterDB-matching/twitter-acct-status/julDec2015-140M/lisa_census_out.csv.gz"

	start1 = NULL # not used
	#stop1 = start2 = 2612361881
	#stop2 = start3 = 2223107388
	outF1 = "/home/lfriedl/twitterUSVoters/data/twitterDB-matching/twitter-acct-status/julDec2015-140M/accountsToLoad1.csv.gz"
	outF2 = "/home/lfriedl/twitterUSVoters/data/twitterDB-matching/twitter-acct-status/julDec2015-140M/accountsToLoad2.csv.gz"

	#outFD1 = "/home/lfriedl/twitterUSVoters/data/twitterDB-matching/twitter-acct-status/julDec2015-140M/accountsOmitted1.csv.gz"
	#outFD2 = "/home/lfriedl/twitterUSVoters/data/twitterDB-matching/twitter-acct-status/julDec2015-140M/accountsOmitted2.csv.gz"
	#preprocessProfileTextFile(inFile, outF1, compressOutput=T, pruneMoreForeign = T, 
		#appendToOutputFiles=T, stopAtID=stop1, outFileForDroppedForeign=outFD1)
	#preprocessProfileTextFile(inFile, outF2, compressOutput=T, pruneMoreForeign = T,
		#appendToOutputFiles=T, startWhenID=start2, stopAtID=stop2, outFileForDroppedForeign=outFD2)
}
