

library(data.table)
library(bit64)	# needed if using fread for input
source("nameChunking.R")

# for speed:
#library(compiler)
#enableJIT(3)  # higher numbers for more aggressive JITting
options(warn=1)  # print warnings as they occur


# Code that takes as input a delimited file of Twitter profiles and 
# 1. Drops unambiguously foreign accounts (optionally writing them to a separate file)
# 2. Computes name and handle words to index, as a new column
# Main function is now preprocessProfileTextFileParallel(). See prepSeveralFiles.R for example usage.


# New! Do the same as before, but (1) parallelized, and (2) adapted to file format of ~kjoseph/profile_study/.
# Function to handle text file containing profile info: 
# -discard rows that are definitely foreign
# -add index terms for name and handle words (as 1 column: nameHandleWords)
# Assumes input file is a (possibly compressed) tsv containing embedded newlines.
# Notice that column positions are hard-coded inside the function.
# Using chunkSize=-1 should read in the whole file at once.
preprocessProfileTextFileParallel = function(inFile, outFile, outFileForDroppedForeign=NULL,
                           appendToOutputFiles=F, compressOutput=T,
                           chunkSize=500000, pruneMoreForeign=T, timeProfiling=T, inParallel=T, 
						   # for debugging
						   stopAfterChunk=NULL, needCleanInfile=T, useFread=T) {

	## Set up parallel stuff ##
	if (inParallel) {
		# note to self for future: can call sfInit(parallel=F) to unparallelize. The only 
		# reason I have all the plain apply() versions is for development / debugging.
			require(snowfall)
			sfInit(parallel=TRUE,cpus=10)
			# components for making isDefinitelyForeign work:
			sfLibrary(stringi)
			#sfLibrary(data.table)
			sfExport("onlyForeignAlphabet", "isDefinitelyForeign", "isDefinitelyForeignVectorized")
			sfExport("USTimeZoneStrings")
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


	if (useFread) {
		# Note about using fread vs scan: fread is (a) smarter and faster, but (b) brittle.
		# (a) Fread's defaults do good things with whitespace and quotes that scan's don't, so the output files differ accordingly.
		# (b) With fread, I do need an initial step of removing null characters and saving to a temp file. 
		# Also it's not built for chunk by chunk computation, so dies if you ask for more
		# lines than the file has.
		print("Using fread for speed")
		if (needCleanInfile) {
			print("Making a copy of the file w/o null chars")
			r = readBin(inFile, raw(), file.info(inFile)$size)
			r[r==as.raw(0)] = as.raw(0x20)
			tfile = tempfile(fileext=".txt")
			writeBin(r, tfile)
			rm(r)
			inFile = tfile
		}
		totLines = length(count.fields(inFile, sep="\t", comment.char="", quote=""))
	} 

	# gzfile can read .gz, others, and uncompressed too
	# (if !useFread, I'll open & close the connection but otherwise ignore it)
	con = gzfile(inFile, open="rt", encoding="UTF-8")
	

    
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
	colNamesToQuote = c(inputColNames[c(2,4,6,7,12,16)], "nameHandleWords")


	## Start reading and processing ## 

	if (timeProfiling) {
			startTime = Sys.time()
	}

	chunkCnt = 1
    numForeignDropped = 0
    numLinesKept = 0
	while (!isIncomplete(con)) {

		if (timeProfiling) {
				print(paste("About to read chunk", chunkCnt, "of profiles at", round(Sys.time()-startTime, 2), units(Sys.time()-startTime)))
		}

		if (useFread) {
			if (chunkSize == -1) {
				if (chunkCnt == 1) {
					#accountLines = fread(inFile, nrows=-1, na.strings=NULL)
					accountLines = fread(inFile, nrows=-1, na.strings=NULL, quote='')
				} else {
					break	# only had 1 big chunk, in this case
				}
			} else {
				numberToRequest = min(chunkSize * chunkCnt, totLines) - chunkSize * (chunkCnt - 1)
				if (numberToRequest <= 0) { # the stopping condition, in this case
					break
				}
				accountLines = fread(inFile, nrows=numberToRequest, skip=chunkSize * (chunkCnt - 1), na.strings=NULL, quote='')
			}
		} else {
			accountLines = as.data.table(matrix(
											# odd syntax, since I can't figure out how to get scan to put things into a list with length > 1
											scan(con, what=as.list(""), nlines=chunkSize, sep="\t", quiet=T, na.strings=NULL, skipNul=T, quote="", multi.line=F)[[1]], 
											byrow=T, nrow=chunkSize))
			if (nrow(accountLines) == 0) { # to catch a corner case
				break
			}
		}

		colnames(accountLines) = inputColNames

		print(paste("Read", nrow(accountLines), "lines of profiles to process"))
		if (timeProfiling) {
				print(paste("     Time since start:", round(Sys.time()-startTime, 2), units(Sys.time()-startTime)))
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
				print(paste("     did some gsubs, about to detect foreign profiles at", round(Sys.time()-startTime, 2), units(Sys.time()-startTime)))
		}

        ## Check for and discard foreign rows ##
		mat = accountLines[, .(profile_lang, tweet_lang, tz_offset, name, location, tz_name)]
		if (inParallel) {
			#sfExport("mat")		# every time I've tested, even with 100k lines, exporting makes it go slower
			if (pruneMoreForeign) {
				# two options here, really. Explicitly parallel one works better.
				definitelyForeign = sfApply(mat, 1, function(x) isDefinitelyForeignVectorized(x[1], x[2], x[3], x[4], x[5], x[6]))
				#mat = as.data.frame(mat)
				#definitelyForeign = isDefinitelyForeignVectorized(mat[,1], mat[,2], mat[,3], mat[,4], mat[,5], mat[,6])
			} else {
				definitelyForeign = sfApply(mat, 1, function(x) isDefinitelyForeignVectorized(x[1], x[2], x[3], x[4], x[5]))
			}
		} else {
			mat = as.data.frame(mat)
			if (pruneMoreForeign) {
				#definitelyForeign = apply(mat, 1, function(x) isDefinitelyForeignVectorized(x[1], x[2], x[3], x[4], x[5], x[6]))
				definitelyForeign = isDefinitelyForeignVectorized(mat[,1], mat[,2], mat[,3], mat[,4], mat[,5], mat[,6])		# yep, faster than plain apply
			} else {
				#definitelyForeign = apply(mat, 1, function(x) isDefinitelyForeignVectorized(x[1], x[2], x[3], x[4], x[5]))
				definitelyForeign = isDefinitelyForeignVectorized(mat[,1], mat[,2], mat[,3], mat[,4], mat[,5])
			}
		}
		if (sum(definitelyForeign) > 0) {
			numForeignDropped = numForeignDropped + sum(definitelyForeign)
            if (!is.null(outFileForDroppedForeign)) {
				# write.table handles embedded quotes (but write.csv doesn't do appending?)
                write.table( accountLines[definitelyForeign, colNamesToPrint, with=F], file=conOut2, quote=which(colNamesToPrint %in% colNamesToQuote),
							sep=",", row.names=F, col.names=F, qmethod="d")
				flush(conOut2)
			}
			accountLines = accountLines[!definitelyForeign,]
		}
		if (timeProfiling) {
				print(paste("     separated out", sum(definitelyForeign), "foreign profiles at", round(Sys.time()-startTime, 2), units(Sys.time()-startTime)))
		}

        ## Create index terms ##
		if (inParallel) {
			#sfExport("accountLines")       # expt: does this help? nope: like above, seems to slow it down.
			nameHandleWords = sfApply(accountLines[, .(name, handle)], 1, 
									function(x) { 
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
									function(x) { 
										x1 = putThemTogether(x[1], x[2], printSummary=F); 
										paste(c(x1$nameWords, x1$handleWords), collapse=" ")
									} )
		}
		accountLines = cbind(accountLines, nameHandleWords=nameHandleWords)
		if (timeProfiling) {
				print(paste("     done with index terms at", round(Sys.time()-startTime, 2), units(Sys.time()-startTime)))
		}

		# Replace True/False with TRUE/FALSE (to make SQL happy later)
		booleanFields = c("is_protected", "is_verified")
        accountLines = cbind(accountLines[, -booleanFields, with=F], accountLines[, lapply(.SD, toupper), .SDcols=booleanFields])

        # Shorten time zone spec (truncate to: Eastern, Central, Mountain, Pacific)
		accountLines = cbind(accountLines[, -"tz_name", with=F], accountLines[, .(tz_name=sub(" Time \\(US \\& Canada\\)", "", tz_name))])

		if (timeProfiling) {
				print(paste("     about to fix date and coords at", round(Sys.time()-startTime, 2), units(Sys.time()-startTime)))
		}
         # Shorten date formatting
        fieldsWithDates = c("date_created", "date_last_seen")
        accountLines = cbind(accountLines[, -fieldsWithDates, with=F], accountLines[, lapply(.SD, makeDate), .SDcols=fieldsWithDates])

        # Fix coords, if present
		accountLines = cbind(accountLines[, -"coords", with=F], accountLines[, .(coords=sapply(coords, fixGPS))]) 

		# (Do I need to check for NAs?)


		## Write out ##

		if (timeProfiling) {
				print(paste("     about to write out chunk", chunkCnt, "(", nrow(accountLines), " lines) at", round(Sys.time()-startTime, 2), units(Sys.time()-startTime)))
		}
		write.table( accountLines[, colNamesToPrint2, with=F], file=conOut, quote=which(colNamesToPrint2 %in% colNamesToQuote), 
						sep=",", row.names=F, col.names=F, qmethod="d")
		flush(conOut)
        numLinesKept = numLinesKept + nrow(accountLines)
		if (!is.null(stopAfterChunk) && stopAfterChunk == chunkCnt) {
			break
		}
		chunkCnt = chunkCnt + 1
	}

    close(con)
    close(conOut)
    if (!is.null(outFileForDroppedForeign)) {
        close(conOut2)
    }
	if (inParallel) {
		sfStop()
	}
	if (useFread && needCleanInfile) {
		file.remove(tfile)
	}
    print(paste("Done! (Kept", numLinesKept, "entries and dropped", numForeignDropped, "foreign ones.)"))

}


USTimeZoneStrings = c("Arizona", "Hawaii", "Alaska", "Indiana (East)",
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
				 "US/Hawaii", "US/Indiana-Starke", "US/Michigan", "US/Mountain", "US/Pacific", "US/Pacific-New")

# Filter is conservative. Its logic:
# 1. If any language is "en" --> plausibly U.S.
# (Profile language seems to be public, and probably defaults to "en", so English-speaking users will generally have that value.)
# 2. No language is "en" and there's a time zone --> trust the time zone 
#	(exclude those with the wrong offset, and optionally, further exclude those with the wrong string)
# 3. No language is "en" and no latin-alphabet characters in name or location --> foreign
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
            if (! (timeZoneString %in% USTimeZoneStrings)) {
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

isDefinitelyForeignVectorized = function(profileLang, statusLang, timeZoneUTCOffset, name, locString, timeZoneString=NULL) {
	finalAnswer = rep(NA, length(profileLang))

	prefixProfileLang = sapply(profileLang, FUN=substr, start=1, stop=2)
	prefixStatusLang = sapply(statusLang, FUN=substr, start=1, stop=2)
	hasEN = (prefixProfileLang == "en" | prefixStatusLang == "en")
	# rules: hasEN --> u.s.
	alreadyDefiniteAnswer = hasEN
	finalAnswer[hasEN] = F	# u.s.

	hasTZ = ( sapply(timeZoneUTCOffset, nchar) > 0 & !is.na(as.numeric(timeZoneUTCOffset)) )
	utc = as.numeric(timeZoneUTCOffset)
	foreignTime1 = hasTZ & (utc > -18000 | utc < -28800) & (utc != -32400) & (utc != -36000)	# hasTZ & foreignTime1 --> foreign
	alsoCheckTime2 = hasTZ & !foreignTime1 & !is.null(timeZoneString)							# offset looks U.S., but we have a string to check too
	# hasTZ & !foreignTime1 & !alsoCheckTime2 --> u.s.
	foreignTZString = !(timeZoneString %in% USTimeZoneStrings)
	# hasTZ & !foreignTime1 & alsoCheckTime2 --> do whatever foreignTZString says

	alreadyDefiniteAnswer = alreadyDefiniteAnswer | hasTZ
	finalAnswer[!hasEN & hasTZ & foreignTime1] = T	# foreign
	finalAnswer[!hasEN & hasTZ & !foreignTime1 & !alsoCheckTime2] = F 	# u.s.
	finalAnswer[!hasEN & hasTZ & !foreignTime1 & alsoCheckTime2] = foreignTZString[!hasEN & hasTZ & !foreignTime1 & alsoCheckTime2]

	# the stringi call is probably the slowest part, so only do it where necessary
	if (sum(!alreadyDefiniteAnswer)) {
		namesLeft = name[!alreadyDefiniteAnswer]
		locsLeft = locString[!alreadyDefiniteAnswer]
		onlyForeignAlphaName = sapply(namesLeft, onlyForeignAlphabet)
		onlyForeignAlphaLoc = ( sapply(locsLeft, nchar) == 0 | sapply(locsLeft, onlyForeignAlphabet) )
		onlyForeignAlpha = (onlyForeignAlphaName & onlyForeignAlphaLoc)
		finalAnswer[!alreadyDefiniteAnswer] = onlyForeignAlpha
	}

	return(finalAnswer)

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

