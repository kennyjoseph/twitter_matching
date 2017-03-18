
# Function to pull out Twitter profiles whose names match each voter. 
# (Much simpler than in earlier versions; now, no location processing has taken place yet.)
#
# Iterates through voter file. 
# For each voter, clean the name, query the DB for matches, and write out all matches. That's it.
# Hard-coded maximum of 10 matches printed per voter. (Should make this a variable and export it in runInParallel.R.)

# Note: matchCountsFile omits voters that didn't have enough words in their name to query the DB.

library(RMySQL)
options(warn=1)               # print warnings as they occur
source("../voter_file_aggregation/voter_name_cleaning.R")   # for getNameWordsFromVoter()

# Very important for speed: These tables' indexes need to be pre-loaded into the key cache (takes a few (3-4) minutes).
alreadyInKeyCache = TRUE      # TRUE if it's been done manually; otherwise, will do it after opening db connection.
# tables to use in twitterusers DB:
#DBtables = c("profiles2017_2")
#DBtables = c("profiles2017v2_3", "profiles2017v2_4")
DBtables = c("profiles2017v2_1", "profiles2017v2_2", "profiles2017v2_3", "profiles2017v2_4")

# All-purpose function we want.
# Input: voter file, in any of three formats (see below).
# Outputs:
#   -outfileForCandMatches is .tsv file of all good-looking matches, to use in next step of matching.
#   -voterfileOut [optional] shows number of matches per voter
# Param inputFileHasCounts toggles between three input files: (1) voters-someFields.csv, which is csv;
# (2) voters-moreFields.txt, which is tab-separated and adds voter counts per zip, state and country;
# or (3) the original file format provided by the DNC, which is csv with many columns, but not counts
countTwitterVoterMatches = function(voterfileIn, outfileForCandMatches, 
				inputFileFormat=3, startWithRecord=1, stopAfterRecord=NULL, matchCountsFile = NULL) {
    
    print(paste("Args to main function: voterfileIn=", voterfileIn, ", outfileForCandMatches=", outfileForCandMatches, 
        ", inputFileFormat=", inputFileFormat, ", startWithRecord=", startWithRecord, ", stopAfterRecord=", stopAfterRecord,
        ", matchCountsFile=", matchCountsFile))
    
    # db connection
    dbCon = initializeDBCon()
    
    # in/out files
    con = file(voterfileIn, open="rt")
	twOut2 = file(outfileForCandMatches, open="wt")
    if (!is.null(matchCountsFile)) {
		conCnts = file(matchCountsFile, open="wt")
	}
    
    inputIsCSV = F
    if (inputFileFormat == 1) {		# voters-someFields.csv
		# voter id, first name, last name, city, state (where registered)
		inputColsToKeep = c(1,3:4, 13, 2)
		inputIsCSV = T
    } else if (inputFileFormat == 2) {		# voters-moreFields.txt
		# 21:22 = voters_in_country, voters_in_state
		inputColsToKeep = c(1,3:4, 21:22, 13, 2)	
    } else if (inputFileFormat == 3) {	# orig format .csv
		# voter id, first name, last name, flags_online, city, state (where registered)
		inputColsToKeep = c(1,3:4, 45, 17, 2)
		inputIsCSV = T
    }
	colsToQuote = length(inputColsToKeep) + 2:6   # quote fields (from db) name, handle, loc, descrip, url
    
    # read and write headers
    if (!inputIsCSV) {
    	# Must use colClasses = "character" here and at end of loop to avoid initials of F or T being expanded to FALSE or TRUE
        fields = read.table(con, header=T, nrows=startWithRecord, stringsAsFactors=F, sep="\t", fill=T, quote="", comment.char="", colClasses = "character")
    } else {
        fields = read.csv(con, header=T, nrows=startWithRecord, stringsAsFactors=F, colClasses = "character")
    }

	writeLines(paste(c(colnames(fields[inputColsToKeep]), "twProfileID", "twProfileName", "twProfileHandle", "twProfileLoc", "etc"),
				 collapse="\t"), con=twOut2)

    if (!is.null(matchCountsFile)) {
		writeLines(paste(c(colnames(fields[inputColsToKeep]), "numCandidateMatches"), collapse="\t"), con=conCnts)
	}

	if (startWithRecord > 1) {	# skip to the bottom of the chunk loaded already
		fields = fields[nrow(fields),]
	}
	lineCount = startWithRecord - 1
	#lineCount = 0       
	#lineCount = 1       # count the header, at least while testing, so round-numbered input files get outputs printed
	
	queryCountTimes = c()
	querySelectTimes = c()

    while (length(fields)) {

		# fields is a data frame with 1 row
		fields[is.na(fields)] = ""
		firstName = fields[1,3]
		lastNames = fields[1,4]
			
		# a vector of lower-case words with punctuation removed
		firstNameWords = getNameWordsFromVoter(firstName)
		lastNameWords = getNameWordsFromVoter(lastNames)
		nameWords = c(firstNameWords, lastNameWords)

		numMatches = 0
        
        # Search for this record only if there's a >1-letter word in both firstName and lastName fields.
        # (MySQL has been started with special flags so that all words of 2 or more characters will be active in queries)
        if (!( sum(nchar(firstNameWords) > 1) && sum(nchar(lastNameWords) > 1))) {
            # this will print in what's otherwise the "name" field for the matched profile
            #profileOfInterest[2] = "[skipped this record]"
            totNumMatches = -99  # special code to mean "skipped this record"
            
        } else {
            startTime = Sys.time()
			totNumMatches = countTwitterProfiles(dbCon, nameWords, maxOk=10)	# save time by only retrieving records when the number is small
			endTime = Sys.time()
			queryCountTimes = c(queryCountTimes, as.numeric(endTime - startTime, units="secs"))
            
			# If we have "unique in zip" field, require it be 1. Normally, take all lines.
			uniqueInZip = (fields[23] == 1 || inputFileFormat != 2)		
			
			# Write out candidates matches, as input to further matching rules 
			if (uniqueInZip && totNumMatches > 0 && totNumMatches <= 10) {
                startTime = Sys.time()
				twProfiles = getAllTwitterProfiles(dbCon, nameWords)	# data.frame with columns: id, name, handle, locationString, inferredLoc
    			endTime = Sys.time()
    			querySelectTimes = c(querySelectTimes, as.numeric(endTime - startTime, units="secs"))

				write.table(cbind(fields[inputColsToKeep], twProfiles),
					file=twOut2, quote=colsToQuote, sep="\t", row.names=F, col.names=F, qmethod="d")
					#matrix(c(rep(fields[inputColsToKeep], each=nrow(twProfiles)),             # repeat voter fields
					#		 twProfiles$id, twProfiles$name, twProfiles$handle, twProfiles$location, twProfiles$inferredLoc, 
					#		 twProfiles$thisState, twProfiles$plainUS, twProfiles$noLoc, twProfiles$unparsed, twProfiles$otherUS, twProfiles$foreign),
					#	   nrow=nrow(twProfiles)),
			}
		}

		if (!is.null(matchCountsFile)) {
				writeLines(paste(c(fields[inputColsToKeep], totNumMatches), collapse="\t"), con=conCnts)
		}

		lineCount = lineCount + 1
		if (lineCount %% 100 == 0) {
			print(paste("finished line", lineCount))
			flush(twOut2)
			if (!is.null(matchCountsFile)) {
				flush(conCnts)
			}
			print(paste("Timing for count(*) calls: mean=", round(mean(queryCountTimes), 3), "sec, var=", var(queryCountTimes)))
			#print(paste(queryCountTimes, collapse=" "))
			print(paste("Timing for select(*) calls: num=", length(querySelectTimes), ", mean=", round(mean(querySelectTimes), 3), 
			            "sec, var=", var(querySelectTimes)))
			queryCountTimes = c()
			querySelectTimes = c()
		}
		if (!is.null(stopAfterRecord) && stopAfterRecord == lineCount) {
		    break
		}
        
        lineFound = readLines(con, n=1)
        fields = c()
        if (length(lineFound)) {
            if (!inputIsCSV) {
                fields = read.table(text=lineFound, header=F, nrows=1, stringsAsFactors=F, 
                                    sep="\t", fill=T, quote="", comment.char="", colClasses = "character")
            } else {
                fields = read.csv(text=lineFound, header=F, nrows=1, stringsAsFactors=F, colClasses = "character")
            }
        }
    }
	
    close(con)
	close(twOut2)
    dbDisconnect(dbCon)
    if (!is.null(matchCountsFile)) {
		close(conCnts)
    }
}



# Returns a number.
# maxOk further short-circuits counting by:
#  -not looking at additional tables after maxOk is surpassed
#  -using "limit maxOk+1" to shorten subquery too
countTwitterProfiles = function(dbCon, nameWords, maxOk=-1) {
    matchString = paste0("match(nameHandleWords) against('", paste0("+", nameWords, collapse=" "), "' in boolean mode)")
    subqLimit = ifelse(maxOk > 0, maxOk + 1, 100)
    
    currentCnt = 0
    for (dbT in DBtables) {
    	# avoid long queries using this cap:
    	query = paste("select count(id) as cnt from (select id from", dbT, "where", matchString, "limit", subqLimit, ") as a")
        res1 = dbGetQuery(dbCon, query)
        currentCnt = currentCnt + res1$cnt
        if (maxOk > 0 && currentCnt > maxOk) {
            return(subqLimit)   # better than currentCnt b/c gives everyone the same number for "past limit"
        }
    }
	return(currentCnt)
}

# Returns a data.frame of results. If no hits, result contains 0 rows.
getAllTwitterProfiles = function(dbCon, nameWords) {
    matchString = paste0("match(nameHandleWords) against('", paste0("+", nameWords, collapse=" "), "' in boolean mode)")
    
    for (dbT in DBtables) {
        query = paste("select * from", dbT, "where", matchString)
        res1 = dbGetQuery(dbCon, query)
        if (!exists("profiles")) {
            profiles = res1
        } else {
            profiles = rbind(profiles, res1)
        }
    }
	
	return(profiles)
}


initializeDBCon = function() {
    # db connection
    # will only work from the achtung cluster
    dbCon = dbConnect(dbDriver("MySQL"), user="twitterusers", password="twitterusers", 
                      host="achtung-db", dbname="twitterusers")
    # right away, increase tmp table size, so it doesn't spend eons writing stuff to disk
    query = "SET tmp_table_size = 1024 * 1024 * 64" # 64M
    queryRes = dbSendQuery(dbCon, query)
    dbClearResult(queryRes)
    query = "SHOW VARIABLES LIKE 'tmp_table_size'"
    queryRes  = dbSendQuery(dbCon, query)
    answer = dbFetch(queryRes)
    print(answer)
    dbClearResult(queryRes)
    
    # magic potion to make SQL actually speak to us in Unicode 
    # (via http://stackoverflow.com/questions/12869778/fetching-utf-8-text-from-mysql-in-r-returns)
    queryRes = dbSendQuery(dbCon, "SET NAMES utf8")
    
    if (!alreadyInKeyCache) {
        query = paste("LOAD INDEX INTO CACHE", paste(DBtables, collapse=", "))
        queryRes  = dbSendQuery(dbCon, query)
        answer = dbFetch(queryRes)
        print(answer)
        dbClearResult(queryRes)
    }
        
    
    return(dbCon)
}



# Oct 2016
# initial test took under 6 minutes for 1000, or about 172 records/min. Repeating same records: 27 seconds total.
#voterfileIn = "~/twitterUSVoters-0/voters/head_voterfile_national2m.csv"    
# Full 2M took 18 hours the first time, 15 to repeat.
#voterfileIn = "~/twitterUSVoters-0/voters/voterfile_unique_national2m.csv"


# March 2017
# and...that's too slow. Taking 3.5 minutes per 100, or 33.7 for 1000. Too slow!
# Simple query optimization: count(id) instead of count(*) and short-circuit if the first table was too big --> only 22 minutes for 1000.
# voterfileIn="~/twitterUSVoters-0/voters/head3.1"
# outfileForCandMatches = "candMatches-head3.1.csv"
# matchCountsFile = "matchCounts-head3.1.csv"

#system.time(countTwitterVoterMatches(voterfileIn, outfileForCandMatches=outfileForCandMatches, inputFileFormat=3, matchCountsFile=matchCountsFile)) 
