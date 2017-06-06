

# To run: 
usage = "Usage: Rscript [--vanilla] runTS.R <fileNum> <actionNum> <force=1 or 0>"
# where fileNum corresponds to the >50 pre-split input files, actionNum = 1 or 2, and force=1 means do it even if output file exists already

# Directory structure:
# /net/data/twitter-voters
#	/voter-data
#		/targetsmart   	# raw data from TargetSmart
#		/ts_cleaned	# preprocessed
#		/ts_chunks	# split into files of < 3 million people. (Input to this script.)
#	/matching-work-files
#		/cand-matches	# (Created during part1.)
#			/one-subdir-per-input-file
#		/with-locations	# (Output of part1.)
#			/one-subdir-per-input-file  	# (eventually deleted)
#			/handful of files per input file 
#	/match-results		# (Output of part2.)
#		/handful of files per input file 

# Initialize vars
voterfileDir 		= "/net/data/twitter-voters/voter-data/ts_chunks"
numVoterfilesWeKnowAbout = 124   # for sanity check
candMatchesBaseDir 	= "/net/data/twitter-voters/matching-work-files/cand-matches"
locsBaseDir 		= "/net/data/twitter-voters/matching-work-files/with-locations"
matchResultsBaseDir 	= "/net/data/twitter-voters/match-results"

scriptsBaseDir = "/home/lfriedl/twitter_matching/"
placeListDir = file.path(scriptsBaseDir, "add_locs_and_do_match/data/placeLists/")

# Master list of files = whatever's in voterfileDir. Make sure it doesn't change without our knowing about it.
voterfiles = list.files(voterfileDir, pattern="\\.tsv$")
if (length(voterfiles) != numVoterfilesWeKnowAbout) {
	stop("Something has changed in ", voterfileDir, "; please check this and update script before proceeding")
}

# Process command-line instructions
run_command_line_call = function() {
	args = commandArgs(T)   # everything after executable name
	if (length(args) < 2) {
		stop("Not enough args\n", usage)
	}
	fileNum = as.integer(args[1])
	partNum = as.integer(args[2])
	if (length(args) > 2 && !(args[3] == "0")) {
		forceIt = T
	} else {
		forceIt = F
	}

	if (fileNum > numVoterfilesWeKnowAbout) {
		stop("There is no input file number ", fileNum)
	}
	if (!(partNum %in% 1:2)) {
		stop("There is no action number ", partNum)
	}
	print(paste0("Calling run_part", partNum, "(", fileNum, ", force=", forceIt, ") at ", Sys.time()))
	if (partNum == 1) {
		run_part1(fileNum, forceIt)
	} else if (partNum == 2) {
		run_part2(fileNum, forceIt)
	}
	print(paste("all done! at", Sys.time()))
}

# From input file name, attempts to create a substring we'll want to use for subdirs and output files
getVoterfileStem = function(voterfileName) {
	st = sub("\\.tsv", "", voterfileName)
	return(st)
}

# Includes header row in count
getWordCount = function(voterfilePath) {
	print(paste("Calling word-count on", voterfilePath))

        wcOut = system2("wc", voterfilePath, stdout=T)
        numRecords = as.numeric(strsplit(wcOut, " +")[[1]][2])
	return(numRecords)
}

run_part1 = function(fileNum, force=F) {
	voterfileName = voterfiles[fileNum] 	# looks something like WI_chunk1.tsv
	print(paste("Working on file", voterfileName))
        voterfileIn = file.path(voterfileDir, voterfileName)
	voterfileStem = getVoterfileStem(voterfileName)

	# Construct names of output files and check whether they exist yet
        candMatchesDir = file.path(candMatchesBaseDir, voterfileStem) 
	if (dir.exists(candMatchesDir)) {
		print(paste("Directory", candMatchesDir, "already exists"))
		hasFiles = list.files(candMatchesDir, pattern="matchCounts")	# these stick around even after the others get deleted
		if (length(hasFiles) && !force) {
			stop("Found existing output files in ", candMatchesDir, "; please remove or run again using 'force' option")
		}
	} else {
		dir.create(candMatchesDir, recursive=T)         # create the outdir if not exists
	}
        candMatchLocsFile = file.path(locsBaseDir, voterfileStem, "allCandidateMatchLocs.csv")
	if (file.exists(candMatchLocsFile) & !force) {
		stop("Found existing output file ", candMatchLocsFile, "; please remove or run again using 'force' option")
	}

	# Get numRecords
	numRecords = getWordCount(voterfileIn) - 1

	# Query against Twitter DB.
        setwd(file.path(scriptsBaseDir, "mySQL_load_retrieve"))
        source("runInParallel.R")
	print(paste("Will put candidate matches in directory", candMatchesDir))

        filterFieldForQuerying = "city_count"
        runOnFileChunks(voterfileIn, candMatchesDir, linesPerCall = 50000, totVoterRecords = numRecords, 
                                        numCPUs=2, uniquenessFilterField=filterFieldForQuerying, randomOrder=T)                
        # <-- Biggest time bottleneck; takes ~12 hours per million voters.
	print(paste("Finished querying the SQL DB! at", Sys.time()))


	# Add locs and rbind
        candMatchesLocsWorkDir = file.path(locsBaseDir, voterfileStem, "candidateMatchFilesWithLocs")     
        dir.create(candMatchesLocsWorkDir, recursive=T)
	print(paste("Will add locations working in directory", file.path(locsBaseDir, voterfileStem)))

        setwd(file.path(scriptsBaseDir, "add_locs_and_do_match"))
        source("addLocation.R")

        t = NULL        # var used to check for errors
        t = system.time(runDirectory(candMatchesDir, candMatchesLocsWorkDir, placeListDir))      # add location
	# <-- Highest processor usage (10 CPUs)
	print("Timing for location computations:")
        print(t)
        grabAll(candMatchesLocsWorkDir, candMatchLocsFile)            # rbind
        if (file.exists(candMatchLocsFile) && file.size(candMatchLocsFile) >= 150 * numRecords) {       # cutoff taken from existing files. Was 200, but now reducing a bit.
                unlink(candMatchesLocsWorkDir, recursive=T)     # ok to delete outDir once we have candMatchLocsFile
                # also remove candMatchesDir/candMatches-* at this point. But not the other files in that dir (matchCounts-* and out-*).
                unlink(paste0(candMatchesDir, "/candMatches-*")) 
        }

	print(paste("Done with part1 at", Sys.time()))

}

run_part2 = function(fileNum, force=F) {
	voterfileName = voterfiles[fileNum] 	# looks something like WI_chunk1.tsv
	print(paste("Working on file", voterfileName))
        voterfileIn = file.path(voterfileDir, voterfileName)
	voterfileStem = getVoterfileStem(voterfileName)

	# Construct names of output files and check whether they exist yet
	candMatchLocProbsFile = file.path(locsBaseDir, voterfileStem, "allCandidateMatchLocProbs.csv")
	if (file.exists(candMatchLocProbsFile) & !force) {
		stop("Found existing output file ", candMatchLocProbsFile, "; please remove or run again using 'force' option")
	}

        foreignCutoff = .9
	filterFieldForMatching = "city_count"
	filterFieldBase = substr(filterFieldForMatching, 1, nchar(filterFieldForMatching) - 6)
	matchResultsFile = file.path(matchResultsBaseDir, paste0("matches-", voterfileStem, "-uniq", filterFieldBase, "-Ctree", foreignCutoff, "-rule5-wDups.csv"))
	matchResultsOut = file.path(matchResultsBaseDir, paste0("matches-", voterfileStem, "-uniq", filterFieldBase, "-Ctree", foreignCutoff, "-rule5-wDups.out"))
	if ((file.exists(matchResultsFile) || file.exists(matchResultsOut)) & !force) {
		stop("Found existing output file ", matchResultsFile, "or", matchResultsOut, "; please remove or run again using 'force' option")
	}


        # Get numRecords
	numRecords = getWordCount(voterfileIn) - 1	
	howManyMillions = numRecords / 1000000 		# for downsampling. This ratio reproduces the "downsample = .5" for 2M voters.

	# Add probMetaForeign
        setwd(file.path(scriptsBaseDir, "add_locs_and_do_match"))
	source("randomForest.R")
	# caution: is a terrible memory hog.
        candMatchLocsFile = file.path(locsBaseDir, voterfileStem, "allCandidateMatchLocs.csv")
	addPredictionToCandMatches(infile=candMatchLocsFile, outfile=candMatchLocProbsFile, downsampleTrainingFactor=min(1, 1/howManyMillions))

	# Do the matching
        source("do_the_matching.R")
	capture.output(matchTheMatches(voterfileIn=NULL, matchFileIn=candMatchLocProbsFile, fullMatchFileOut=matchResultsFile,
					matchRulesVersion = 5, filterField = filterFieldForMatching, filterFieldMax = 1, 
					foreignIfProbScoreAbove = foreignCutoff, removeDups=FALSE), file=matchResultsOut)

	print(paste("Done with part2; see results log at", matchResultsOut))

}

# Actually do the call down here
run_command_line_call()

