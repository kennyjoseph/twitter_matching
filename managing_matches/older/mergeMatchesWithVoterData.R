library(data.table)

usage = "Usage: Rscript [--vanilla] mergeMatchesWithVoterData.R <matchFiles.csv> <outfile.csv>"

# We'll probably run this on all files in a directory, at once.
# Outfiles can have names like: "allMatches-wDups.csv"

origVoterfileDir = "/net/data/twitter-voters/voter-data/targetsmart"

run_command_line_call = function() {
        args = commandArgs(T)   # everything after executable name
        if (length(args) < 2) {
                stop("Not enough args\n", usage)
        }
	outfile = args[length(args)]
	infiles = args[1:(length(args) - 1)]

	# Read all the infiles and rbind them! Into "allMatches"
	for (infile in infiles) {
		data1 = fread(infile)
		print(paste("In file", infile, "found", nrow(data1), "lines"))
		if (exists("allMatches")) {
			allMatches = rbind(allMatches, data1)
		} else {
			allMatches = data1
		}
	}

	# Which raw voterfiles are represented?
	# match files look like, e.g.
	#   voter_id,twProfileID,twProfileName,twProfileHandle,twProfileLoc
	#   ts_IA-2842709,859005715,Cody Braunschweig,C_Braunschweig,"Madrid, Iowa"
	allMatches[, voter_id := substr(voter_id, start=4, stop=nchar(voter_id))]  # chop off 'ts_' so we can merge on the rest
	voterState = substr(allMatches$voter_id, start=1, stop=2)
	statesNeeded = unique(voterState)
	
	# Merge each state with its voterfile, and ... (don't even need to store?) Write right out.
	for (state in statesNeeded) {
		voterfile = paste0(origVoterfileDir, "/tsmart_northeastern_install_file_", state, ".csv")
		print(paste("Looking for", sum(voterState == state), "people in", voterfile))
		voterData = fread(voterfile)
		# (Just grabbing all fields)
		mergedState = merge(allMatches[voterState == state,], voterData, by.x="voter_id", by.y="voterbase_id", all.x=T)

		unmatchedCnt = sum(is.na(mergedState$voter_id))
		if (unmatchedCnt > 0) {
			print(paste("For ", state, ", had ", unmatchedCnt, " voters who we didn't find in the original voterfile."))
		}

		#print(paste("Saving voters from", state, "--", nrow(mergedState), "-- to", outfile))
		print(paste("Saving voters from", state, "to", outfile))
		if (state == statesNeeded[1]) {
			fwrite(mergedState, file=outfile)
		} else {
			fwrite(mergedState, file=outfile, append=T)
		}

		#if (exists("allMerged")) {
		#	allMerged = rbind(allMerged, mergedState)
		#} else {
		#	allMerged = mergedState
		#}
	}
}

# Actually do the call down here
run_command_line_call()

