

source("matchPeople3.R")

# Code in this file: makes calls to countTwitterVoterMatches() many times.
# See the very bottom for live / best version, which calls runOnFileChunks().

# When running as a single process, it looks like this:
# voterfileIn="~/twitterUSVoters-0/voters/voterfile_unique_national2m.csv"
# outfileForCandMatches = "candMatches.csv"
# matchCountsFile = "matchCounts.csv"
#countTwitterVoterMatches(voterfileIn, outfileForCandMatches=outfileForCandMatches, matchCountsFile=matchCountsFile)

# numCPUs: should be able to ask for up to 32 easily. For more, would need to first call sfSetMaxCPUs(number=[..]).
initSnowfall = function(numCPUs=10) {
	require(snowfall)
	sfInit(parallel=TRUE,cpus=numCPUs)
	# components for making countTwitterVoterMatches work:
	sfLibrary(RMySQL)
	sfExport("countTwitterVoterMatches", "countTwitterProfiles", "getAllTwitterProfiles", "initializeDBCon", "getNameWordsFromVoter")
	alreadyInKeyCache = T	# must manually ensure this; not ok to let multiple nodes all make the call to set it
	sfExport("DBtables", "alreadyInKeyCache")
}


# Note that record counting (totVoterRecords, startLines, stopLines) always ignores the header (header = line 0).
# totVoterRecords is needed for up front so that we know how many calls [chunks] to do.
runOnFileChunks = function(voterfileIn, outDir, linesPerCall, totVoterRecords=2000000, numCPUs=10) {

	numCalls = ceiling(totVoterRecords / linesPerCall)
	startLines = vector(mode="numeric", length=numCalls)
	stopLines = vector(mode="numeric", length=numCalls)
	candMatchFiles = vector(mode="character", length=numCalls)
	matchCountFiles = vector(mode="character", length=numCalls)
	messageFiles = vector(mode="character", length=numCalls)

	fileCnt = 0
	for (i in 1:numCalls) {
		fileCnt = fileCnt + 1
		startLines[fileCnt] = (fileCnt - 1) * linesPerCall + 1
		stopLines[fileCnt] = min(fileCnt * linesPerCall, totVoterRecords)

		candMatchFiles[fileCnt] = file.path(outDir, paste("candMatches-", fileCnt, ".txt", sep=""))
		matchCountFiles[fileCnt] = file.path(outDir, paste("matchCounts-", fileCnt, ".txt", sep=""))
		messageFiles[fileCnt] = file.path(outDir, paste("out-", fileCnt, ".txt", sep=""))
	}

	print("Created list of args, about to initialize the cluster")
	initSnowfall(numCPUs)

	print("About to make the big call! Check out-*.txt files in output directory to see stdout from each run")
	files = cbind(candMatchFiles, startLines, stopLines, matchCountFiles, messageFiles)
	sfExport("voterfileIn")
	sfApply(files, 1, function(x) capture.output(system.time(
										countTwitterVoterMatches(voterfileIn, outfileForCandMatches=x[1], 
													startWithRecord=as.numeric(x[2]), stopAfterRecord=as.numeric(x[3]), matchCountsFile=x[4])),
										file=x[5]))
	print("finished running!!")
	sfStop()

}



if (F) {	
	# national 2M sample
	voterfileIn="~/twitterUSVoters-0/voters/voterfile_unique_national2m.csv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/data"

	#system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 200, totVoterRecords = 2300))
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 2000))		# at this rate, expect each job to take ~10 min, and each node will have 100 such jobs --> 17 hours total?
											# in practice, each job is taking 15-20 min instead --> 34 hours?
											# after 1 hour, had 77k lines --> 26 hours?
	# actual runtime (March 2017): 21.22 hours. (Observation: final few files went *much* faster than others because competing processes had already finished.)

	# earlier national 100k sample
	voterfileIn="~/twitterUSVoters/data/voter-data/voterfile_sample100k_uniqInState.csv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/data_100k"
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 2000, totVoterRecords = 52764, numCPUs=15))		
	# took 40 minutes total: 18-26 min per chunk for the first chunks, a bit less (as little as 15 min) for the second.
	# note: that looks about the same speed as above--(2/3) * 1 hr = 40 min, and (2/3) * 77k ~= 52k. Suggesting that number of processes
	# doesn't matter, and that the bottleneck is instead the mySQL server.
}
