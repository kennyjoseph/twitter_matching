

source("matchPeople3.R")

# Code in this file: makes calls to countTwitterVoterMatches() many times.
# See the very bottom for live / best version, which calls runOnFileChunks().

# When running as a single process, it looks like this:
# voterfileIn="~/twitterUSVoters-0/voters/voterfile_unique_national2m.csv"
# outfileForCandMatches = "candMatches.txt"
# matchCountsFile = "matchCounts.txt"
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
# totVoterRecords is needed for up front so that we know how many calls [chunks] to do. Should be fine to round it up to any number within the same chunk.
# randomOrder flag is useful for load balancing: otherwise, the first CPU gets all the low numbers (which are faster) and finishes much earlier.
#	its drawback: can't restart midway through
runOnFileChunks = function(voterfileIn, outDir, linesPerCall, totVoterRecords=2000000, numCPUs=10, 
				uniquenessFilterField=NULL, voterFileFormat=2, randomOrder=FALSE) {

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
	if (randomOrder) {
		files = files[order(sample(nrow(files))),]	# could instead interleave while preserving order, but that's non-trivial to code for arbitrary numCPUs
	}
	sfExport("voterfileIn", "voterFileFormat", "uniquenessFilterField")
	sfApply(files, 1, function(x) capture.output(system.time(
										countTwitterVoterMatches(voterfileIn, outfileForCandMatches=x[1], 
													voterFileFormat=voterFileFormat, uniquenessFilterField=uniquenessFilterField,
													startWithRecord=as.numeric(x[2]), stopAfterRecord=as.numeric(x[3]), matchCountsFile=x[4])),
										file=x[5]))
	print("finished running!!")
	sfStop()

}



if (F) {	
	# national 2M sample
	voterfileIn="~/twitterUSVoters-0/voters/voterfile_unique_national2m.csv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/data"

	# (if redoing later: would need to add voterFileFormat=1 flag)
	#system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 200, totVoterRecords = 2300))
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 2000))		# at this rate, expect each job to take ~10 min, and each node will have 100 such jobs --> 17 hours total?
											# in practice, each job is taking 15-20 min instead --> 34 hours?
											# after 1 hour, had 77k lines --> 26 hours?
	# actual runtime (March 2017): 21.22 hours, or 94k / hr. (Observation: final few files went *much* faster than others because competing processes had already finished.)

	# earlier national 100k sample
	voterfileIn="~/twitterUSVoters/data/voter-data/voterfile_sample100k_uniqInState.csv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/data_100k"
	# (if redoing later: would need to add voterFileFormat=1 flag)
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 2000, totVoterRecords = 52764, numCPUs=15))		
	# took 40 minutes total: 18-26 min per chunk for the first chunks, a bit less (as little as 15 min) for the second.
	# note: that looks about the same speed as above--(2/3) * 1 hr = 40 min, and (2/3) * 77k ~= 52k. Suggesting that number of processes
	# doesn't matter, and that the bottleneck is instead the mySQL server.

	# public voter data
	# Delaware
	voterfileIn = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/delaware.tsv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/public_voters/DE"
	filterField = "zipcode_count"
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 2000, totVoterRecords = 645500, numCPUs=15, uniquenessFilterField=filterField))		
	# took 10.67 hours, averaging ~60k / hr, or 16.8 / sec. This was with 15 CPUs, now using the _oneMassive table.
	
	# RI
	voterfileIn = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/rhode_island.tsv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/public_voters/RI"
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 2000, totVoterRecords = 740025, numCPUs=8, uniquenessFilterField=filterField))		
	# off to a much faster start: more like ~90k / hr. (Except two workers had errors, which we'll need to recover separately?)
	# Finished (apart from errors) 562807 lines in 7.2 hours, or 78k / hr, or 21.7 / sec.
	# When it's a single process, each chunk of 2000 takes 70-90 sec, so 22-28 / sec.

	# OK
	voterfileIn = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/oklahoma.tsv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/public_voters/OK"
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 10000, totVoterRecords = 2121709, numCPUs=4, uniquenessFilterField=filterField))		
	# taking 20-40 min per chunk of 10000 (high variance!); after 17 hrs, have done 1513053, or ~89k / hr.
	# total: about 24 hrs, so 88.5k / hr using oneMassive table and 4 CPUs.
	
	# CT
	voterfileIn = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/connecticut.tsv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/public_voters/CT"
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 10000, totVoterRecords = 2342334, numCPUs=2, uniquenessFilterField=filterField))		
	# previous results suggest we could meet or beat the previous speed running with only 2 CPUs at once. Let's find out!
	# indeed: this gave 109k / hr (or 21.4 hours total).

	# CO
	voterfileIn = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/colorado.tsv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/public_voters/CO"
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 10000, totVoterRecords = 3836424, numCPUs=1, uniquenessFilterField=filterField))		
	# as a test, see if a single CPU can possibly match or beat 2 CPUs. (Still with _oneMassive table.)
	# yes, totally. 
	# I wonder if there are fewer names meeting search criteria? Out of first 10k, CT had 1377 select(*) calls; CO had 766; OK had 889; RI had 2321.
	# --> That could explain why RI was slowest and CO fastest among that set, but not why CT was faster than OK.
	# In any case, this is going absurdly fast (10k per 4 min), so we should use the same setting later.
	# In total: 46 hours. Nothing special: 83k / hr. For whatever reason, it really slowed down along the way. Originally 4 min/chunk, then 8. Num calls 
	# per chunk might have varied.

	# Notice: we're failing to optimize since we query for the same exact name many times, but on the plus side, with these sorted voter files, 
	# they're all in a row, so will benefit from caching.

	# WA
	voterfileIn = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/washington.tsv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/public_voters/WA"
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 10000, totVoterRecords = 4575297, numCPUs=2, uniquenessFilterField=filterField))		
	# 39 hrs, or 117k / hr.

	# Now, I've finally implemented (some basic) caching, so it should go faster (fewer queries, provided names are sorted).
	# NC
	voterfileIn = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/north_carolina.tsv"
	outDir = "/home/lfriedl/twitter_matching/mySQL_load_retrieve/public_voters/NC"
	system.time(runOnFileChunks(voterfileIn, outDir, linesPerCall = 10000, totVoterRecords = 6844783, numCPUs=2, uniquenessFilterField=filterField))		
	# (shifting to use allCalls.txt in place of these lines anyway)
	# Note: the later "chunks" take longer (even at the beginning!), so it's probably because of the extra time to reach the starting line?
	# But no: there's a real difference in the number of queries needed. Maybe the voter file has the more common names first? Or there are simply a lot of 
	# people with last names such as "anderson" or "adams".
	# woo! --> something like a 5x speedup from before.
	# At 21.5 hrs, had finished 5,170,000 lines (240k / hr! ), but was now only running on one processor. To balance (among other reasons), I restarted the remaining chunks.

	# MI (using other script): 7.39M records
	# at 9 hours, avg = 318 k / hr
	# at 23.33 hrs, CPU 1 finished its jobs: 5.88M done so far, avg of 250k / hr
	# 11 hrs later, 1360k more done, avg of 124 / hr on the single processor (when doing the caching)
	# total: 38 hrs, so 194k / hr
	# So, load balancing is the next issue.

	# OH: 7891835 records
	# ran 24 + 10.75 hrs before server error, then another 4.25 hrs = 39 hrs total, avg of 202k / hr
	# (using random order so load was balanced)

	# FL: 13622737 records
	# took 74 hrs, or 184k / hr

	# later: "whole" 100k sample. Used allCalls.R, but with tweaks:
	# voterfileIn = voterfiles100k[2]
	# filterFieldForQuerying = "voters_in_zip5"
	# runOnFileChunks() call: add a flag voterFileFormat=1
	# voterfileBase = "natl100k_allUsable"
        # filterFieldForMatching = "voters_in_state"
        # filterFieldBase = substr(filterFieldForMatching, 11, nchar(filterFieldForMatching))
	# matching: do the initial 3 calls from the usual workflow (rule3, rule3 w/RF, rule5 w/RF), adding the flag voterfileOldFormat=T.



}

