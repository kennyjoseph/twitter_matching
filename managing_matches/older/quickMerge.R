
# for NH, IA, WI: instead of running them through the pipeline, simply take the subset of (existing) match results
# corresponding to the sample.

library(data.table)

# match results live under /net/data/twitter-voters/match-results: 
# targetsmart_fullStates/matches-IA_chunk1-uniqcity-Ctree0.9-rule5-wDups.csv vs. targetsmart_preferred/matches-preferred_AK_chunk1-uniqcity-Ctree0.9-rule5-wDups.csv
matchResultsDirPref     = "/net/data/twitter-voters/match-results/targetsmart_preferred_2" 
matchResultsDirFull     = "/net/data/twitter-voters/match-results/targetsmart_fullStates" 
samplesDir            = "/net/data/twitter-voters/voter-data/preferred_chunks_2"
#statesOfInterest = c("NH", "IA", "WI", "AK", "MN", "PA")
statesOfInterest = "AL"
for (state in statesOfInterest) {
	# read all filesThisState
	allSamples = list.files(samplesDir)
	sampledFilesThisState = allSamples[grepl(paste0("preferred_", state), allSamples)]		# e.g. preferred_AK_chunk1.tsv
	#sampledFilesThisState <- Sys.glob(file.path(samplesDir, paste0("preferred_", state,"*")))

	for (sampleFile in sampledFilesThisState) {
		# grab the fullMatches file for the same chunk
		basename = substr(sampleFile, 1, nchar(sampleFile) - 4)  # chops off ".tsv"
		basename = substr(sampleFile, 11, nchar(basename))  # chops off "preferred_"
		fullMatchFile = Sys.glob(file.path(matchResultsDirFull, paste0("matches-", basename, "*csv")))

		# merge the one with the other on voter_id
		sampledVoters = fread(file.path(samplesDir, sampleFile))$voter_id
		allMatches = fread(fullMatchFile)
		sampledMatches = allMatches[voter_id %in% sampledVoters,]

		# save to appropriate place
		outfile = file.path(matchResultsDirPref, paste0("matches-preferred_",basename, "-uniqcity-Ctree0.9-rule5-wDups.csv"))
		fwrite(sampledMatches, file=outfile)
	}
}
