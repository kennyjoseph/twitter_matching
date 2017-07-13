
library(data.table)
source("panelDefns.R")

####
# Functions to reason about multiple panels
# 
# createDedupedUniverse: given a list of panels, identify duplicate twitter IDs and voters, and reason about which ones are legitimate.
# 	Saves:  -master list of matches + reasoning (to use like a DB).  cols will be: voter_id, twProfileID, source_panels, dup_handling, dup_voterIDs.
#		-with voter data merged in, one huge list of all ok-to-use matches in the "common input format"; 
#		-one list per panel of matches merged with their raw voter data. Here (only) includes 2 versions of voter ID: voter_id and orig_voter_id.
# getTwIDsForPanels: given a universe and a list of panels, 
#	-Consult master list to get IDs (voter_id + twitter_id + panel)
# getVoterDataForPanels: using IDs from getTwIDsForPanels,
#	-Merge against this universe's match files to get voter data in either format
####

####
# For testing, use panels we actually need right now:
panelsInUniverse = c("TSmart-natlSample-1", "DNC-natl-1", "DNC-natl-2", "DNC-100k-geo", "TSmart-5fullStates")
panelsForNatlSample = c("TSmart-natlSample-1", "DNC-natl-2", "DNC-100k-geo")
 
# (when ready to run this for real, will need: 
# (a) to define "TSmart-5fullStates" in config file,
# (b) to fix/update TSmart national sample to include the new part. Could call that one "old", then create a combo with both; 
# but probably we'll just create a new one containing both.)
####

####
# Voter IDs: use input-data format as the universal one here.
# When does input-data ID == raw-data ID?
# public: there's only 1 input format
# TS: they differ ("ts_" + xx)
# DNC: each DNC has a different prefix
#
# Since DNC input files have different prefixes for voter ids, most consistent way to handle dups among them
# is by treating them as different blocks (albeit w/special handling, since no uncertainty in these matches).
# This is so we can continue to have a single voter_id per row of the master list.
####

# Function relies on panelDefns.R:getPanelInfo() to have accurate filenames of (a) sourceMatchFilesWithDups and (b) voterDataFiles.
# Assumes each config$sourceMatchFilesWithDups expands via file globbing to a list of >= 1 file.
# Assumes the voter_ids in voterDataFiles can be translated to those in raw input files by removing everything up to the last underscore.
# Cannot assume sourceMatchFilesWithDups contains fields from common input format; some do, others just have ~5 cols.
# outDirAndPrefix is the start of the file path for all saved files.  Can be just a dir or also contain a prefix + "-"
createDedupedUniverse = function(panelNames, outDirAndPrefix) {

	# get list of match files, keeping track of panel associations
	matchFilesToCombine = c()
	panels = c()		# holds mapping of file(s) <--> panel name
	panelInfo = list()	# holds mapping of panel name --> panelInfo 
	for (panelName in panelNames) {
		config = getPanelInfo(panelName, printInfo=F)
		panelInfo[[panelName]] = config
		filesToCombine = getFilesFromGlob(config$sourceMatchFilesWithDups)
		matchFilesToCombine = c(matchFilesToCombine, filesToCombine)
		panels = c(panels, rep(panelName, length(filesToCombine)))
	}

	# open all of them and rbind     #, keeping track of source file (<-- or not necessary, actually)
	dataTables = list()
	for (i in 1:length(matchFilesToCombine)) {
		matchFile = matchFilesToCombine[i]
		data = fread(matchFile)
		# keep just 2 cols
		data = data[, .(voter_id, twProfileID)]
		data$panel = panels[i]

		#data$sourceFile = matchFile
		#dataSource = ifelse(grepl("DNC", panels[i]), "DNC", ifelse(grepl("public", panels[i]), "public", "TSmart"))
		# for DNC only, need to convert voter ID so they can be matched across files
		#if (dataSource == "DNC") {
		#	data$unif_voter_id = as.numeric(gsub(".*_", "", data$voter_id))
		#} else {
		#	data$unif_voter_id = data$voter_id
		#}

		dataTables[[i]] = data
	}
	inData = rbindlist(dataTables)		# cols: voter_id, [unif_voter_id,] twProfileID, [sourceFile,] panel

	# make it so each voterID occurs just once (example where that could still happen: TS sample vs. full states)
	dataRows = inData[, .(source_panels = paste(.SD$panel, collapse="||")), by=.(voter_id, twProfileID)]		
	# set up cols to track/handle duplicates
	dataRows$dup_handling = "normal" 	# cols now: voter_id, twProfileID, source_panels

	print(paste("Across all the panels, had", nrow(inData), "rows of matches, or", nrow(dataRows), "distinct rows of matches"))

	# sanity check
	dupVoters = duplicated(dataRows$voter_id)
	if (sum(dupVoters) > 0) {	# there shouldn't be any, so complain
					# (actually, could potentially happen through non-determinism in classifier)
		voterIDs_with_dups = unique(dataRows$voter_id[dupVoters])
		pairRows = dataRows[voter_id %in% voterIDs_with_dups,]
		print(paste("Found", length(voterIDs_with_dups), "voter IDs that got matched to different Twitter IDs, e.g.,"))
		print(head(pairRows))
		dataRows[voter_id %in% voterIDs_with_dups, dup_handling := "drop-voter"]  
	} else {
		print("Good, no voter ID got matched to > 1 Twitter ID. (Shouldn't be possible anyway.)")
	}

	# get list of non-unique twitter IDs <-- simplest possible identification of duplicates 
	dupTwitter = duplicated(dataRows$twProfileID)
	dupTwitterIDs = unique(dataRows$twProfileID[dupTwitter])
	dataRows[twProfileID %in% dupTwitterIDs, dup_handling := "drop-twitter"]
	print(paste(length(dupTwitterIDs), "Twitter IDs appear more than once"))


	# todo:
	# once we're smart, decide which rows can actually stay
	# requires merging against voter data (use input format) to compare. 
	# (voter_id should match exactly)
	dataRows = manageDupVoters(dataRows, panelNames)


	# delete rows we don't like
	dedupedData = dataRows[!grepl("drop", dup_handling),]

	# save data:
	#   whole "master" table (including dups)
	fwrite(dataRows, paste0(outDirAndPrefix, "allPairs.csv"))
	
	#   all rows (minus dups) with voter data from common input format
	outfile = paste0(outDirAndPrefix, panelName, "-inputFormat.csv")
	saveWithInputVoterData(dataRows, panelInfo)

	# for each panel, save:
	#  twitter IDs alone
	for (panelName in panelNames) {
		print(paste("Saving Twitter IDs for panel", panelName))
		matchesThisPanel = dedupedData[grepl(panelName, source_panels),]
		idsOutfile = paste0(outDirAndPrefix, panelName, ".twIDs.txt")
		fwrite(list(matchesThisPanel$twProfileID), file=idsOutfile)
	}

	#   and merged with raw voter data
	saveWithRawVoterData(dataRows, panelInfo)

}


getTwIDsForPanels = function(universePath, panelNames) {

	# 1. Read universe's master file of pairs
	masterFile = paste0(universePath, "allPairs.csv")
	allPairs = fread(masterFile)
	allPairs = allPairs[!grepl("drop", dup_handling),]	# they're in the master file but never wanted

	# 2. Select rows in union of these panels
	anyPanelPattern = paste0(panelNames, collapse="|")
	rowsWeWant = grepl(anyPanelPattern, allPairs$source_panels)
	goodPairs = allPairs[rowsWeWant,]

	# 3. Apply (hard-coded) precedence rules to select 1 row from each clump
	# todo!

	# 4. Return data.table of twID, voterID, panel

}

# input: data.table that has a column dup_handling that may read "drop-twitter" for dup twitter IDs, and a column source_panels.
# output: same data.table with a new column dup_voterIDs (holding the list -- each equivalence class), and dup_handling changed to "check-voters" for those.
manageDupVoters = function(dataRows, panelNames) {

	# 1. First, find rows referring to the same DNC voter
	if (sum(grepl("DNC", panelNames)) > 1) {   # relevant if using more than one DNC data source
		isDNC = grepl("DNC", dataRows$source_panels)
		dataRows$orig_voter_id = as.numeric(gsub(".*_", "", dataRows$voter_id))   # keep only portion after the last underscore

		# note: only considering matches, here, in which both voters got linked to the same TwitterID. 
		# It's conceivable they might not have. But that would require a second check, and I'm not sure what I'd do if I knew.
		voterClumps = dataRows[isDNC, .(numInClump = .N, dup_voterIDs = paste(.SD$voter_id, collapse="||")), by=.(orig_voter_id, twProfileID)]
		# has cols: orig_voter_id, dup_voterIDs, twProfileID, numInClump

		withNeighbors = merge(dataRows, voterClumps[numInClump > 1, .(orig_voter_id, twProfileID, dup_voterIDs)], all.x = T, by = c("orig_voter_id", "twProfileID"))
		dataRows = withNeighbors
		dataRows[, orig_voter_id := NULL]
	}


	# 2. More complex checking (not coded yet)


	# 3. Finally, all rows with a non-NA field for dup_voterIDs --> ok to use, as long as we recognize the duplicates.
	dataRows[!is.na(dup_voterIDs), dup_handling := "check-voters"]

	return(dataRows)

}

saveWithRawVoterData = function(dataRows, panelInfo) {
	panelNames = names(panelInfo)

	for (panelName in panelNames) {

		matchesThisPanel = dedupedData[grepl(panelName, source_panels),]

		print(paste("Merging in raw voter data for", panelName))
		config = panelInfo[[panelName]]
		voterDataFiles = getFilesFromGlob(config$voterDataFiles)

		# There's some untracked messiness I need to handle with voter_ids: the input format prepended a panel description to them.
		# I should be able to recover raw file's version of the ID like so:
		matchesThisPanel$orig_voter_id = as.numeric(gsub(".*_", "", matchesThisPanel$voter_id))   # keep only portion after the last underscore

		matchesList = list()
		for (i in 1:length(voterDataFiles)) {
			voterFile = voterDataFiles[i]
			voterData = fread(voterFile)
			idcolname = ifelse("voter_id" %in% colnames(voterData), "voter_id", "personid")
			matchesWithVoterData = merge(matchesThisPanel[, .(orig_voter_id, twProfileID)], voterData, by.x = "orig_voter_id", by.y = idcolname)
			setnames(matchesWithVoterData, "orig_voter_id", "voter_id")   # always save as voter_id
			matchesList[[i]] = matchesWithVoterData
		}
		matchesThisPanelVoterData = rbindlist(matchesList)
		if (nrow(matchesThisPanel) == nrow(matchesThisPanelVoterData)) {
			print(paste("Good! Merged in voter data for all", nrow(matchesThisPanel), "matches in the panel"))
		} else {
			print(paste("Had", nrow(matchesThisPanel), "matches; after merging, only", nrow(matchesThisPanelVoterData)))
		}
		outfile = paste0(outDirAndPrefix, panelName, "-rawFormat.csv")
		fwrite(matchesThisPanelVoterData, file=outfile)
	}
		

}

saveWithInputVoterData = function(dataRows, panelInfo, outfile) {
	panelNames = names(panelInfo)

	bigMatchesList = list()
	for (j in 1:length(panelNames)) {
		panelName = panelNames[j]

		matchesThisPanel = dedupedData[grepl(panelName, source_panels),]

		print(paste("Merging in common-format voter data for", panelName))
		config = panelInfo[[panelName]]
		voterDataFiles = getFilesFromGlob(config$matchingInputFiles)

		matchesList = list()
		for (i in 1:length(voterDataFiles)) {
			voterFile = voterDataFiles[i]
			voterData = fread(voterFile)
			idcolname = ifelse("voter_id" %in% colnames(voterData), "voter_id", "personid")
			matchesWithVoterData = merge(matchesThisPanel[, .(voter_id, twProfileID)], voterData, by = "voter_id")
			matchesList[[i]] = matchesWithVoterData
		}
		matchesThisPanelVoterData = rbindlist(matchesList)
		bigMatchesList[[j]] = matchesThisPanelVoterData
		if (nrow(matchesThisPanel) == nrow(matchesThisPanelVoterData)) {
			print(paste("Good! Merged in voter data for all", nrow(matchesThisPanel), "matches in the panel"))
		} else {
			print(paste("Had", nrow(matchesThisPanel), "matches; after merging, only", nrow(matchesThisPanelVoterData)))
		}
	}
	allMatches = rbindlist(bigMatchesList)
	fwrite(allMatches, file=outfile)

}

getFilesFromGlob = function(pathString) {
	magicWords = "shopt -s extglob"		# need to run this once in order for certain [i.e., public data] patterns to work, at the command line anyway
	# the pattern-matching I want only runs in bash, whereas "system" calls only sh. But can explicitly call bash with the commands:
	files = system("bash", input=c(magicWords, paste("ls", pathString)), intern=T)
	return(files)

}
