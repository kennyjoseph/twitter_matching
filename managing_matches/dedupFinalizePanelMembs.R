
library(data.table)
source("panelDefns.R")

####
# Functions to reason about multiple panels
# See also Readme.md for documentation.
####
# 
# (Call once)
# createDedupedUniverse: given a list of panels, identify duplicate twitter IDs and voters, and reason about which ones are legitimate.
# 	Saves:  -master list of matches + reasoning (to use like a DB).  cols will be: voter_id, twProfileID, source_panels, dup_handling, dup_voterIDs.
#		 	note: each real-person can appear in several data sources (DNC, public, or TSmart). dup_voterIDs connects the IDs across sources.
#			      Within each source, each real-person has 1 row, yet they can belong to several panels. source_panels lists them.
#		-with voter data merged in, one huge list of all ok-to-use matches in the "common input format"; 
#		-one list per panel of matches merged with their raw voter data. Here (only) includes 2 versions of voter ID: voter_id and orig_voter_id.
# 	In the finished universe, we only allow "duplicate" Twitter accounts when they correspond to the same real person. Each voter can be on > 1 panel though.
#	To see a well-defined mapping of people to panels, call getIDsForPanels.
#
###
# Data storage in a universe: (all files generated in createDedupedUniverse)
# prefix + "masterList.csv"			<-- one line per twProfileID + voter_id in match files, mult panels listed per line
# prefix + "inputFormat.csv"			<-- one line per twProfileID + voter_id after deduplication. Panel not listed or assigned. (A few lines are missing for DNC data.)
# prefix + panelName + "-rawFormat.csv"		<-- one file per panel in the universe, lines look just like raw voter data, apart from fields twProfileID, orig_voter_id and voter_id. 
# prefix + panelName + ".twIDs.txt" <-- not strictly necessary, but simpler for times when we don't need to attach voter data
###
#
# (API function -- Call to get panel membership for an arbitrary list of panels)
# Handles how to assign real-people who are actually on multiple panels. Uses precedence list from panelDefns.R.
#
# getIDsForPanels: given a universe and a list of panels, return list of triplets (voter_id + twitter_id + panel)
# (not written) getVoterDataForPanels: given a universe and a list of panels, get triplets, then merge them against 
#	the universe's match files to get voter data in either format.
####



# version 1
# For testing, use panels we actually need right now (Twitter accounts we currently follow):
panelsInUniverse = c("TSmart-natlSample-1", "DNC-natl-1", "DNC-natl-2", "DNC-100k-geo", "TSmart-5fullStates")

# for testing:
#panelsInUniverse = c("TSmart-natlSample-1", "DNC-natl-2", "DNC-100k-geo")
#panelsInUniverse = c("TSmart-5fullStates", "DNC-natl-2", "DNC-100k-geo")


# version 2
panelsInUniverse = c("TSmart-natlSample-2-combo", "DNC-natl-1", "DNC-natl-2", "DNC-100k-geo", "TSmart-5fullStates")	# All that we collect data for
# What we actually want to use in prediction paper
panelsForNatlSample = c("TSmart-natlSample-2-combo", "DNC-natl-2", "DNC-100k-geo")					# All that we want to analyze data for right now
# I must have then run:
# universePath = "~/voter-stuff/panels/universe_july19/"
# createDedupedUniverse(panelsInUniverse, universePath)
# idsForNatlSample = getIDsForPanels(universePath, panelsForNatlSample)
# fwrite(idsForNatlSample, paste0(universePath, "panelIDs-natlSample.csv"))

# version 3 (Sept 2017)
panelsInUniverse = c("TSmart-all-May2017", "TSmart-natlSample-2-combo", "DNC-natl-1", "DNC-natl-2", "DNC-100k-geo")	# Any that we might potentially want to look at
panelsForMillionMan = c("TSmart-all-May2017", "DNC-natl-2", "DNC-100k-geo")	# Keeping the DNC panels for their California people 
# universePath = "~/voter-stuff/panels/universe_sept-13-2017/"	<-- trailing slash is important
# createDedupedUniverse(panelsInUniverse, universePath)
# idsForMillionMan = getIDsForPanels(universePath, panelsForMillionMan)
#  to get data in common input format:
# inputDemogs = fread(paste0(universePath, "inputFormat.csv"), colClasses = list(character=c(21, 26)))
# merged1 = merge(idsForMillionMan, inputDemogs, by=c("twProfileID", "voter_id"))
# fwrite(merged1, paste0(universePath, "people-for-million-man-panel.csv"))
# finally, what I've prepped for Kenny is [probably]: sort *twIDs.txt | uniq > all-union.twIDs.txt 

# version 4 (Oct/Nov 2017)
panelsForTargetSmart = c("TSmart-CA-Oct2017", "TSmart-all-May2017")
panelsInUniverse = c("TSmart-CA-Oct2017", panelsInUniverse)
# universePath = "~/voter-stuff/panels/universe_nov-2-2017-keepDNCaround"	<-- trailing slash is important
# createDedupedUniverse(panelsInUniverse, universePath)
# idsForTargetSmart = getIDsForPanels(universePath, panelsForTargetSmart)
#  to get data in common input format:
# inputDemogs = fread(paste0(universePath, "inputFormat.csv"))
# merged1 = merge(idsForTargetSmart, inputDemogs, by=c("twProfileID", "voter_id"))
# fwrite(merged1, paste0(universePath, "people-for-targetsmart-panel.csv"))

# version 5 (Oct/Nov 2017) -- In this version, I'm just forgetting about the DNC matches. (They're already having their Twitter info collected anyway, regardless of what I do here.) 
panelsForTargetSmart = c("TSmart-CA-Oct2017", "TSmart-all-May2017")
panelsInUniverse = c("TSmart-CA-Oct2017", "TSmart-all-May2017")
# universePath = "~/voter-stuff/panels/universe_nov-2-2017/"	<-- trailing slash is important
# createDedupedUniverse(panelsInUniverse, universePath)
# idsForTargetSmart = getIDsForPanels(universePath, panelsForTargetSmart)
#  to get data in common input format:
# inputDemogs = fread(paste0(universePath, "inputFormat.csv"))
# merged1 = merge(idsForTargetSmart, inputDemogs, by=c("twProfileID", "voter_id"))
# fwrite(merged1, paste0(universePath, "people-for-targetsmart-panel.csv"))

# version 6 (Nov 2017) -- not for general use, just a helper for matching DNC-1 panel to TargetSmart records.
universePath = "~/voter-stuff/panels/matching_universe_nov-22-2017/"
panelsInUniverse = c("DNC-natl-1", "TSmart-CA-Oct2017", "TSmart-all-May2017")
# createDedupedUniverse(panelsInUniverse, universePath)	<-- hacked/stopped before it merges with the raw formats.
 

####
# notes on consistency of voter IDs . . .
# Voter IDs: use input-data format as the universal one here.
# When does input-data ID == raw-data ID?
# 	public: there's only 1 input format
# 	TS: they differ ("ts_" + xx)
# 	DNC: each DNC has a different prefix  <-- this makes things too complicated. Easiest fix: change to a uniform prefix "dnc_2015_"
#      		everywhere we read in a file (i.e., match files and input files), and save it the new way.
####


# Function relies on panelDefns.R:getPanelInfo() to have accurate filenames of (a) sourceMatchFilesWithDups and (b) voterDataFiles.
# Assumes each config$sourceMatchFilesWithDups expands via file globbing to a list of >= 1 file.
# Assumes the voter_ids in voterDataFiles can be translated to those in raw input files by removing everything up to the last underscore.
# Cannot assume sourceMatchFilesWithDups contains fields from common input format; some do, others just have ~5 cols.
# outDirAndPrefix is the start of the file path for all saved files.  Can be just a dir (if so, MUST end in '/'!) or also contain a prefix + "-"
createDedupedUniverse = function(panelNames, outDirAndPrefix) {
	outDir = dirname(paste0(outDirAndPrefix, "testStub"))
	print("Making sure output directory can be written to")
	if (!dir.exists(outDir)) {
		if (! dir.create(outDir, recursive = T) ) {
			stop(paste("Could not create directory", outDir))
		}
	}

	panelsInfo = getAllPanelInfo(panelNames)

	inData = readRawMatches(panelsInfo)	# has cols: voter_id, twProfileID, panel

	print(paste("In match files, found", nrow(inData), "matches, subdivided among panels as:"))
	print(table(inData$panel))



	# make it so each voterID occurs just once. (example where that could happen: TS sample vs. TS full states)
	#dataRows = inData[, .(source_panels = paste(.SD$panel, collapse="||")), by=.(voter_id, twProfileID)]		
	# store panels as vector instead of concatenated string
	dataRows = inData[, .(source_panels = list(.SD$panel)), by=.(voter_id, twProfileID)]

	# an initial col for tracking duplicates
	dataRows$dup_handling = "normal" 	# cols now: voter_id, twProfileID, source_panels, dup_handling


	# report / sanity check about rows and voter <--> twitter pair uniqueness
	print(paste("This comes to", nrow(dataRows), "distinct rows of matches"))
	dupVoters = duplicated(dataRows$voter_id)
	if (sum(dupVoters) > 0) {	# there shouldn't be any, so complain
					# (actually, could potentially happen through non-determinism in classifier)
					# also happens in dnc-1 vs. dnc-2.
		voterIDs_with_dups = unique(dataRows$voter_id[dupVoters])
		pairRows = dataRows[voter_id %in% voterIDs_with_dups,]
		print(paste("Found", length(voterIDs_with_dups), "voter IDs that got matched to different Twitter IDs, e.g.,"))
		print(head(pairRows))
		dataRows[voter_id %in% voterIDs_with_dups, dup_handling := "drop-voter"]  
		# What about cases (which I have in front of me) where a drop-voter row is also a drop-twitter one?
		# Don't want to allow a drop-twitter row to potentially be reconciled/allowed back. Fix: re-store this column after drop-twitter handling.
	} else {
		print("Good, no voter ID got matched to > 1 Twitter ID.")
	}

	# flag non-unique twitter IDs <-- simplest possible identification of duplicates 
	dupTwitter = duplicated(dataRows$twProfileID)
	dupTwitterIDs = unique(dataRows$twProfileID[dupTwitter])
	dataRows[twProfileID %in% dupTwitterIDs, dup_handling := "drop-twitter"]
	print(paste(length(dupTwitterIDs), "Twitter IDs appear more than once"))


	# from the nascent master list (dataRows), grab voter data for each voter x panel. 
	simpleVoterData = mergeInInputVoterData(dataRows, panelsInfo)[, -c("source_panels", "dup_handling"), with=F]   # don't want to see the master_list columns here.

	# Reason about dup Twitter IDs: either they remain not-allowed, or we find voter matches and let them come back.
	dataRows = manageDupVoters(dataRows, simpleVoterData, panelNames)
	# Put drop-voter flags back
	if (sum(dupVoters) > 0) {	
		dataRows[voter_id %in% voterIDs_with_dups, dup_handling := "drop-voter"]  
	}

	# save "master" table (including dups)
	fwrite(dataRows, paste0(outDirAndPrefix, "masterList.csv"))
	
	# delete rows we've decided we definitely don't like
	dedupedData = dataRows[!grepl("drop", dup_handling),]
	print(paste("After dropping matches we don't like, we're left with", nrow(dedupedData), "rows of matches, covering", 
			length(unique(dedupedData$twProfileID)), "distinct Twitter IDs"))
	# note: and from here on, only use dedupedData

	# save all rows (minus dups) with voter data from common input format
	#   (saves voter data for each voter x each panel they're in)
	#matchesCommonInputFormat = merge(dedupedData, simpleVoterData, by="voter_id")  # expanded form (1 row per voter x panel), plus all the tracking columns
	# (prev line created 2 cols of twProfileID)
	matchesCommonInputFormat = merge(dedupedData, simpleVoterData, by=intersect(colnames(dedupedData), colnames(simpleVoterData))) # expanded form (1 row per voter x panel), plus all the tracking columns
	outfile = paste0(outDirAndPrefix, "inputFormat.csv")
	fwrite(unique(matchesCommonInputFormat[, -c("source_panels", "dup_handling", "dup_voterIDs"), with=F]), outfile)	# taking unique rows even here b/c same source file is sometimes the input to >1 panel.

	# for each panel, save:
	#  twitter IDs alone
	for (panelName in panelNames) {
		print(paste("Saving Twitter IDs for panel", panelName))
		#matchesThisPanel = dedupedData[grepl(panelName, source_panels),]
		matchesThisPanel = dedupedData[which(sapply(dedupedData$source_panels, function (x) { panelName %in% x })),] 
		idsOutfile = paste0(outDirAndPrefix, panelName, ".twIDs.txt")
		fwrite(list(matchesThisPanel$twProfileID), file=idsOutfile)
	}

	#   and merged with raw voter data (one row per voter x panel)
	saveWithRawVoterData(dedupedData, panelsInfo, outDirAndPrefix)

}

getAllPanelInfo = function(panelNames) {
	panelInfo = list()	# holds mapping of panel name --> getPanelInfo 
	for (panelName in panelNames) {
		config = getPanelInfo(panelName, printInfo=F)
		panelInfo[[panelName]] = config
	}
	return(panelInfo)
}

readRawMatches = function(panelInfo) {

	# get list of match files, keeping track of panel associations
	matchFilesToCombine = c()
	panels = c()		# holds mapping of file(s) <--> panel name
	for (panelName in names(panelInfo)) {
		config = panelInfo[[panelName]]
		filesToCombine = getFilesFromGlob(config$sourceMatchFilesWithDups)
		matchFilesToCombine = c(matchFilesToCombine, filesToCombine)
		panels = c(panels, rep(panelName, length(filesToCombine)))
	}

	# open all of them and rbind 
	dataTables = list()
	for (i in 1:length(matchFilesToCombine)) {
		matchFile = matchFilesToCombine[i]
		data = fread(matchFile)
		# keep just 2 cols
		data = data[, .(voter_id, twProfileID)]
		data$panel = panels[i]

		# for DNC only, need to convert voter ID so they can be matched across files
		dataSource = computeDataSource(panels[i])
		if (dataSource == "DNC") {
			data$voter_id = paste0("dnc_2015_", gsub(".*_", "", data$voter_id))   # old: as.numeric(gsub(".*_", "", data$voter_id))
		}

		dataTables[[i]] = data
	}
	inData = rbindlist(dataTables, use.names=T)		# cols: voter_id, [unif_voter_id,] twProfileID, [sourceFile,] panel
	return(inData)
}

# returns a data.table of triplets: twID, voterID, panel
getIDsForPanels = function(universePath, panelNames) {

	# 1. Read universe's master file of pairs
	print("Reading master list of panels in universe")
	masterFile = paste0(universePath, "masterList.csv")
	allPairs = fread(masterFile)
	# note: we saved 2 columns as lists, but fread doesn't read them in as such, so need to fix manually. (flag "sep2" isn't implemented yet.)
	allPairs[, source_panels := strsplit(source_panels, "|", fixed=T)]
	allPairs[, dup_voterIDs := strsplit(dup_voterIDs, "|", fixed=T)]
	allPairs = allPairs[!grepl("drop", dup_handling),]	# they're in the master file but never wanted

	# 2. Select rows in union of these panels
	#anyPanelPattern = paste0(panelNames, collapse="|")
	#rowsWeWant = grepl(anyPanelPattern, allPairs$source_panels)
	#goodPairs = allPairs[rowsWeWant,]
	# with vector representation:
	print("Getting data for panels of interest")
	goodPairs = allPairs[sapply(source_panels, function (x) { any(panelNames %in% x) }),]

	# 3. Apply (hard-coded) precedence rules to select 1 row from each clump
	panelPrecedence = getPanelPrecedences()  # use this ordering to break ties.

	rowsPerPanel = list()
	i = 0
	for (nextPanel in panelPrecedence) {
		if (! (nextPanel %in% panelNames)) {
			next
		}
		i = i + 1

		print(paste("Gathering rows for panel", nextPanel))

		# take rows for this panel (by looking in source_panels field)
		wantThisRow = sapply(goodPairs$source_panels, function (x) { nextPanel %in% x })
		rowsToUse = goodPairs[wantThisRow,]
		rowsToUse[, panel := nextPanel]		# assign them to this panel
		rowsPerPanel[[i]] = rowsToUse

		goodPairs = goodPairs[!wantThisRow,]
		# From remaining pool of rows, remove anyone else that shares a cluster with this
		# To identify the rest of the cluster: look in dup_voterIDs for the rows we just grabbed,
		# and remove those voter_ids from the rest of the pool.
		votersToKill = unique(unlist(rowsPerPanel[[i]][dup_handling == "check-voters", dup_voterIDs]))
		goodPairsToKill = (goodPairs$voter_id %in% votersToKill)
		goodPairs = goodPairs[!goodPairsToKill,]
	}
	rowsToKeep = rbindlist(rowsPerPanel, use.names=T)
	if (nrow(goodPairs) > 0) {   # sanity check: rowsToKeep should cover everything in goodPairs (but with each ID once).
		print("warning: didn't use all up the rows of goodPairs")
	}

	# 4. Return data.table of twID, voterID, panel
	return(rowsToKeep[, .(twProfileID, voter_id, panel)])

}

# input: dataRows: data.table that has a column dup_handling that may read "drop-twitter" for dup twitter IDs, and a column source_panels.
#        simpleVoterData: data.table that has twProfileID, voter_id and voter cols from the common input format, 1 row per voter x panel.
# output: same data.table with a new column dup_voterIDs (holding the list -- each equivalence class), and dup_handling changed to "check-voters" for those.
manageDupVoters = function(dataRows, simpleVoterData, panelNames) {

	# May see same-voter in several ways: within same data source vs. across them; matched with same Twitter ID vs. with different ones.
	# -If within the same data source, we'll assume different IDs = different people. (If same IDs, it was treated before this function.)
	# -So we only care about across data sources. (DNC vs. public vs. TSmart)
	# -If matched to different Twitter IDs, we'll assume they're different people.
	# -So: only want to investigate [clusters of] voters that have the same Twitter ID and are (all) from different data sources.
	#  (First coded this as: each row of simpleVoterData (for a given Twitter ID) must come from a different _panel_. But, 
	#  that would have permitted the case of two different voter_ids within different panels from the same source, to be treated as matches.)

	# Among these candidates, we'll consider voters to match iff they have the same first & last names, zipcode, and birthyear.

	# Plan: 
	# -create clusters by twitter ID
	# -iff cluster satisfies my rules (all are from diff data sources; matching voters), then update the dup_handling field.

	# Looking just at voter_ids we care about, merge simpleVoterData with dataRows.
	voterRowsOfInterest = merge(dataRows[dup_handling=="drop-twitter", ], simpleVoterData)

	# Create clusters by twitter ID
	voterClusters = voterRowsOfInterest[,
				.(dup_voterIDs = list(unique(.SD$voter_id)), numDataSources = length(unique(computeDataSource(unlist(.SD$source_panels)))),
				numDistinctFNames = length(unique(.SD$first_name)), numDistinctLNames = length(unique(.SD$last_name)), 
				numDistinctBYears = length(unique(.SD$birth_year)), numDistinctZips = length(unique(.SD$zipcode))), 
				# not actually needed: numInClump = .N, panelsInClump = list(unique(unlist(.SD$source_panels))),
				by=.(twProfileID)]
	print(paste("Duplicate handling: looking at", nrow(voterClusters), "clusters of voters that share twitter IDs"))
	# check cluster conditions
	sameVoterClusters = voterClusters[   #numInClump > 1 & sapply(panelsInClump, length) == numInClump &
					  numDataSources == sapply(dup_voterIDs, length) & numDataSources > 1 & 
					  numDistinctFNames == 1 & numDistinctLNames == 1 & numDistinctBYears == 1 & numDistinctZips == 1,]
	print(paste("Among these,", nrow(sameVoterClusters), "clusters seem to contain just the same voter multiple times"))

	# For twitter IDs we're now happy with,
	# -Merge dup_voterIDs in to dataRows table
	# -Update dup_handling field to "check-voters", which means ok to use, as long as we recognize the duplicates.
	dr2 = merge(dataRows, sameVoterClusters[, .(twProfileID, dup_voterIDs)], all.x=T)  

	# Note that non-matches end up with dup_voterIDs = list(NULL) -- a list of length 0. Fix this: has to be some kind of list, but fwrite
	# can't handle that initial value.

	hadMergeMatch = sapply(dr2$dup_voterIDs, length) > 1
	dr2[hadMergeMatch, dup_handling := "check-voters"]
	dr2[!hadMergeMatch, dup_voterIDs := list("")]
	
	return(dr2)

}

saveWithRawVoterData = function(dataRows, panelInfo, outDirAndPrefix) {
	panelNames = names(panelInfo)
	possVoterIDColNames = c("voter_id", "personid", "voterbase_id")	  # will use the first of these in the raw voter data (each data source should only have 1)

	for (panelName in panelNames) {

		# concatenated representation
		#matchesThisPanel = dataRows[grepl(panelName, source_panels),]
		matchesThisPanel = dataRows[which(sapply(dataRows$source_panels, function (x) { panelName %in% x })),] 

		print(paste("Merging in raw voter data for", panelName))
		config = panelInfo[[panelName]]
		voterDataFiles = getFilesFromGlob(config$voterDataFiles)

		# There's some untracked messiness I need to handle with voter_ids: the input format prepended a panel description to them.
		# I should be able to recover raw file's version of the ID like so:
		#matchesThisPanel$orig_voter_id = as.numeric(gsub(".*_", "", matchesThisPanel$voter_id))   # keep only portion after the last underscore
		# <-- wait, what was as.numeric for? Doesn't work with TSmart.
		matchesThisPanel$orig_voter_id = gsub(".*_", "", matchesThisPanel$voter_id)   # keep only portion after the last underscore

		if (grepl("TSmart", panelName)) {
			charColsArg = list(character=c(9:11, 15, 17, 26, 31:33, 35:36, 43:46, 49:53, 70, 76, 80))
		} else {
			charColsArg = NULL
		}

		matchesList = list()
		for (i in 1:length(voterDataFiles)) {
			gc()
			voterFile = voterDataFiles[i]
			if ("zip" == substr(voterFile, nchar(voterFile) - 2, nchar(voterFile)) {
				voterData = fread(paste("unzip -p", voterFile), colClasses=charColsArg)
			} else {
				voterData = fread(voterFile, colClasses=charColsArg)
			}
			idcolname = possVoterIDColNames[first(which(possVoterIDColNames %in% colnames(voterData)))]
			voterData[[idcolname]] = as.character(voterData[[idcolname]])
			matchesWithVoterData = merge(matchesThisPanel[, .(orig_voter_id, voter_id, twProfileID)], voterData, by.x = "orig_voter_id", by.y = idcolname)
			#setnames(matchesWithVoterData, "orig_voter_id", "voter_id")   # always save as voter_id
			matchesList[[i]] = matchesWithVoterData
		}
		matchesThisPanelVoterData = rbindlist(matchesList, use.names=T)
		if (nrow(matchesThisPanel) == nrow(matchesThisPanelVoterData)) {
			print(paste("Good! Merged in voter data for all", nrow(matchesThisPanel), "matches in the panel"))
		} else {
			print(paste("Had", nrow(matchesThisPanel), "matches; after merging, only", nrow(matchesThisPanelVoterData)))
		}
		outfile = paste0(outDirAndPrefix, panelName, "-rawFormat.csv")
		fwrite(matchesThisPanelVoterData, file=outfile)
	}
		

}

computeDataSource = function(panels) {
	return(ifelse(grepl("DNC", panels), "DNC", ifelse(grepl("public", panels), "public", "TSmart")))
}

# [old/misc]
# Output number of rows should equal input. (Ignores any dup_handling info.) When there are multiple source_panels, this uses data from the first one.
# Saves one big file containing input-format data for every voter row we'll ever use from any panel. Where a voter is in multiple panels, 
# save a row for each panel. (I.e., it's ok for this file to have duplicates.)
# Output number of rows should equal input. (Ignores any dup_handling info.) When there are multiple source_panels, this uses data from the first one.
#saveWithInputVoterData = function(dataRows, panelInfo, outfile) {

# (Now) Returns the join of whatever's in dataRows (probably the master list) with the input format (matching on "voter_id").
# Expects input to contain voter_id and source_panels columns.
# Where a voter is in multiple panels, save a row for each panel. (I.e., it's ok for this file to have duplicates.)
# note: prepped input files lack some rows seen in matches. Can ask KJ, but proceed without them if the percentage stays small.
mergeInInputVoterData = function(dataRows, panelInfo) {
	panelNames = names(panelInfo)

	bigMatchesList = list()
	for (j in 1:length(panelNames)) {
		panelName = panelNames[j]
		dataSource = computeDataSource(panelName)

		#matchesThisPanel = dataRows[grepl(panelName, source_panels),]
		matchesThisPanel = dataRows[which(sapply(dataRows$source_panels, function (x) { panelName %in% x })),] 
		# if we wanted to assign every row to a single panel
		#matchesThisPanel = dataRows[which(sapply(dataRows$source_panels, function (x) { panelName == first(x) })),] 

		print(paste("Merging in common-format voter data for", panelName))
		config = panelInfo[[panelName]]
		voterDataFiles = getFilesFromGlob(config$matchingInputFiles)

		matchesList = list()
		for (i in 1:length(voterDataFiles)) {
			voterFile = voterDataFiles[i]
			voterData = fread(voterFile)
			if (dataSource == "DNC") {
				voterData$voter_id = paste0("dnc_2015_", gsub(".*_", "", voterData$voter_id))   
			}
			
			#matchesWithVoterData = merge(matchesThisPanel[, .(voter_id, twProfileID)], voterData, by = "voter_id")
			matchesWithVoterData = merge(matchesThisPanel, voterData, by = "voter_id")
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
	allMatches = rbindlist(bigMatchesList, use.names=T)
	#fwrite(allMatches, file=outfile)
	return(allMatches)

}

getFilesFromGlob = function(pathString) {
	ps = paste(pathString, collapse=" ")  # to allow for list argument
	magicWords = "shopt -s extglob"		# need to run this once in order for certain [i.e., public data] patterns to work, at the command line anyway
	# the pattern-matching I want only runs in bash, whereas "system" calls only sh. But can explicitly call bash with the commands:
	files = unique(system("bash", input=c(magicWords, paste("ls", ps)), intern=T))
	return(files)

}
