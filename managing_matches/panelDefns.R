
#####
# Intended usage:
#   source("panelDefns.R")
#   allPanelNames()
#   config = getPanelInfo("IA-NH-1")
#	A list containing: 
#			   matchFile, twitterIDsFile, 	<-- not currently filled in / used; createDedupedUniverse() will create multiple versions, anyway.
#			   voterDataFiles, matchingInputFiles,
# 			   panelDescrip, voterDataFileDescrip, twitterDBDescrip, matchingDescrip
#			   sourceMatchFilesWithDups, constituentPanels
# or, to make life simpler, (?)
#   twitterIDs = getTwitterIDs(twitterPanel="IA-NH-1")
#####

#####
# For documentation's sake, the tweets we've collected correspond to...
# 1. /net/data/twitter-voters/tweets/dnc (was ~kjoseph/political_twitter/tweets/voter_twitter_data_10_10)  <-- now, tweets is old_tweets
#   contains DNC-natl-1 and DNC-IANH-1 (plus a few extras that get removed during deduplication)
#   collected from Oct 2016 to June 2017
# 0. /net/data/twitter-voters/tweets/random
#   (contains data from 100000 randomly selected accounts with U.S. time zone strings)
#   collected May to June 2017
# 2. /net/data/twitter-voters/tweets/public 
#   contains all-DNC-plus-public (plus a few extras that get removed during deduplication)
#   collected from May to June 2017
# 3. /net/data/twitter-voters/tweets_6_23_all			<-- now deleted?
#   contains the union of #2 (all-DNC-plus-public), #0 (random), as much of TSmart-natlSample-1 as was ready at that time (50 places minus 13 states we had full elsewhere), 
#   and all of TargetSmart data for WI, IA and NH.
#   collected during June 2017
# 4. /net/data/twitter-voters/all_tweets_7_5
#   contains TSmart-natlSample-1, #0 (random), all of TS for WI/PA/MN/IA/NH, DNC-natl-1, DNC-natl-2 and DNC-100k-geo
#####

# N.B.: don't make any panel name a substring of another
allPanelNames = function() {
	configNames = c("DNC-natl-1", 		# matched in 2016, used in existing work
			"DNC-IANH-1",		# matched in 2016, data collected with DNC-natl-1 but not used

		"DNC-natl-2", "DNC-IANH-2",	# versions matched in 2017
		"DNC-100k-2", "DNC-100k-geo",

		# (nah, these don't need to be broken out individually)
		#"public-CO", "public-CT", "public-DE", "public-FL", "public-MI", "public-NC", "public-OH", "public-OK", "public-RI", "public-WA", 
		
		"public-10states",
		"TSmart-natlSample-1",
		"TSmart-5fullStates",
		"TSmart-natlSample-oldPeople",
		"TSmart-all-May2017",
		"TSmart-CA-Oct2017",

		# combinations start here:  	
		"TSmart-natlSample-2-combo"	# if we call two panels a "combo" in this file, never have to think of them as separate outside this file.
		# these aren't used, are more like what I'm now calling "universes":
		#"all-DNC-plus-public",			# these tweets have all been collected together
		#"TSmart-natlSample-plus-DNC-natl"	# full TSmart would be a superset of DNC, but this is only a sample  <-- not sure we'll want exactly this, though

		)
	return(configNames)


}

# When we want data from > 1 panel, and the same voter occurs on more than one panel, this ordering dictates which panel gets each person.
getPanelPrecedences = function() {
	# General idea: TSmart > public > DNC
	# 		national samples > full states
	# 		big samples > smaller samples
	# 		newer > older
	c("TSmart-CA-Oct2017", "TSmart-all-May2017", "TSmart-natlSample-2-combo", "TSmart-natlSample-1", "TSmart-natlSample-oldPeople", "TSmart-5fullStates", "public-10states", "DNC-natl-2", 
		"DNC-IANH-2", "DNC-100k-geo", "DNC-100k-2", "DNC-natl-1", "DNC-IANH-1")
}

# To define a panel, (a) add it to the list of all configNames above, and (b) in getPanelInfo, give it at least a panelDescrip.
getPanelInfo = function(panelName, printInfo=T) {

	allVars = list(matchFile=NULL, twitterIDsFile=NULL,
			voterDataFiles=NULL, matchingInputFiles=NULL, 
			panelDescrip="", voterDataFileDescrip="", twitterDBDescrip="", matchingDescrip="",
			sourceMatchFilesWithDups=NULL, constituentPanels=NULL)

	# Descriptions shared by multiple panels
	allVars$twitterIDsFile = "<todo>"	# since we have none yet
	allVars$twitterDBDescrip = paste("About 237 million Twitter profiles (SQL tables: profiles2017*). From ~400 million IDs provided by Alan covering users appearing in the firehose Jan 2014 to Aug 2016.",
					"Profile data pulled March 2017. DB includes only accounts alive at that time and not screened out as 'clearly foreign' (via time zone & language).")
	allVars$matchingDescrip = paste("Matching performed April 2017 or later. Only looks at people whose names are unique at the city level (at least).",
					"'rule 5': Requires a single Twitter name match to the right location and no others to blank/unknown locations; either Twitter profile has correct city & state,",
					"or it has correct state and the name is unique at the state level. A classifier removes some foreign profiles based on profile metadata.")
	if (grepl("TSmart", panelName, ignore.case=T)) {
		if (grepl("Oct2017", panelName)) {
			allVars$voterDataFileDescrip = "TargetSmart data, Oct 2017, containing U.S. residents"
		} else {
			allVars$voterDataFileDescrip = "TargetSmart data, May 2017, containing 231 million U.S. residents"
		}
	} else if (grepl("public", panelName, ignore.case=T)) {
		allVars$voterDataFileDescrip = "Public voter data, downloaded March 2017"
	}  # DNC: varied, so describe each separately



	# Usually 5 vars to set in each: 1 descrip, 2 x input files, 2 x match files (plus twitterIDs eventually); plus optional 1 for constituent pieces
	# Plus voterData descrip for DNC sources.
	# For combinations, inputs and sourceMatchFiles copied automatically; need to provide descrip, matchFile (plus twitterIDs eventually), and constituentPanels
	if (panelName == "TSmart-natlSample-1") {
		allVars$panelDescrip = "National sample from TargetSmart voter data"
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/targetsmart/tsmart_northeastern_install_file_*.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/preferred_chunks/preferred_*_chunk*.tsv"

		allVars$matchFile = "<todo!>"
		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/targetsmart_preferred/matches-preferred_*_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv"
		allVars$constituentPanels = "[samples from each of the individual 49 states + DC, not labeled individually]"

	} else if (panelName == "TSmart-5fullStates") {
		allVars$panelDescrip = "From TargetSmart voter data, all matches from 5 states: IA,NH,WI,PA,MN"
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/targetsmart/tsmart_northeastern_install_file_*.csv"
		allVars$matchingInputFiles = c("/net/data/twitter-voters/voter-data/ts_chunks/IA_chunk*.tsv",
						"/net/data/twitter-voters/voter-data/ts_chunks/NH_chunk*.tsv",
						"/net/data/twitter-voters/voter-data/ts_chunks/WI_chunk*.tsv",
						"/net/data/twitter-voters/voter-data/ts_chunks/PA_chunk*.tsv",
						"/net/data/twitter-voters/voter-data/ts_chunks/MN_chunk*.tsv")

		allVars$matchFile = "<todo!>"
		allVars$sourceMatchFilesWithDups = c("/net/data/twitter-voters/match-results/targetsmart_fullStates/matches-IA_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv",
							"/net/data/twitter-voters/match-results/targetsmart_fullStates/matches-NH_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv",
							"/net/data/twitter-voters/match-results/targetsmart_fullStates/matches-WI_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv",
							"/net/data/twitter-voters/match-results/targetsmart_fullStates/matches-PA_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv",
							"/net/data/twitter-voters/match-results/targetsmart_fullStates/matches-MN_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv")

	} else if (panelName == "TSmart-natlSample-oldPeople") {
		allVars$panelDescrip = "National sample from TargetSmart voter data, oversampling people aged > 45"
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/targetsmart/tsmart_northeastern_install_file_*.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/preferred_chunks_2/preferred_*_chunk*.tsv"

		allVars$matchFile = "<todo!>"
		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/targetsmart_preferred_2/matches-preferred_*_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv"

	} else if (panelName == "TSmart-all-May2017") {
		allVars$panelDescrip = "All matches from TargetSmart voter data (the version provided to us May 2017)"
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/targetsmart/tsmart_northeastern_install_file_*.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/ts_chunks/*_chunk*.tsv"

		allVars$matchFile = "<todo!>"
		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/targetsmart_fullStates/matches-*_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv"

	} else if (panelName == "TSmart-CA-Oct2017") {
		allVars$panelDescrip = "Matches from TargetSmart CA voter data (the version provided to us Oct 2017)"

		# This matching also used extra_state_files/CA_extra.tsv, which comes from the May data.
		# todo: fix the universe deduping I did that only used the main CA raw file.
		allVars$voterDataFiles = c("/net/data/twitter-voters/voter-data/targetsmart_oct2017/tsmart_northeastern_install_file_CA.csv",
					   "/net/data/twitter-voters/voter-data/targetsmart/tsmart_northeastern_install_file_*.csv")

		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/ts_chunks/CA_chunk*.tsv"

		allVars$matchFile = "<todo!>"
		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/targetsmart_Oct2017/matches-CA_chunk*-uniqcity-Ctree0.9-rule5-wDups.csv"

	} else if (panelName == "public-10states") {
		allVars$panelDescrip = "Matches from 10 full states of public voter data"
		#allVars$voterDataFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/raw_voter/public_voter_files/*/"
		#allVars$matchingInputFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/<some *>tsv"
		# now with symbolic links!
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/public/raw"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/public/prepped/+([a-z_]).tsv"	 # this will work at command line only if the command "shopt -s extglob" has been run first
														 # to enable extended file globbing. The point is to exclude "voterfile" DNC ones.

		allVars$matchFile = "<todo!>"
		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/public/matches-*-uniqcityRF0.9-rule5-wDups.csv"
		allVars$constituentPanels = "[each of the individual 10 states, not labeled individually]"

	} else if (panelName == "DNC-natl-2") {
		allVars$panelDescrip = "DNC national sample: now about ~29k matches"
		allVars$voterDataFileDescrip = "DNC data from Jan 2016: national sample of 2 million voters whose names are unique in their state"
		#allVars$voterDataFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/raw_voter/dnc_voter_files/voterfile_unique_national2m.csv"
		#allVars$matchingInputFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/voterfile_unique_national2m.tsv"
		# now with symbolic links!
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/dnc/raw/voterfile_unique_national2m.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/dnc/prepped/voterfile_unique_national2m.tsv"

		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/dnc_2017/matches-natl2m-uniqstateRF0.9-rule5-wDups.csv"
		# (note: the rule3 version is just there for compatibility with fields for 100k data)
		allVars$matchFile = "<todo!>"

	} else if (panelName == "DNC-IANH-2") {
		allVars$panelDescrip = "DNC IA and NH matches: about ~26k matches"
		allVars$voterDataFileDescrip = "DNC data from Sept 2015: all voters from IA or NH whose names are unique in their state"
		#allVars$voterDataFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/raw_voter/dnc_voter_files/voterfile_unique_ia_nh_20150915.csv"
		#allVars$matchingInputFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/voterfile_unique_ia_nh_20150915.tsv"
		# now with symbolic links!
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/dnc/raw/voterfile_unique_ia_nh_20150915.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/dnc/prepped/voterfile_unique_ia_nh_20150915.tsv"

		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/dnc_2017/matches-voterfile_unique_ia_nh_20150915-uniqcityRF0.9-rule5-wDups.csv"
		allVars$matchFile = "<todo!>"

		# Note: there were IA/NH (and 100k) matches from the earlier version of the DB and process, but afaik they were never pulled and used.

	} else if (panelName == "DNC-100k-2") {
		allVars$panelDescrip = "Handful (~800) of matches from DNC's earliest 100k national sample"
		allVars$voterDataFileDescrip = paste("DNC data from June 2015: national sample of 100k likely voters (= either turned out in 2012 or registered after that);",
							"we've joined corresponding name freqs file and subsetted to names unique in their state.")
		#allVars$voterDataFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/raw_voter/dnc_voter_files/voterfile_sample100k_allUsable.csv"
		# now with symbolic links!
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/dnc/raw/voterfile_sample100k_allUsable.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/dnc/prepped/voterfile_sample100k_allUsable.tsv"

		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/dnc_2017/matches-natl100k_allUsable-uniqstateRF0.9-rule5-wDups.csv"
		allVars$matchFile = "<todo!>"

	} else if (panelName == "DNC-100k-geo") {
		allVars$panelDescrip = "Matches (~1200) from DNC's earliest 100k national sample, taking advantage of Derek's geo data"
		allVars$matchingDescrip = paste(allVars$matchingDescrip, "Note: for this data set, used geo data from Derek Ruths when we have none or when it enables a match", sep="\n")
		allVars$voterDataFileDescrip = paste("DNC data from June 2015: national sample of 100k likely voters (= either turned out in 2012 or registered after that);",
							"we've joined corresponding name freqs file and subsetted to names unique in their state.")
		#allVars$voterDataFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/raw_voter/dnc_voter_files/voterfile_sample100k_allUsable.csv"
		# now with symbolic links!
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/dnc/raw/voterfile_sample100k_allUsable.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/dnc/prepped/voterfile_sample100k_allUsable.tsv"

		allVars$sourceMatchFilesWithDups = "/net/data/twitter-voters/match-results/dnc_2017/matches-natl100k_allUsable-uniqstate-geoRuths-RF0.9-rule5-wDups.csv"
		allVars$matchFile = "<todo!>"

	} else if (panelName == "DNC-natl-1") {
		allVars$panelDescrip = "The famous 22k panel used in initial analyses"

		# same voterfile vars as DNC-natl-2
		allVars$voterDataFileDescrip = "DNC data from Jan 2016: national sample of 2 million voters whose names are unique in their state"
		#allVars$voterDataFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/raw_voter/dnc_voter_files/voterfile_unique_national2m.csv"
		#allVars$matchingInputFiles = "/home/kjoseph/twitter_matching/voter_file_aggregation/data/cleaned_voter_files_with_counts/voterfile_unique_national2m.tsv"
		# now with symbolic links!
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/dnc/raw/voterfile_unique_national2m.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/dnc/prepped/voterfile_unique_national2m.tsv"

		allVars$sourceMatchFilesWithDups = "/home/lfriedl/twitterUSVoters/data/twitterDB-matching/match-results/locsFeb3/national2MWithDups-rule3.csv"
		allVars$matchFile = "/home/lfriedl/twitterUSVoters/data/twitterDB-matching/match-results/locsFeb3/national2M-rule3.csv"
		# (note: matchFile has columns from raw input format)

		# Since this is from an older matching process...
		allVars$twitterDBDescrip = paste("About 290 million Twitter profiles (SQL tables: allProfilesW*).", 
						"IDs provided by Alan covering users appearing in the firehose Jan 2014 to June 2015, profile data from their most recent tweet at that time.",
						"(No check to see if account was still alive at time of matching.)")
		allVars$matchingDescrip = paste("Matching performed Feb 2016. 'rule 3': Only looks at people whose names are unique at the state level (at least).",
						"Requires a single Twitter name match to the right location and no others to blank/unknown locations.")

		# Note for archival purposes: there was an intermediate version of the DB prepared but not used much (SQL tables: profiles2016*).
		# It used "account census" data from Devin, and then was updated to cover all of 2015's IDs -- see the data sprawl in /home/lfriedl/twitterUSVoters/data/twitterDB-matching/twitter-acct-status.
		# Matches from that are in twitterUSVoters/data/twitterDB-matching/Oct2016. All ignored, I believe, in favor of the 2017 update.

	} else if (panelName == "DNC-IANH-1") {
		allVars$panelDescrip = "DNC matches from IA and NH (about ~21k), tweets included in the 'dnc' grouping collected from Oct 2016 to Jun 2017"
		
		allVars$voterDataFileDescrip = "DNC data from Sept 2015: all voters from IA or NH whose names are unique in their state"
		allVars$voterDataFiles = "/net/data/twitter-voters/voter-data/dnc/raw/voterfile_unique_ia_nh_20150915.csv"
		allVars$matchingInputFiles = "/net/data/twitter-voters/voter-data/dnc/prepped/voterfile_unique_ia_nh_20150915.tsv"

		allVars$sourceMatchFilesWithDups = "(was never created)"
		# note: these were individually deduped, though. Well anyway, special case, together with DNC-natl-1.
		# note: I went back and successfully ran DNC-natl-1 preserving dups, but didn't bother for these -- don't foresee the need.
		#allVars$sourceMatchFilesWithDups = file.path("/home/lfriedl/twitterUSVoters/data/twitterDB-matching/match-results/locsFeb3", c("IA-rule3.csv", "NH-rule3.csv"))
		allVars$matchFile = file.path("/home/lfriedl/twitterUSVoters/data/twitterDB-matching/match-results/locsFeb3", c("IA-rule3.csv", "NH-rule3.csv"))
		#allVars$matchFile = "(single deduped version never created)"

		# created just like DNC-natl-1
		allVars$twitterDBDescrip = paste("About 290 million Twitter profiles (SQL tables: allProfilesW*).", 
						"IDs provided by Alan covering users appearing in the firehose Jan 2014 to June 2015, profile data from their most recent tweet at that time.",
						"(No check to see if account was still alive at time of matching.)")
		allVars$matchingDescrip = paste("Matching performed Feb 2016. 'rule 3': Only looks at people whose names are unique at the state level (at least).",
						"Requires a single Twitter name match to the right location and no others to blank/unknown locations.")

	} else if (panelName == "all-DNC-plus-public") { 
	# Combinations!!

		allVars$constituentPanels = c("DNC-natl-2", "public-10states", "DNC-IANH-2", "DNC-100k-2")
		allVars$panelDescrip = paste("475,016 matches comprising the (deduplicated) union of public data (10 states), oldest DNC 100k sample, and DNC samples",
					"with names unique in states: 2 million national sample and all of IA and NH. Tweets collected starting May 2017 as 'public' group.") 

		allVars$matchFile = "/home/lfriedl/twitter_matching/add_locs_and_do_match/allMatchesTogether/allMatches-skinny.csv"

	} else if (panelName == "TSmart-natlSample-plus-DNC-natl") {
		allVars$constituentPanels = c("TSmart-natlSample-1", "DNC-natl-2")
		allVars$panelDescrip = "Union of national samples from each of TSmart and DNC"
		
		allVars$matchFile = "<todo!>"
	} else if (panelName == "TSmart-natlSample-2-combo") {
		allVars$constituentPanels = c("TSmart-natlSample-1", "TSmart-natlSample-oldPeople")
		allVars$panelDescrip = "TargetSmart national sample including second pass that filled in older people"
		
		allVars$matchFile = "<todo!>"
	}


	# Special handling for combinations: they need their own descriptions and final output files, 
	# but other lists of files come straight from the constituent sources.
	if (length(allVars$constituentPanels) > 1) {
		for (constit in allVars$constituentPanels) {
			constitConfig = getPanelInfo(constit, printInfo=F)
			if (!is.null(constitConfig)) {
				allVars$voterDataFiles = unique(c(allVars$voterDataFiles, constitConfig$voterDataFiles))
				allVars$matchingInputFiles = unique(c(allVars$matchingInputFiles, constitConfig$matchingInputFiles))
				allVars$sourceMatchFilesWithDups = unique(c(allVars$sourceMatchFilesWithDups, constitConfig$sourceMatchFilesWithDups))
				#allVars$voterDataFileDescrip = paste(allVars$voterDataFileDescrip, ", and, ", constitConfig$voterDataFileDescrip, collapse="")
				allVars$voterDataFileDescrip = paste(unique(allVars$voterDataFileDescrip, constitConfig$voterDataFileDescrip), collapse=", and, ")
			}
		}
	}


	if (! is.null(allVars$panelDescrip)) {
		allVars$panelName = panelName
		if (printInfo) {
			cat(describeConfig(allVars))
		}
		return(allVars)
	} else {
		return(NULL)
	}
}

describeConfig = function(config) {
	msg = "Using files from the following configuration:\n"

	msg = paste0(msg, "Twitter panel = ", config$panelName, "\n\t", 
			config$panelDescrip, "\n")

	if (! is.null(config$constituentPanels)) {
		msg = paste0(msg, "Constituent pieces = ", paste(config$constituentPanels, collapse=" "), "\n")
	}
	msg = paste0(msg, "Match file = ", config$matchFile, "\n")
	msg = paste0(msg, "Voter data = ", config$voterDataFileDescrip, "\n")
	msg = paste0(msg, "Twitter data = ", config$twitterDBDescrip, "\n")
	msg = paste0(msg, "Matching process = ", config$matchingDescrip, "\n")
  
	return(msg)

}

