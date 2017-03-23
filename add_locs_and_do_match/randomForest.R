# This file: code that computes and adds a probMetaForeign column for the candidate matches, using profile meta-data only (e.g., time zones and languages).
# Function to call:
# addPredictionToCandMatches(infile="data/allCandidateMatchLocs2M.csv", outfile="data/allCandidateMatchLocProbs2M.csv", downsampleTrainingFactor=.5, saveModelFile="data/partyT_from_natl2M.rds")
# 
# The feature preprocessing is ad hoc. In an effort to reduce the number of feature values (b/c of random forest's limitations, 
# which are no longer relevant), I coalesced values that seemed like they'd make sense to, but I didn't 
# experiment much with those choices. Using all the original labels might be fine (or even preferred) with ctree().


#library(randomForest)	# apparently its variable selection is biased in favor of continuous vars and discrete with many splits.
library(party)		# cparty solves those issues. I'll try both.
library(data.table)
library(ROCR)		# for computing AUC

# note, whoops: geoCoords got truncated/dropped back at my first script in the pipeline: cleanProfilesIndexNames.R

# downsampleTrainingFactor: defaults to 1 = don't downsample. I used .5 for the natl2M set.
# reusing model file DOES NOT WORK yet, because model can't handle feature values not seen at training time.
#   If we want to make it work, (a) set numToKeep to not truncate any, and (b) delete newdata values not seen in 
#   training data, which is stored in partyT@data@get("input").
addPredictionToCandMatches = function(infile, outfile, saveModelFile=NULL, useModelFile=NULL, downsampleTrainingFactor=1) {

	dataWithFeatures = prepLabeledData(infile, trainingOnly = F)	
	data = dataWithFeatures$data
	featureNames = dataWithFeatures$features

	# specify rows for training, applying
	hasUsefulLoc = !(data$inferredLoc %in% c("", "unparsed"))

	# for speed considerations, only use half of these?
	actuallyUse = sample(nrow(data), downsampleTrainingFactor * nrow(data))	# gives a bunch of row numbers
	actuallyUseBool = (1:nrow(data)) %in% actuallyUse

	if (!is.null(useModelFile)) {
		partyT = readRDS(useModelFile)
	} else {
		# learn a ctree model
		partyT = ctree(isForeign ~ profileLang2 + tweetLang2 + timeZoneOffset2 + timeZoneName2 + yearCreated + isProtected 
											+ isVerified + numFollowers + numFollowing + numTweets, 
								data = as.data.frame(data)[hasUsefulLoc & actuallyUseBool,])
	}

	if (!is.null(saveModelFile)) {
		saveRDS(partyT, saveModelFile)
	}

	# apply it to all rows (including those it trained on, to keep things simple)
	partyPreds = treeresponse(partyT, newdata = as.data.frame(data))
	partyProbs = matrix(unlist(partyPreds), byrow=T, ncol=2)[,2]

	# report AUC on labeled data
	predObj = prediction(partyProbs[hasUsefulLoc], data$inferredLoc[hasUsefulLoc] == "foreign")
	print(paste("AUC for labeled data:", performance(predObj, "auc")@y.values[[1]]))

	# save results
	data$probMetaForeign = partyProbs
	fwrite(data, file=outfile)

}

# Used while experimenting/testing
runXVal = function() {
	infile = "data/allCandidateMatchLocs2M.csv"
	labeledData = prepLabeledData(infile)

	featureCols = 1:(ncol(labeledData) - 1)
	responseCol = ncol(labeledData)

	fold = sample(1:10, nrow(labeledData), replace=T)

	for (i in 1:10) {
		# downsample the data to run faster while debugging/testing 
		actuallyUse = sample(nrow(labeledData), .2*nrow(labeledData))	# gives a bunch of row numbers
		actuallyUseBool = (1:nrow(labeledData)) %in% actuallyUse
		# temp
		#downweightNegs = sample(nrow(labeledData), .5 * nrow(labeledData)) 
		#actuallyUseBool2 = actuallyUseBool & ((1:nrow(labeledData)) %in% downweightNegs | labeledData[[responseCol]] == "TRUE")

		# construct train/test sets for this fold
		rfTrainData = as.data.frame(labeledData)[fold != i & actuallyUseBool, featureCols]
		rfTrainResp = as.data.frame(labeledData)[fold != i & actuallyUseBool, responseCol]
		rfTestData = as.data.frame(labeledData)[fold==i & actuallyUseBool, featureCols]
		rfTestResp = as.data.frame(labeledData)[fold==i & actuallyUseBool, responseCol]

		rfRetVal = randomForest(x=rfTrainData, y=rfTrainResp, xtest=rfTestData, ytest=rfTestResp)
		#rfRetVal = randomForest(x=rfTrainData, y=rfTrainResp, xtest=rfTestData, ytest=rfTestResp, importance=T)	# to get estimates of variable importance

		# calculate AUC:
		predObj = prediction(rfRetVal$votes[,2], rfTrainResp)
		x1 = performance(predObj, "auc")	# look at the number in x1@y.values[[1]]
		print(paste("AUC on training data:", x1@y.values[[1]]))

		predObj = prediction(rfRetVal$test$votes[,2], rfTestResp)
		x1 = performance(predObj, "auc")
		print(paste("AUC on test data:", x1@y.values[[1]]))

		print(paste("Confusion matrix for test data using cutoff of", .9))
		printConfusionMatrix(rfRetVal$test$votes[,2], rfTestResp, .9)

		# What seems to be happening with RF, out of the box, is that it decides to optimize accuracy on imbalanced classes by predicting FALSE every time.
		# Try balancing the training classes?
		# That didn't actually change much. Maybe it's inherent to the problem that it's easier to tell when something is FALSE (i.e., U.S.) and harder to be sure if it's foreign.
		# In both cases, AUC on both training and test was about 0.70.

		partyT = ctree(isForeign ~ profileLang2 + tweetLang2 + timeZoneOffset2 + timeZoneName2 + yearCreated + isProtected 
											+ isVerified + numFollowers + numFollowing + numTweets, 
								data = as.data.frame(labeledData)[fold != i & actuallyUseBool,])
		partyPredTrain = treeresponse(partyT, newdata = as.data.frame(labeledData)[fold != i & actuallyUseBool,])
		partyProbsTrain = matrix(unlist(partyPredTrain), byrow=T, ncol=2)[,2]

		partyPredTest = treeresponse(partyT, newdata = as.data.frame(labeledData)[fold == i & actuallyUseBool,])
		partyProbsTest = matrix(unlist(partyPredTest), byrow=T, ncol=2)[,2]

		# AUCs:
		predObj = prediction(partyProbsTrain, labeledData$isForeign[fold != i & actuallyUseBool])
		x1 = performance(predObj, "auc")	# look at the number in x1@y.values[[1]]
		print(paste("AUC on training data:", x1@y.values[[1]]))
		predObj = prediction(partyProbsTest, labeledData$isForeign[fold == i & actuallyUseBool])
		x1 = performance(predObj, "auc")	# look at the number in x1@y.values[[1]]
		print(paste("AUC on training data:", x1@y.values[[1]]))

		# Switching to ctree() immediately gets us to AUC of 0.84. (cforest() took forever on my machine, so I haven't gone back to it quite yet.)
		# Plus, it gives reasonable confusion matrices. Looks like we could use a cutoff of .7-.95 to mark 10-20% of records as foreign (where in truth, 33% are foreign)
		# with relatively few false foreigns (.5-1% of U.S. marked foreign).
		# ctree() still gets 0.84 if you use numToKeep=1000 for all the feature preprocessing. (Which still manually recodes some, but has higher cardinality.)
		# When increasing training data by removing the use of actuallyUseBool, AUC goes up to .844 on train & test (from .841).

		# (Took ~40 min on achtung--maybe because using cforest_unbiased as recommended by https://journal.r-project.org/archive/2009-2/RJournal_2009-2_Strobl~et~al.pdf)
		partyForest = cforest(isForeign ~ profileLang2 + tweetLang2 + timeZoneOffset2 + timeZoneName2 + yearCreated + isProtected
		                                            + isVerified + numFollowers + numFollowing + numTweets,
		                                data = as.data.frame(labeledData)[fold != i & actuallyUseBool,], controls=cforest_unbiased(mtry=3, ntree=50))
		partyPredTrain = treeresponse(partyForest, newdata = as.data.frame(labeledData)[fold != i & actuallyUseBool,])
		partyPredTest = treeresponse(partyForest, newdata = as.data.frame(labeledData)[fold == i & actuallyUseBool,])
		partyProbsTest = matrix(unlist(partyPredTest), byrow=T, ncol=2)[,2]
		# got AUC of .83 on test data
		

		# Naive Bayes gets .82-.83. So, that's the right ballpark for a simple model.
		 nb = naiveBayes(isForeign ~ profileLang2 + tweetLang2 + timeZoneOffset2 + timeZoneName2 + yearCreated + isProtected
		                  + isVerified + numFollowers + numFollowing + numTweets,
		                  data = as.data.frame(labeledData)[fold != i & actuallyUseBool,], laplace=1)
		nb_predsTrain = predict(nb, newdata = as.data.frame(labeledData)[fold != i & actuallyUseBool,], type="raw")
		nb_predsTest = predict(nb, newdata = as.data.frame(labeledData)[fold == i & actuallyUseBool,], type="raw")

		predObj = prediction(nb_predsTrain[,2], labeledData$isForeign[fold != i & actuallyUseBool])
		x1 = performance(predObj, "auc")	# look at the number in x1@y.values[[1]]
		print(paste("AUC on training data:", x1@y.values[[1]]))
		predObj = prediction(nb_predsTest[,2], labeledData$isForeign[fold == i & actuallyUseBool])
		x1 = performance(predObj, "auc")	# look at the number in x1@y.values[[1]]
		print(paste("AUC on training data:", x1@y.values[[1]]))
	}	
}


printConfusionMatrix = function(votes, truth, cutoff) {
		  print("                FALSE   TRUE (predicted)")
    print(paste("(truth) FALSE", sum(votes < cutoff & truth == "FALSE"), sum(votes >= cutoff & truth == "FALSE"),  collapse="\t"))
    print(paste("        TRUE", sum(votes < cutoff & truth == "TRUE"), sum(votes >= cutoff & truth == "TRUE"),  collapse="\t"))
}

# Returns data.table with 10 preprocessed features + a label (isForeign, derived from inferredLoc).
# If trainingOnly, returns only those columns and rows.
# Else returns the whole thing for further work. In which case, still need to filter rows before building classifier.
prepLabeledData = function(infile, trainingOnly=T) {
	candMatches = fread(infile)

	# As training data, we'll use all lines for which a location was inferrable from the location string
	# (Doesn't matter if it matched, just that we know the location)
	if (trainingOnly) {
		candMatches = candMatches[!(inferredLoc %in% c("", "unparsed")),]
	}
	training = candMatches

	# a bunch of columns are unlabeled after twProfileLoc and before locContainsCity and inferredLoc
	colnames(training)[11:25] = c("descriptionString", "urlString", "acctCreationDate", "lastSeenDate",
					"numFollowers", "numFollowing", "numTweets", "isProtected", "isVerified", 
					"timeZoneOffset", "timeZoneName", "profileLang", "tweetLang", "geoCoords", "nameHandleWords")

	# make the label a factor so RF knows we're doing classification
	training$isForeign = as.factor(training$inferredLoc == "foreign")

	# RF can handle discrete inputs, but only up to 32 values each
	training$yearCreated = as.factor(substr(training$acctCreationDate, 1, 4))

	# some preprocessing
	training$profileLang2 = preprocProfileLang(training$profileLang)
	training$tweetLang2 = preprocTweetLang(training$tweetLang)
	training$timeZoneOffset2 = preprocTZOffset(training$timeZoneOffset)
	training$timeZoneName2 = recodeTimeZoneLabel(training$timeZoneName)

	features = c("profileLang2", "tweetLang2", "timeZoneOffset2", "timeZoneName2", "yearCreated", "isProtected", "isVerified",
			"numFollowers", "numFollowing", "numTweets")	# not sure why these last 3 would matter, but doesn't hurt to try

	if (trainingOnly) {
		return(training[, c(features, "isForeign"), with=F])
	} else {
		return(list(data=training, features=features))
	}
}

preprocTZOffset = function(tzOffset, numToKeep = 32) {
	# some inputs are NA
	tzOffset[is.na(tzOffset)] = "unknown"

	# legitimate U.S. offsets: -28800 <= utc <= -18000 OR utc == -32400 or -36000 
	# all are among most popular, fortunately
	# values are in seconds, so, e.g., Eastern = -18000 = -5 * 60 * 60
	# ughhh -- just realized I'd forgotten about daylight savings. At which point Eastern goes to -4 * 3600 = -14400,
	# which, fortunately, is still prominent in our data. But was preferentially screened out before. :(

	# Taking a quick look at the offsets labeled foreign: after "unknown", most popular is 0 (Britain), but 
	# then they get into U.S. places. Could be b/c of other Western Hemisphere countries, and maybe Twitter also 
	# defaults to putting people in Pacific time.

	# I can't think of anything fancy after all, so just truncate like with the others
	t1 = sort(table(tzOffset), d=T)
	tzOffset[!( tzOffset %in% names(t1[1:(numToKeep - 1)]) )] = "rare"

	# make it a factor
	tzOffset = as.factor(tzOffset)
	return(tzOffset)

}

preprocProfileLang = function(profileLang, numToKeep=32) {
	profileLang = tolower(profileLang)

	# Change rare language markers to "rare", keeping the first 31.
	# But first, treat "en-*" specially:
	profileLang[profileLang == "en-us"] = "en"
	profileLang[substr(profileLang, 1, 3) == "en-"] = "en-otherCountry"

	t1 = sort(table(profileLang), d=T)
	profileLang[!( profileLang %in% names(t1[1:(numToKeep - 1)]) )] = "rare"

	# make it a factor
	profileLang = as.factor(profileLang)
	return(profileLang)
}

# (note: language of most recent tweet == "" iff profile is protected. versus "und", which means
# twitter couldn't figure out the language.)
preprocTweetLang = function(tweetLang, numToKeep = 32) {
	# copying and pasting from above, hence the variable name
	profileLang = tolower(tweetLang)

	t1 = sort(table(profileLang), d=T)
	profileLang[!( profileLang %in% names(t1[1:(numToKeep - 1)]) )] = "rare"

	# make it a factor
	profileLang = as.factor(profileLang)
	return(profileLang)
}


# time zone text is important: it further differentiates the UTC offset.
# Too many labels. Most popular are plain names of time zones or cities, but the form "Europe/London" is also present.
# Want to treat the US region carefully, and collapse others if they're rare.
recodeTimeZoneLabel = function(tzName, numToKeep = 20) {

	t3 = table(tzName)
	asiaSlash = names(t3)[grepl("Asia/", names(t3))]
	tzName[tzName %in% asiaSlash] = "Asia/"
	europeSlash = names(t3)[grepl("Europe/", names(t3))]
	tzName[tzName %in% europeSlash] = "Europe/"
	africaSlash = names(t3)[grepl("Africa/", names(t3))]
	tzName[tzName %in% africaSlash] = "Africa/"
	australiaSlash = names(t3)[grepl("Australia/", names(t3))]
	tzName[tzName %in% australiaSlash] = "Australia/"
	pacificSlash = names(t3)[grepl("Pacific/", names(t3))]
	tzName[tzName %in% pacificSlash] = "Pacific/"
	# when first testing, the above lines reduced number of labels from 189 (only) to 170.
	# in real/full data, it takes them from 268 to 213

	# in addition, there are potentially a bunch with "America/Indiana*"   (though actually not in our data)
	indianaSlash = names(t3)[grepl("America/Indiana", names(t3))]
	tzName[tzName %in% indianaSlash] = "America/Indiana"

	# no need to touch these
	mainUSTimeZoneStrings = c("Arizona", "Hawaii", "Alaska", "Indiana (East)",
				 "Eastern", "Central", "Mountain", "Pacific")
        
	# complete list of US named zones from https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
	USVarious = c( "America/Chicago", "America/Denver", "America/New_York", "America/Los_Angeles",
			 "America/Boise", "America/Detroit", "America/Anchorage", "America/Phoenix",
			 "America/Fort_Wayne", "America/Indiana",
			 "America/Juneau", "America/Kentucky/Louisville", "America/Kentucky/Monticello", "America/Knox_IN", "America/Louisville",
			 "America/Menominee", "America/Metlakatla", "America/Nome", "America/Sitka", "America/Yakutat", 
			 "America/North_Dakota/Beulah", "America/North_Dakota/Center", "America/North_Dakota/New_Salem", 
			 "Navajo", "US/Alaska", "US/Aleutian", "US/Arizona", "US/Central", "US/Eastern", "US/East-Indiana",
			 "US/Hawaii", "US/Indiana-Starke", "US/Michigan", "US/Mountain", "US/Pacific", "US/Pacific-New")
	# and a bunch of observed abbreviations
	USVarious = c(USVarious, "EST", "EDT", "CST", "CDT", "PST", "PDT", "MST", "MDT", "GMT-4", "GMT-5", "GMT-6", "GMT-7", "GMT-8")
 
	tzName[tzName %in% USVarious] = "America/variousUS"	

	otherAmericaSlash = names(t3)[grepl("America/", names(t3))]
	tzName[tzName %in% otherAmericaSlash & tzName != "America/variousUS"] = "America/nonUS"	# down to 164
	
	basicallyLondon = c("UTC", "GMT", "BST")
	tzName[tzName %in% basicallyLondon] = "London"

	# Let's try to distinguish US vs. non-US for cities sharing our time zones
	# *** May want to expand this list based on names we observe ***
	placesNonUS = c("Bogota", "Central America", "Chihuahua", "Guadalajara", "Lima", "Mazatlan", "Mexico City",
			"Monterrey", "Saskatchewan", "Tijuana")  # left out Quito b/c it's so popular
	tzName[tzName %in% placesNonUS] = "America/nonUS"

	# so now, collapse rare names, but make sure to keep around these 8:
	importantLocalTZ = c(mainUSTimeZoneStrings, "America/nonUS", "America/variousUS")
	numNotChangeable = 2 + length(importantLocalTZ) 	# length(importantLocalTZ) + "" + "foreign"
	
	# What do freqs look like outside those main ones
	t4 = table(tzName[ !(tzName %in% importantLocalTZ) & tzName != ""])
	numFreqToKeep = max(0, numToKeep - numNotChangeable)
	placesToKeep = names(sort(t4, d=T)[1:numFreqToKeep])	

	#numFreqToKeep = min(53, numToKeep - numNotChangeable)
	#placesToKeep = names(sort(t4, d=T)[1:numFreqToKeep])	# 43 most frequent, besides "" and importantLocalTZ. 
							# (That number chosen because + "" + importantLocalTZ + "foreign" gives 53 == RF's max allowed categories.)
	tzName[ !(tzName %in% importantLocalTZ) & tzName != "" & !(tzName %in% placesToKeep)] = "rare"

	tzName = as.factor(tzName)
	return(tzName)
}

