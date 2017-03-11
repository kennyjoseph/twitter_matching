

source("cleanProfilesIndexNames.R")

# Code in this file: makes calls to preprocessProfileTextFileParallel() from cleanProfilesIndexNames.R. Many times.
# See the very bottom for live / best version, which calls runOnManyFiles().

runOnManyFiles = function(inDir, inFiles, outDir) {

	fullInFiles = file.path(inDir, inFiles)
	outFs = vector(mode="character", length=length(inFiles))
	outFDs = vector(mode="character", length=length(inFiles))
	# do a little loop as the easiest way to fill in outFs and outFDs
	messageFiles = vector(mode="character", length=length(inFiles))

	fileCnt = 0
	for (filePiece in inFiles) {
		fileCnt = fileCnt + 1

		f2 = filePiece
		substr(f2, start=nchar(f2) - 2, stop=nchar(f2)) = "csv"
		outF = file.path(outDir, f2)
		outFs[fileCnt] = outF

		substr(f2, start=nchar(f2) - 2, stop=nchar(f2)) = "out"
		messageF = file.path(outDir, f2)
		messageFiles[fileCnt] = messageF

		substr(f2, start=nchar(f2) - 13, stop=nchar(f2)) = "foreign.csv.gz"
		outFD = file.path(outDir, f2)
		outFDs[fileCnt] = outFD
	}

	print("Created list of args, about to initialize the cluster")
	initSnowfall()

	print("About to make the big call! Check *.out files in output directory to see stdout from each run")
	files = cbind(fullInFiles, outFs, outFDs, messageFiles)
	#sfClusterApplyLB(files, 1, function(x) safeClusterDoOneFile(x[1], x[2], x[3]))	 <-- wrong b/c it doesn't do margins, only single vector arg
	sfApply(files, 1, function(x) capture.output(safeClusterDoOneFile(x[1], x[2], x[3]), file=x[4]))
	print("finished running!!")

}

initSnowfall = function() {
	require(snowfall)
	sfInit(parallel=TRUE,cpus=10)
	# components for making isDefinitelyForeign work:
	sfLibrary(stringi)
	sfExport("onlyForeignAlphabet", "isDefinitelyForeign", "isDefinitelyForeignVectorized")
	sfExport("USTimeZoneStrings")
	# components for making putThemTogether work:
	sfExport("putThemTogether", "getWordsFromTwitterHandle", "splitHandleNative", "getWordsFromTwitterName",
			"getWordsFromTwitterNameForHandleMatching", "processNameText", "getHandleChunksUsingNameWords",
			"assembleAlignment", "getContinguousChunksFromDataStructure", "getMatchedChunksFromDataStructure",
			"itemizeAvailableChunks")
	# the big function itself
	sfLibrary(data.table)
	sfExport("preprocessProfileTextFileParallel", "safeClusterDoOneFile")
	# helpers I didn't export before
	sfExport("makeDate", "fixGPS")
}


safeClusterDoOneFile = function(inFile, outF, outFD) {
		print(paste("At", Sys.time(), "starting to work on inFile", inFile, ", with outF=", outF, "and outFD=", outFD))

		# todo: surround by try/catch
		result = tryCatch( {
			preprocessProfileTextFileParallel(inFile, outF, outFileForDroppedForeign=outFD,
			   appendToOutputFiles=F, compressOutput=F,
			   chunkSize=-1, pruneMoreForeign=T, timeProfiling=T, inParallel=F)
			   # while testing/debugging
			   #chunkSize=1000, stopAfterChunk=1, pruneMoreForeign=T, timeProfiling=T, inParallel=F)
			print(paste("Finished successfully with inFile", inFile))
			   },
			error = function(err) {
				print(paste("Died on inFile", inFile, "with the following error:"))
				print(err)
			}
		)
}	

# Example usage / small tests
if (F) {
	inFile = "/home/kjoseph/profile_study/collection_1_22/100_user_info.txt"
	inFile = "/home/lfriedl/100_noNul-head.txt"	# or a small one

	outF = "/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/data/100_prep.csv"
	outFD = "/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/data/100_foreign.csv.gz"
	preprocessProfileTextFileParallel(inFile, outF, outFileForDroppedForeign=outFD,
				   appendToOutputFiles=F, compressOutput=F,
				   chunkSize=5000, pruneMoreForeign=T, timeProfiling=T, inParallel=F, stopAfterChunk=4, needCleanInfile=F) 
	# or
	outF = "/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/data/100_prepPar.csv"
	outFD = "/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/data/100_foreignPar.csv.gz"
	preprocessProfileTextFileParallel(inFile, outF, outFileForDroppedForeign=outFD,
				   appendToOutputFiles=F, compressOutput=F,
				   chunkSize=5000, pruneMoreForeign=T, timeProfiling=T, inParallel=T, stopAfterChunk=4, needCleanInfile=F) 
	# can also experiment with useFread=F flag, but there's no reason to ever use it

	# test on a whole file
	inFile = "/home/lfriedl/100_noNul.txt"
	outF = "/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/data/100_bigPrep.2.csv"
	outFD = "/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/data/100_bigForeign.2.csv.gz"
	preprocessProfileTextFileParallel(inFile, outF, outFileForDroppedForeign=outFD,
				   appendToOutputFiles=F, compressOutput=F,
				   chunkSize=-1, pruneMoreForeign=T, timeProfiling=T, inParallel=T, needCleanInfile=F) 
}

# One way to run everything: 1 file at a time, parallelized. This turns out not to be the fast way.
if (F) {	# actual processing of everything in a directory
	inDir = "/home/kjoseph/profile_study/collection_20170306"
	outDir = "/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/data_collection_20170306"
	fileList = list.files(path=inDir, pattern="_user_info.txt", no..=T)

	fileCnt = 0
	startAtFile = 23
	for (filePiece in fileList) {
		fileCnt = fileCnt + 1
		if (fileCnt < startAtFile) {
			next
		}
		inFile = file.path(inDir, filePiece)
		f2 = filePiece
		substr(f2, start=nchar(f2) - 2, stop=nchar(f2)) = "csv"
		outF = file.path(outDir, f2)

		substr(f2, start=nchar(f2) - 13, stop=nchar(f2)) = "foreign.csv.gz"
		outFD = file.path(outDir, f2)

		print(paste("At", Sys.time(), "starting to work on file", fileCnt, ":", filePiece))
		preprocessProfileTextFileParallel(inFile, outF, outFileForDroppedForeign=outFD,
					   appendToOutputFiles=F, compressOutput=F,
					   chunkSize=-1, pruneMoreForeign=T, timeProfiling=T, inParallel=T)
		gc()
	}
	print("finished!!!")

}
# notes:
# -error on file 10 : 108_user_info.txt. during putThemTogether, got "Character conversion: Unmappable input sequence / Invalid character. (U_INVALID_CHAR_FOUND)"
# -ditto, file 14: 111_user_info.txt
# -ditto, file 22 : 119_user_info.txt
# -ditto, file 23 : 11_user_info.txt
# -ditto, file 27 : 123_user_info.txt

if (F) {	# a new way to run things
	inDir = "/home/kjoseph/profile_study/collection_20170306"
	outDir = "/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/data_collection_20170306"
	fileList = list.files(path=inDir, pattern="_user_info.txt", no..=T)

	infiles = rev(sort(fileList))		# start at the bottom, so as not to conflict with other version
	# we put the first 100 of the reversed list on achtung04.

	# on achtung03, send the broken ones from before: 14, 22, 23, and 27; then the rest through #130. (100 + 130 = total number of input files)
	infiles = infiles[1:130]
	infiles = infiles[c(14, 22, 23, 27:130)]

	runOnManyFiles(inDir, infiles, outDir)
}
