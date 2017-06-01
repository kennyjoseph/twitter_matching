library(data.table)

# To run: 
usage = "Usage: Rscript [--vanilla] prepCountInputs.R <fileNum>"
# where fileNum corresponds to the 50 states
args = commandArgs(T)   # everything after executable name

# This script bundles data from ts_cleaned + extra_state_files into input files for a state: 
# -rbind raw-ish and extra state files for a state
# -redo counts
# -split into files of 2 million lines each (if remainder < 1M, append to previous)

# Directory structure: 
# /net/data/twitter-voters
#       /voter-data
#               /targetsmart    # raw data from TargetSmart
#               /ts_cleaned     # preprocessed
#               /ts_chunks      # split into files of < 3 million people. 
#       /matching-work-files
#               /cand-matches   # 
#                       /one-subdir-per-input-file
#               /with-locations # 
#                       /one-subdir-per-input-file      # (eventually deleted)
#                       /handful of files per input file 
#       /match-results          # 
#                       /handful of files per input file 

# Initialize vars
rawStateDir 		= "/net/data/twitter-voters/voter-data/ts_cleaned"	# example filename: tsmart_northeastern_install_file_AK.tsv
extraStateDir 		= "/net/data/twitter-voters/voter-data/extra_state_files"	# example filename: AK_extra.tsv
voterfileDir            = "/net/data/twitter-voters/voter-data/ts_chunks"	# for output

# example rawState filename: tsmart_northeastern_install_file_AK.tsv
# example extraState filename: AK_extra.tsv

run_command_line_call = function(args, linesPerFile = 2000000, maxLinesPerFile = 3000000) {
        if (length(args) != 1) {
                stop("Expected exactly 1 arg\n", usage)
        }
        fileNum = as.integer(args[1])
	allStateFiles = list.files(rawStateDir, pattern="\\.tsv$")
	rawStateFileStem = allStateFiles[fileNum]
	rawStateFile = file.path(rawStateDir, rawStateFileStem)
	stateAbbrev = substr(rawStateFileStem, nchar(rawStateFileStem) - 5, nchar(rawStateFileStem) - 4)
	extraStateFile = file.path(extraStateDir, paste0(stateAbbrev, "_extra.tsv"))
	if (!file.exists(extraStateFile)) {
		stop("Didn't find extra state file")
	}

	data1 = fread(rawStateFile)
	data2 = fread(extraStateFile, header=FALSE)
	allData = rbind(data1, data2, use.names=FALSE)
	print(paste(stateAbbrev, ": found", nrow(allData), "rows"))
	
	withCntZip = allData[, .(zipcode_cnt = .N), by=.(first_name, last_name, zipcode, state)]
	withCntCity = allData[, .(city_cnt = .N), by=.(first_name, last_name, city, state)]
	withCntCounty = allData[, .(county_cnt = .N), by=.(first_name, last_name, county, state)]
	withCntState = allData[, .(state_cnt = .N), by=.(first_name, last_name, state)]

	allDataWith1 = merge(allData, withCntZip)
	# must specify "by" b/c default for data.table is shared "key" columns only
	allDataWith2 = merge(allDataWith1, withCntCity, by=intersect(colnames(allDataWith1), colnames(withCntCity)))
	allDataWith3 = merge(allDataWith2, withCntCounty, by=intersect(colnames(allDataWith2), colnames(withCntCounty)))
	allDataWith4 = merge(allDataWith3, withCntState, by=intersect(colnames(allDataWith3), colnames(withCntState)))
	# (manually checked that counts are reasonable compared to original ones. now drop originals.)

	origColnamesWanted = setdiff(colnames(allData), c("zipcode_count", "city_count", "county_count", "state_count")) 
	# notice: allData's order is how we want to write it out
	cleanCnts = allDataWith4[, .(zipcode_count = zipcode_cnt, city_count = city_cnt, county_count = county_cnt, state_count = state_cnt,
					zipcode_cnt = NULL, city_cnt = NULL, county_cnt = NULL, state_cnt = NULL)]
	allDataClean = cbind(allDataWith4[, origColnamesWanted, with=F], cleanCnts)
	# <-- looks just like input files, except:
	# 1. Counts updated
	# 2. Count columns reordered to be more reasonable

	# Write it out!
	# Into how many files? Generally use linesPerFile lines, but the last one can be up to maxLinesPerFile.
	linesWritten = 0
	fileCnt = 0
	while (linesWritten < nrow(allDataClean)) {
		# write a file
		fileCnt = fileCnt + 1

		outfile = file.path(voterfileDir, paste0(stateAbbrev, "_chunk", fileCnt, ".tsv"))
		startLine = linesWritten + 1

		# if the remainder is small, push it into this file too
		if (nrow(allDataClean) - linesWritten <= maxLinesPerFile) {
			stopLine = nrow(allDataClean)
		} else {
			stopLine = linesWritten + linesPerFile 
		}

		fwrite(allDataClean[c(startLine:stopLine),], file=outfile, sep="\t")
		print(paste("Wrote", outfile))
		linesWritten = stopLine
	}


}


# Actually do the call down here
run_command_line_call(args)

