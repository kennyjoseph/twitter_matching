
# Wrapper for scripts in run_pipeline: pre-processes Targetsmart voter data, then queries voters against database, infers locations, and writes matches.
# Recommended way to run this: within a "screen" session, start R, and copy and paste these commands to the prompt.

rawDir         = "/net/data/twitter-voters/voter-data/targetsmart_oct2017"         # example filename: tsmart_northeastern_install_file_ak.zip
cleanedDir     = "/net/data/twitter-voters/voter-data/input_to_pipeline/ts_cleaning"   # example filename: tsmart_northeastern_install_file_ak.tsv
extraStateDir  = "/net/data/twitter-voters/voter-data/input_to_pipeline/extra_state_files2"       # example filename: ak_extra.tsv
chunkDir       = "/net/data/twitter-voters/voter-data/ts_oct2017_chunks"       # example filename: ak_chunk1.tsv
# Directories need not exist already. (Safer anyway to create new dirs than to reuse the old ones.)

repoBaseDir = "/home/lfriedl/twitter_matching"

# 1. raw --> cleaned: generate_targetsmart_csvs.py
# lfriedl@achtung04:~/twitter_matching/run_pipeline$ python generate_targetsmart_csvs.py ~/voter-stuff/voter-data/targetsmart_oct2017 ~/voter-stuff/voter-data/input_to_pipeline/ts_cleaning ZIP
# Notes: 
# -Can be parallelized by editing the code to set the NUM_CPUS variable to >1 (e.g., 4). However, this caused some memory errors.
# -If script fails and is restarted, it picks up where it left off.
# -Runtime: ~1 day
print("wrapper: starting generate_targetsmart_csvs.py")
system2("python", args=c(file.path(repoBaseDir, "voter_file_aggregation/code/generate_targetsmart_csvs.py"), 
			rawDir, cleanedDir, "ZIP"))

# 2. cleaned --> extra: generate_extra_files.py
# Runtime: ~1 hour
print("wrapper: starting generate_extra_files.py")
system2("python", args=c(file.path(repoBaseDir, "voter_file_aggregation/code/generate_extra_files.py"), 
			cleanedDir, extraStateDir))

# 3. cleaned + extra --> chunks: prepCountInputs.R
# Runtime: ~1 hour
print("wrapper: starting prepCountInputs.R")
source(file.path(repoBaseDir, "run_pipeline/prepCountInputs.R"))
numChunkFiles = 0
for (i in 1:51) {
	numChunkFiles = numChunkFiles + run_command_line_call(i, cleanedDir, extraStateDir, chunkDir)
}
print(paste("wrapper: done with chunks, got", numChunkFiles))

# 4. chunks --> cand-matches: runTS.R part 1
print("wrapper: starting runTS.R part 1")
source(file.path(repoBaseDir, "run_pipeline/runTS.R"))

# update some global vars it set
voterfileDir = chunkDir
numVoterfilesWeKnowAbout = numChunkFiles
scriptsBaseDir = repoBaseDir
placeListDir = file.path(scriptsBaseDir, "add_locs_and_do_match/data/placeLists/")
# ok to leave hard-coded candMatchesBaseDir, locsBaseDir, matchResultsBaseDir

sanity_check()   # see if we have the number of chunk files expected

# Runtime: ~6 hours per chunk
for (i in 1:numChunkFiles) {
	run_part1(i)
}

# 5. cand-matches --> match-results: runTS.R part 2
print("wrapper: starting runTS.R part 2")

# Runtime: ~20 min per chunk
for (i in 1:numChunkFiles) {
	run_part2(i)
}
