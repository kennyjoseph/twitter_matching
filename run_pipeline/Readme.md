# Data directory structure
 
	All is on achtung under /net/data/twitter-voters:
       /voter-data
          /targetsmart    # raw data from (e.g.) TargetSmart.
          		          # (Don't move it -- location is recorded in panelDefns.R)
          /input_to_pipeline
             /ts_cleaned     # preprocessed (by generate_targetsmart_csvs.py)
             				  # (ok to delete later.)
             /extra_state_files  # people who live in a different state than registered
                             # (ok to delete later.)
             /ts_chunks      # split into files of < 3 million people. (Input to runTS.R)
          		             # (Don't move it -- location is recorded in panelDefns.R, used as "input format" for panel universes)
       /matching-work-files
          /cand-matches   # (Created during runTS.R, part 1)
             /one-subdir-per-input-file
          /with-locations # (Output of runTS.R, part 1)
             /one-subdir-per-input-file      # (eventually deleted)
             /handful of files per input file 
       /match-results          # (Output of runTS.R, part 2)
          /handful of files per input file  --> manually moved into subdirs (locations recorded in panelDefns.R)




# Scripts for sending TargetSmart data through the matching pipeline:

TLDR: a wrapper `run_pipeline/allCommands.R` now has code to do everything up through (and including) runTS.R. 

- `../voter_file_aggregation/code/generate_targetsmart_csvs.py` takes raw TargetSmart data file (one per state), select and cleans columns to produce files we keep in `ts_cleaned`.
- `../voter_file_aggregation/code/generate_extra_files.py` takes the complete set of `ts_cleaned` files and creates, for every state of residence, a file under `extra_state_files` for voters who live here but were registered elsewhere. (Files produced so far have correct formatting, incorrect counts.)
- ```run_pipeline/prepCountInputs.R```: takes all voters (and non-voters) from 1 state, recomputes counts, subdivides into "chunk" files.
- [Optional] Run ```run_pipeline/generate_preferred_files.R``` to generate a sub-sampled collection for matching from the TS data
- ```run_pipeline/runTS.R```: To run parts 1 and 2 of matching pipeline, in separate calls, for each input file.
	- part 1: for a single "chunk" file, match against the SQL Twitter universe, and then parse/infer locations for candidate matches
	- part 2: train and apply a classifier for P(US loc | profile metadata); use "matching rules" to change candidate matches into actual matches.			
- Then see `../managing_matches/dedupFinalizePanelMembs.R` to deduplicate and join against the voter files. Before this step, need to make an entry in `managing_matches/panelDefns.R`.

		
# Setup for new voter data (e.g., where are locations configured?)
- The wrapper script `allCommands.R` manages paths during matching.
	- Choose a location for the raw data and for the chunks.
- Before deduping finished matches, need to make an entry for the dataset in `managing_matches/panelDefns.R`. Each entry stores meta-data for a version of the panel, including paths for all data we're going to keep around.

# Data maintenance
- We should make sure to have backed up: 
	1. /net/data/twitter-voters/voter-data/[every raw directory]
	1. /net/data/twitter-voters/match-results
	1. /net/data/twitter-voters/panels
- We should make sure to compress:
	1. /net/data/twitter-voters/voter-data/[every raw directory]
	1.  /net/data/twitter-voters/voter-data/[every "chunks" directory]
	1. Anything else we choose not to delete
- We should go ahead and delete:
	- After scripts ran successfully:
		1. /net/data/twitter-voters/voter-data/input_to_pipeline/ts_cleaned
		1. /net/data/twitter-voters/voter-data/input_to_pipeline/extra_state_files
		1. /net/data/twitter-voters/matching-work-files/cand-matches/out-*.txt
		1. /net/data/twitter-voters/matching-work-files/with-locations/*/allCandidateMatchLocs.csv
	- If no reason to track matching outcomes for each voter:
		- /net/data/twitter-voters/matching-work-files/cand-matches/*
		- /net/data/twitter-voters/matching-work-files/*
		- Else if keep these around, should move to a subdir for the data set, and also compress.

