
Scripts for sending TargetSmart data through the matching pipeline:

- `../voter_file_aggregation/code/generate_targetsmart_csvs.py` takes raw TargetSmart data file (one per state), select and cleans columns to produce files we keep in `ts_cleaned`.
- `../voter_file_aggregation/code/generate_extra_files.py` takes the complete set of `ts_cleaned` files and creates, for every state of residence, a file under `extra_state_files` for voters who live here but were registered elsewhere. (Files produced so far have correct formatting, incorrect counts.)
- ```prepCountInputs.R```: takes all voters (and non-voters) from 1 state, recomputes counts, subdivides into "chunk" files.
- [Optional] Run ```generate_preferred_files.R``` to generate a sub-sampled collection for matching from the TS data
- ```runTS.R```: To run parts 1 and 2 of matching pipeline, in separate calls, for each input file.
	- part 1: for a single "chunk" file, match against the SQL Twitter universe, and then parse/infer locations for candidate matches
	- part 2: train and apply a classifier for P(US loc | profile metadata); use "matching rules" to change candidate matches into actual matches.
- Then see `../managing_matches/dedupFinalizePanelMembs.R` to deduplicate and join against the voter files.
