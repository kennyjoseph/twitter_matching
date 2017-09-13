
Scripts for sending TargetSmart data through the matching pipeline:

- [things KJ did] changes raw TargetSmart data files (one per state) into ts_cleaned + extra_state_files (which have correct formatting, incorrect counts)
- ```prepCountInputs.R```: takes all voters (and non-voters) from 1 state, recomputes counts, subdivides into "chunk" files.
- [Optional] Run ```generate_preferred_files.R``` to generate a sub-sampled collection for matching from the TS data
- ```runTS.R```: To run parts 1 and 2 of matching pipeline, in separate calls, for each input file.
	- part 1: for a single "chunk" file, match against the SQL Twitter universe, and then parse/infer locations for candidate matches
	- part 2: train and apply a classifier for P(US loc | profile metadata); use "matching rules" to change candidate matches into actual matches.
- Then see `../managing_matches/dedupFinalizePanelMembs.R` to deduplicate and join against the voter files.
