
Three scripts for sending TargetSmart data through the matching pipeline (of which only 2 exist so far):

-prepCountInputs.R: takes all voters (and non-voters) from 1 state, recomputes counts, creates "chunk" files.
-runTS.R: To run parts 1 and 2 of matching pipeline, in separate calls, for each input file.
-[note: will need to modify do_the_matching so that it postpones joining the matches to the voter files]
-[something to deduplicate all the matches found, then join against (all!) voter files]
	(Depending on the number of matches we're working with, can probably do it all in memory, grabbing
	the right section of code from aggregateDedupAllMatches.R)
