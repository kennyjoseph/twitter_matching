Code that takes candidate matches (files), computes inferredLoc from the Twitter location field, computes probMetaForeign from the Twitter profile info,
and performs the matching.

Files:
addLocation.R -- call this to compute locations (parallelized) on the candidate match files, then combine them all into 1.
processLocations.R -- isLocationInUSA() code called by addLocation.R.	
randomForest.R -- trains a classifier (right now, a party::ctree()) to predict from profile meta-data whether the inferredLoc will be foreign vs. U.S.
do_the_matching.R -- takes candidate matches and outputs definite matches. (This version of the wrapper code hasn't yet been tested & run, but the helper functions have.)
	Currently uses inferredLoc, but not equipped to reason about probMetaForeign or geo data.
analyses/ -- notebook-style work looking at (a) low-hanging near-matches to ask for geo data on, (b) how matching results differ across iterations. Also (c) plots of distributions of probMetaForeign 
	in the labeled, unlabeled rows of candidate matches.
