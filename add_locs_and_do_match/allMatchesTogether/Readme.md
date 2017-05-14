
This directory contains panel match data for around 475k people.

sourceMatchFiles: for each voter file, the matches we'll use. (10 public states, dnc:IANH, dnc:natl2M (in 2 formats), dnc:100k (in old format only).)
	These were all run with the flag to not deduplicate within files.
	All matching is done first at the city level (with all names unique in their city), then the state (restricted to names unique in their state).

simpleUnion: simply grabs all matches seen without doing any deduplication, produces list of Twitter IDs.

top level directory: the result of (somewhat ugly, somewhat manual) deduplication.

	allMatches-skinny.csv: all 475016 "good" matches, only 11 columns
	allMatches-wide1.csv: most of the matches (474432), all 31 columns
	allMatches-wide2.csv: the rest of the matches (584), 97 columns


	Matches that we dropped because more than one voter was matched to the same Twitter account.
	dupsInStates.csv: dups seen in the state-specific files
	dupsInNatl.csv: dups seen in the natl-level files 
	(prev two also include matches to Twitter accts seen duplicated but not in this file)
	dupsAcrossStateNatl.csv: dups seen between state-specific and natl2M  
	dupsAcrossStateNatl2.csv: dups seen between state-specific and natl100k  

	Matches that we retained because the dup was between a state-specific and a national file, 
	and it seemed to be the same voter. The duplicated/dropped record is saved in this file, the other is 
	preserved in allMatches-wide1.csv.
	dupsProbablyOkSameVoter.csv: state-specific matched natl2M
	dupsProbablyOkSameVoter2.csv: state-specific matched natl100k

	aggregateDedupAllMatches.R: the code 


