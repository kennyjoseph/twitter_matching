`panelDefns.R`: Stores meta-data associated with each version of the panel.

`dedupFinalizePanelMembs.R`: Creates files listing panel members and their IDs. 

This is non-trivial because (a) we have to be smart about handling Twitter IDs that match more than one voter, and (b) the voter data files had different properties (such as format of IDs).

* `createDedupedUniverse(panelNames, outDirAndPrefix)` -- run this to create a master list of all people and all panels they're on. Should include as many panel names as we might potentially care about. **N.B. When the universe changes, the composition of any given panel will also change.** (Due to handling of duplicates.)
	0. Loads all match files (`config$sourceMatchFilesWithDups`) for the list of panels, to get Twitter + voter IDs.
	1. Checks for any voter IDs that occur twice. (If any, warns and drops them.)
	2. Flags any Twitter IDs that occur twice.
	3. Merges in demographic data (`config$matchingInputFiles`) for each panel from the input voter file. 
	4. Among Twitter IDs that occur twice, identify clusters of the "same voter" -- voters must be from different data sources but have matching first name, last name, zip code and birth year. Mark these records as ok to keep, and save the voter IDs for the cluster.
	5. Save master list of all potential matches with annotations.
	<br>--> *prefix + "masterList.csv"*
	6. Save matches we can use, with their "common input format" demographic data. 
	<br>--> *prefix + "inputFormat.csv"*
	6. For each individual panel, save (a) file of IDs and (b) file of "raw" demographic data.
	<br>--> *prefix + "-rawFormat.csv"*
	and *prefix + ".twIDs.txt"*	

* `getIDsForPanels(universePath, panelNames)` -- given a universe and a list of panels we care about right now, this function returns triplets of the form (Twitter ID, voter ID, panel name). It applies hard-coded precedence rules so that each person only gets assigned to 1 panel in the list. Can be joined against existing files *"inputFormat.csv"* or *"-rawFormat.csv"*
