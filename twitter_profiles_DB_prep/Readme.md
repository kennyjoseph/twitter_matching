
Scripts that take as input a directory containing Twitter profile info (in tab-delimited files). We clean and prepare 
them for putting into SQL, providing two actions:
* Do a preliminary screen for foreign accounts, which we don't even need to put into the DB. These are instead shoved into 
secondary files, with the lines essentially untouched.
* Pull apart the name and handle fields to create a new column "nameHandleWords" which can be indexed for searching.
(Doing this because SQL's string matching is very slow, but this way we can create a "fulltext" index (on words) that's faster.)

Files:
* cleanProfilesIndexNames.R holds the main functionality. Runs on 1 input file, optionally parallelized.
* prepSeveralFiles.R calls it for many files.
* nameChunking.R is the nitty gritty of inferring "name words". It's slow and messy, but that's because it tries to cover so many cases.


