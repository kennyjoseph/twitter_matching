# twitter_matching

Pipeline for matching voters to Twitter accounts:
* From Twitter profile data, creates SQL database. 
	* `twitter_profiles_DB_prep/`: Removes foreign accounts and extracts "name words" for indexing by DB.
	* `mySQL_load_retrieve/`: loads data into SQL.
* From voter data, queries DB to find Twitter profiles with matching names, infers their locations, and writes out matches. Scripts in `run_pipeline/` call:
	* `mySQL_load_retrieve/`: Iterate through voter data, writing out possible matches.
	* `add_locs_and_do_match/`: Infer locations, determine matches we are confident about.
* `managing_matches/`: From those matches (and previous runs, possibly from multiple sources of voter data), handles duplicates, and writes out a single "universe" of matches, which contains one or more "panels." (Panel = result of running the pipeline once. Universe = collection of panels; each match can be in more than one panel.)

Also:
* `voter_file_aggregation/`: downloads and preprocesses publicly accessible voter data 
* `random_sampling_of_accounts/`: samples random Twitter accounts and collects basic profile info
