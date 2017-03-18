## to get in:
%  mysql twitterusers -u twitterusers -h achtung-db -p --local-infile=1 
(it asks for password, which is same as username and database name)
(the flag local-infile is needed if we want to use "load data local infile" during the session)
# reminder: if need to write to an outfile, use mysql -e

#  increase tmp table size, so it doesn't spend eons writing stuff to disk 
SET tmp_table_size = 1024 * 1024 * 64; # == 64M, as opposed to the observed default of 16M
SET myisam_sort_buffer_size = 256*1024*1024; # as opposed to the observed default of 8M. This value comes from MySQL docs example.
# to check them: SHOW VARIABLES LIKE 'tmp_table_size'
warnings;	# to print warnings to the screen as they occur; "nowarning" to restore default mode.


# Keeping all the fields in the data file this time.
# (Twitter limits handles to 15 chars, names to 20. Setting fields a bit wider just in case.)
create table if not exists profiles2017v2_1 (id bigint primary key, name varchar(25), handle varchar(20), 
		locationString varchar(200), descriptionString varchar(400), urlString varchar(150),
		acctCreationDate date NOT NULL, lastSeenDate date default NULL, 
		numFollowers int, numFollowing int, numTweets int, isProtected boolean, isVerified boolean,
		timeZoneOffset int default NULL, timeZoneName varchar(30), profileLang varchar(10), tweetLang varchar(10), 
		geoCoords varchar(30), nameHandleWords varchar(200) )
        engine=MyISAM charset='utf8mb4';

create table if not exists profiles2017v2_2 like profiles2017v2_1;
create table if not exists profiles2017v2_3 like profiles2017v2_1;
create table if not exists profiles2017v2_4 like profiles2017v2_1;

# In previous version of data, breaking profiles into 2 tables (neither much over 100 million rows) made querying much faster. Sticking with that here out of caution.
# (To do sometime: test more thoroughly whether it's faster to query these 4 small tables, 2 bigger ones, or 1 massive one)
# profiles2017v2_1 gets files 0, 1, and starting with 10-14.		--> 58778006 records, 20 min to load
# profiles2017v2_2 gets files starting with 15-19.			--> 56230484 records, 20 min
# profiles2017v2_3 gets files starting with 3-9.			--> 79707029 records, 28 min; 27 warnings of duplicate keys (same as below; from files 71 & 77 overlapping)
# profiles2017v2_4 gets files starting with 2.				--> 42249670 records, 15 min; 3 warnings of truncated fields (name; profileLang x 2)
# Total here: 236965189 records (~237M). That's 8.5% smaller than the 258953421 (~259M) we'd have without that "grep" filter below.


# to read data, can set up a named pipe in unix:
# mkfifo namedPipe
## tail -q -n +2 *.csv > namedPipe &						# Important to use "tail -q", else it adds separator/header junk between files.
# tail -q -n +2 *.csv | grep -v ',"[a-z]*"$' > namedPipe &			# Ignore profiles that have only 0 or 1 name words. [^ ] also works in place of [a-z].
# load data local infile '..../namedPipe'

load data local infile '/home/lfriedl/twitter_matching/twitter_profiles_DB_prep/namedPipe'
	into table profiles2017v2_1 charset 'utf8mb4' 
	fields terminated by ',' optionally enclosed by '"' escaped by ''   # ignore 1 lines [when there's a header]
	(id, name, handle, locationString, descriptionString, urlString, acctCreationDate, @lastSeenDateIn,
	numFollowers, numFollowing, numTweets, @isProtectedIn, @isVerifiedIn, 
	@timeZoneOffsetIn, timeZoneName, profileLang, tweetLang, geoCoords, nameHandleWords)
	set lastSeenDate = nullif(@lastSeenDateIn, ''), timeZoneOffset = nullif(@timeZoneOffsetIn, ''),
	    isProtected = if(@isProtectedIn = 'TRUE', 1, 0), isVerified = if(@isVerifiedIn = 'TRUE', 1, 0);

	# @ means "ignore this field" -- or rather, temporarily store it in case there's further processing
# In the CSV files: some strings are quoted, others not; ditto for empty strings; bools are TRUE/FALSE.
# Defaults: if I understand right, usually empty --> NULL. However, that's only relevant when you're manually insert other fields into a row.
# Here, since we send in an empty string, it treats it like an empty string. Need to manually change to NULL if we want that value.
# For text fields, doesn't matter whether we store "" or NULL. But for numbers (lastSeenDate and timeZoneOffset), need to maintain distinction between 0 and NULL, so handle them specially.
# Booleans apparently don't recognize TRUE/FALSE while loading, so convert them too.


# General advice is always to create indices after loading data.
create fulltext index idx_indexWords on profiles2017v2_1 (nameHandleWords);
create fulltext index idx_indexWords on profiles2017v2_2 (nameHandleWords);	
create fulltext index idx_indexWords on profiles2017v2_3 (nameHandleWords);	
create fulltext index idx_indexWords on profiles2017v2_4 (nameHandleWords);	


# The magic call that makes queries fast!
# (Loads the indexes from these tables into memory, so indexed lookups don't even need to access the table on disk.)
LOAD INDEX INTO CACHE profiles2017v2_1, profiles2017v2_2, profiles2017v2_3, profiles2017v2_4;

# Warnings while loading

In profiles2017v2_1:
| Warning | 1265 | Data truncated for column 'locationString' at row 5004074     |
| Warning | 1265 | Data truncated for column 'descriptionString' at row 12917090 |

In profiles2017v2_3:
Warning (Code 1062): Duplicate entry '3198364375' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '3198364377' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '2855224084' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '379556540' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '83455207' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '3522834137' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '1574849634' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '83455209' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '534186572' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '280064535' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '3092267014' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '3092267013' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '2827173973' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '2869393878' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '2507417609' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '636347205' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '308391122' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '4462734634' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '2493531823' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '2493531822' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '2866015496' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '2768556049' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '3954455535' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '749117955145740288' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '703293885984821248' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '540550179' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '540550176' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '540550175' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '435672365' for key 'PRIMARY'
Warning (Code 1062): Duplicate entry '1522647295' for key 'PRIMARY'

