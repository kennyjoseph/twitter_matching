library(data.table)
library(stringi)
library(dplyr)
library(dtplyr)

USTimeZoneStrings = c("Arizona", "Hawaii", "Alaska", "Indiana (East)",
                      "Eastern Time (US & Canada)", "Central Time (US & Canada)",
                      "Mountain Time (US & Canada)", "Pacific Time (US & Canada)",
                      # Consulted https://en.wikipedia.org/wiki/List_of_tz_database_time_zones for full list. 
                      "America/Chicago", "America/Denver", "America/New_York", "America/Los_Angeles",
                      "America/Boise", "America/Detroit", "America/Anchorage", "America/Phoenix",
                      # Indiana, ugh:
                      "America/Fort_Wayne", "America/Indiana/Indianapolis", "America/Indiana/Knox", "America/Indiana/Marengo",
                      "America/Indiana/Petersburg", "America/Indiana/Tell_City", "America/Indiana/Vevay", "America/Indiana/Vincennes",
                      "America/Indiana/Winamac", "America/Indianapolis", 
                      "America/Juneau", "America/Kentucky/Louisville", "America/Kentucky/Monticello", "America/Knox_IN", "America/Louisville",
                      "America/Menominee", "America/Metlakatla", "America/Nome", "America/Sitka", "America/Yakutat", 
                      "America/North_Dakota/Beulah", "America/North_Dakota/Center", "America/North_Dakota/New_Salem", 
                      "Navajo", "US/Alaska", "US/Aleutian", "US/Arizona", "US/Central", "US/Eastern", "US/East-Indiana", 
                      "US/Hawaii", "US/Indiana-Starke", "US/Michigan", "US/Mountain", "US/Pacific", "US/Pacific-New")




## 207 is a random subset of the larger sample, so its fine to just randomly sample from there ... its also from the april sample but that's fine.
inFile <- "~/git/lazerlab/voter_project/207_user_info.txt"
r = readBin(inFile, raw(), file.info(inFile)$size)
r[r==as.raw(0)] = as.raw(0x20)
tfile = tempfile(fileext=".txt")
writeBin(r, tfile)
rm(r)
inFile = tfile
d2 <- fread(inFile)

setnames(d2, c("id", "name", "handle", "url", "is_protected", "location", "description", "num_followers", "num_following",
              "date_created", "tz_offset", "tz_name", "num_tweets", "profile_lang", "date_last_seen", "coords", "tweet_lang",
              "pic_url", "is_verified"))

d2 <- d2[profile_lang == 'en' & tweet_lang == "en" & (d2$tz_name == "None" | d2$tz_name %in% USTimeZoneStrings)]
set.seed(0)

sample_uids <- sample_n(d2,100000)$id
write.table(sample_uids,
            "~/git/lazerlab/voter_project/twitter_matching/random_sampling_of_accounts/random_sample_usalike_users.txt", 
            row.names=F,col.names=F,quote=F)
