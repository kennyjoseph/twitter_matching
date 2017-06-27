import twitter_url_utils
import ujson as json
import gzip
import unicodecsv as csv
import datetime
import os
import multiprocessing

# minDate and maxDate each can be specified via, e.g., datetime.date(2015,4,30)
def getTweetsFromJSONFile(input_file, output_file, minDate, maxDate):

    with gzip.open(input_file, 'r') as fin, gzip.open(output_file, 'wb') as fout:
        wrtr = csv.DictWriter(fout, twitter_url_utils.URL_FIELDS,
                              delimiter='\t', quotechar="'")
        wrtr.writeheader()

        for line in fin:
            tweet = json.loads(line.decode("utf8"))
            python_tweet_date = datetime.datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S +0000 %Y").date()
            if (minDate and python_tweet_date < minDate) or (maxDate and python_tweet_date > maxDate):
                continue

            map(wrtr.writerow, twitter_url_utils.extract_urls_from_tweet(tweet))

# identical to getTweetsFromJSONFile except meant for appending
def addTweetsFromJSONFile(input_file, output_file, minDate, maxDate):
    with gzip.open(input_file, 'r') as fin, gzip.open(output_file, 'ab') as fout:
        wrtr = csv.DictWriter(fout, twitter_url_utils.URL_FIELDS,
                              delimiter='\t', quotechar="'")
        for line in fin:
            tweet = json.loads(line.decode("utf8"))
            python_tweet_date = datetime.datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S +0000 %Y").date()
            if (minDate and python_tweet_date < minDate) or (maxDate and python_tweet_date > maxDate):
                continue

            map(wrtr.writerow, twitter_url_utils.extract_urls_from_tweet(tweet))


def getTweetsFromManyUsers(indir, output_file, minDate, maxDate):
    infiles = os.listdir(indir)
    infiles = filter(lambda x: x[-8:] == ".json.gz", infiles)  # just in case
    infiles.sort()
    #infiles = infiles[0:20]  # use subset for debugging

    # write header for outfile
    with gzip.open(output_file, 'wb') as fout:
        wrtr = csv.DictWriter(fout, twitter_url_utils.URL_FIELDS,
                              delimiter='\t', quotechar="'")
        wrtr.writeheader()


    for infile in infiles:
        #print 'looking for tweets in ' + infile
        full_infile = indir + "/" + infile
        addTweetsFromJSONFile(full_infile, output_file, minDate, maxDate)

def getDailyTweetsForPanel(panel_json_dir, out_dir, minDate, maxDate, multiprocess=False):
    # Construct list of dates -- we need 1 outfile per day
    if (minDate > maxDate):
        print 'Error: minDate must precede maxDate'
        return
    numDaysBetween = (maxDate - minDate).days

    allDates = []
    allOutfiles = []
    for i in range(0, numDaysBetween + 1):
        oneDay = datetime.timedelta(days=1)
        allDates.append(minDate + i * oneDay)
        allOutfiles.append(out_dir + '/' + str(minDate + i * oneDay) + '.tsv.gz')

    if multiprocess:
        pool = multiprocessing.Pool(processes=10)
        r = [pool.apply_async(getTweetsFromManyUsers, [panel_json_dir,
                                                       allOutfiles[i], allDates[i], allDates[i]])
             for i in range(len(allDates))]
        pool.close()
        pool.join()

    else:
        for i in range(len(allDates)):
            getTweetsFromManyUsers(panel_json_dir, allOutfiles[i], allDates[i], allDates[i])


# tests
#getTweetsFromJSONFile('tweetSample.json.gz', 'testOut.tsv.gz', datetime.date(2016, 5, 1), datetime.date(2016, 12, 31))
#getTweetsFromJSONFile('tweetSample.json.gz', 'testOut2.tsv.gz', datetime.date(2016, 12, 1))
#getTweetsFromManyUsers('/net/data/twitter-voters/tweets/dnc', 'testOutMany.tsv.gz', datetime.date(2016, 12, 1), datetime.date(2016, 12, 1))
#getDailyTweetsForPanel('/net/data/twitter-voters/tweets/dnc', 'testDates2', datetime.date(2016, 12, 1), datetime.date(2016, 12, 5), multiprocess=True)


# now for real
getDailyTweetsForPanel('/net/data/twitter-voters/tweets/public', '/net/data/twitter-voters/tweets-extracted/public-daily', datetime.date(2016, 8, 1), datetime.date(2016, 11, 30), multiprocess=True)