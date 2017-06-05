from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from twitter_dm import Tweet
import requests
import sys

consumer_key = "igIJ02FRshlZehZFRsuGyo9Pz"
consumer_secret = "cPGBzSaFGwH0gFlrUtSMEVbVkfu2HDGgzxECY07lfiKumatYa7"
access_token = "2798401248-htvoFRv6oqBMefHQ9CR3kF5w6XR2ZOxEGWkNoCV"
access_token_secret = "hFFvg9pM5JRnixjj7h0yqOC4GXj223mIywA0lwyoFrAcd"

user_set = set()
SAMPLE_SIZE = 100000
class BaseTweepyListener(StreamListener):
    def __init__(self):
        super(StreamListener, self).__init__()

    def on_data(self, data):
        try:
            t = Tweet(data, do_tokenize=False)

            if t.lang != 'en':
                return
            user_set.add(t.user.id)
        except:
            return True

        if len(user_set) % 1000 == 0:
            print(len(user_set))
        if len(user_set) == SAMPLE_SIZE:
            of = open("random_keyword_sample_from_stream_5_1_17.txt","w")
            for u in user_set:
                of.write(str(u)+"\n")
            of.close()
            sys.exit(-1)

        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = BaseTweepyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        try:
            stream = Stream(auth, l)
            stream.filter(track=['clinton','trump','@realDonaldTrump','@HillaryClinton',
                                 'Hillary','#maga','#imwither','donald','#election2016'])
        except requests.packages.urllib3.exceptions.ProtocolError:
            pass
        except requests.packages.urllib3.exceptions.ReadTimeoutError:
            pass
