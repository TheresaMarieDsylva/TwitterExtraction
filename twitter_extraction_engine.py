import json
import time
import re
from pymongo import MongoClient
import tweepy
import config


def make_connection():
    client = MongoClient(
        "mongodb+srv://Mariedb-5408:Flz2ID3QazX2AqIN@cluster0.jhy2v.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client.get_database('myMongoTweet')
    return db


def insert_record(jsonfile):
    db = make_connection();
    records = db.tweet
    json_file = json.loads(jsonfile)
    records.insert_one(json_file)
    print(records.count_documents({}))


def clean_tweets(dictObj):
    keep = []
    meta = []
    entity = []
    if 'created_at' in dictObj:
        val = json.loads(dictObj)

        text = val['text']
        # print(val['id'])
        after = deEmoji(deSpecialChar(deUrl(deSpecial(text)))).replace("'", "")
        # print(after)
        val['text'] = after
        keepers_list = ['created_at', 'id', 'text', 'user',
                        'entities', 'timestamp_ms']

        meta_list = ['id', 'name', 'location', 'description', 'followers_count', 'friends_count', 'listed_count',
                     'favourites_count', 'statuses_count', 'created_at', 'time_zone', 'lang']

        entity_list = ['hashtags', 'user_mentions']

        keep = {
            key: val[key]
            for key in val.keys()
            if key in keepers_list
        }

        meta = {
            key: keep['user'][key]
            for key in keep['user'].keys()
            if key in meta_list
        }

        entity = {
            key: keep['entities'][key]
            for key in keep['entities'].keys()
            if key in entity_list
        }

        keep['user'] = meta
        keep['entities'] = entity
    return keep


def deUrl(sample):
    return re.sub(r'http\S+', '', sample)


def deSpecial(sample):
    return re.sub(r'@\S+', '', sample)


def deSpecialChar(sample):
    return re.sub('[^A-Za-z0-9\t\r\f]+', ' ', sample)


def deEmoji(sample):
    regex_pattern = re.compile(pattern="["
                                       u"\U0001F600-\U0001F64F"
                                       u"\U0001F300-\U0001F5FF"
                                       u"\U0001F680-\U0001F6FF"
                                       u"\U0001F1E0-\U0001F1FF"
                                       "]+", flags=re.UNICODE)
    return regex_pattern.sub(r'', sample)


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self, time_limit=3000):
        super().__init__()
        self.start_time = time.time()
        self.limit = time_limit
        self.counter = 1
        self.test = []
        self.fileSize = 300
        self.tweetSize = 30
        self.cleaned = ""

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            # print(json.dumps(data))
            self.test.append(clean_tweets(data))
            if len(self.test) > self.fileSize:
                self.test = []
                self.counter += 1
                print("file full")

            if self.counter <= self.tweetSize:
                with open("tweet_" + str(self.counter) + ".json", 'w') as uf:
                    uf.write(json.dumps(self.test))
            return True
        else:
            return False


if __name__ == '__main__':
    listener = TwitterStreamListener()
    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_secret_token)

    stream = tweepy.Stream(auth, listener)
    stream.filter(track=['cold', 'flu', 'snow'])
    word_counter = ['flu', 'snow', 'emergency']
