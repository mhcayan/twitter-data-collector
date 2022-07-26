import json
import pandas as pd
import os
import re
from urllib.parse import urlparse
import csv
import validators #for url validation
from Tweepy_Wrapper import Tweepy_Wrapper
import time
from random import randint


PROJECT_PATH = r"F:\E\code\twitter-data-collector"
RESOURES_PATH = os.path.join(PROJECT_PATH, "resources")
USER_INFO_FILE_NAME = "user_info.csv"
ALL_TWEETS_FILE_NAME = "all_tweets.csv"
USER_FOLLOWERS_FILE_NAME = "user_follower_ids.csv"
USER_FOLLOWINGS_FILE_NAME = "user_following_ids.csv"
INPUT_FILE_NAME = "sm_org_17_18EBR2010ct_031021.dta"
SEARCH_RESULT_FILE_NAME = "user_search_result.csv"

#read all keys from json file
with open("keys.json") as file:
    keysjson = json.load(file)

api_key = keysjson["api_key"]
api_key_secret = keysjson["api_key_secret"]
bearer_token = keysjson["bearer_token"]
access_token = keysjson["access_token"]
access_token_secret = keysjson["access_token_secret"]

#authentication and authrization of the api
import tweepy
auth = tweepy.OAuth1UserHandler(consumer_key = api_key, consumer_secret = api_key_secret, 
    access_token= access_token, access_token_secret= access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

def resetConnection():
    auth = tweepy.OAuth1UserHandler(consumer_key = api_key, consumer_secret = api_key_secret, 
        access_token= access_token, access_token_secret= access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

api = tweepy.API(auth, wait_on_rate_limit=True)

def isUrlValid(url):
    return True if validators.url(url) else False

#get user screen_name from twitter url
def get_screen_name_from_url(url):
    return re.sub('/', '', urlparse(url).path)

#return list of twitter (ein, screen_name)
def get_user_screen_names(input_file):
    df = pd.read_stata(input_file)
    ein_column_name = "ein"
    twitter_column_name = "Twitter"
    #read only the ein and twitter url. remove rows with invalid or null url
    df = df[df[twitter_column_name].apply(isUrlValid)][[ein_column_name, twitter_column_name]]
    #get screen name from url and create a list of tuples (ein, screen_name)
    return [(ein, get_screen_name_from_url(url)) for ein, url in df.itertuples(index = False)]

#convert tweepy object to dictionary
def jsonify_tweepy(tweepy_object):

    json_str = json.dumps(tweepy_object._json, indent=2)
    return json.loads(json_str)

def write_data_to_file(output_file_name, tweepy_object):
    with open(output_file_name, 'a') as f:
        json.dump(tweepy_object._json, f, indent=4, sort_keys=True)

def write_df_to_csv(file_path, df, index = False):
    df.to_csv(file_path, index = index, encoding = 'utf-8')

def generate_twitter_user_info_file(screen_name_list, output_file):

    user_attr_list = ["ein", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"]

    with open(output_file, "w", newline = '', encoding = "utf-8") as file:
        w = csv.writer(file)
        w.writerow(user_attr_list)
        for ein, screen_name in screen_name_list:
            try:
                user = api.get_user(screen_name = screen_name)
                w.writerow(
                    [
                        ein,
                        user.id,
                        user.name,
                        user.created_at,
                        user.description.replace('\n', ' ').encode('utf-8'),
                        user.favourites_count,
                        user.friends_count,
                        user.followers_count,
                        user.listed_count,
                        user.location.replace('\n', ' ').encode('utf-8'),
                        user.screen_name,
                        user.statuses_count,
                        user.time_zone,
                        user.verified
                    ]
                )
            except Exception as e:
                print("Exception of type " + str(type(e)) + " details: ein = " + str(ein) + " screen_name = " + str(screen_name))


def get_user_tweets(output_csv_file, max_tweet_count, screen_name_list):

    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        
        w = csv.writer(file)
        w.writerow(["ein", "tweet_id", "full_text", "created_at", "retweeted", "favorite_count", "retweet_count", 
                "user_id", "screen_name", "hashtags", "lang"])

        for ein, screen_name in screen_name_list:
            tweets = tweepy.Cursor(api.user_timeline,
                screen_name = screen_name, include_rts = True, tweet_mode = 'extended')
            for tweet in tweets.items(3):
                w.writerow([
                    ein,
                    tweet.id,
                    tweet.full_text.replace('\n', ' ').encode('utf-8'),
                    tweet.created_at,
                    tweet.retweeted,
                    tweet.favorite_count,
                    tweet.retweet_count,
                    tweet.user.id,
                    tweet.user.screen_name,
                    [hashtag["text"] for hashtag in tweet.entities["hashtags"]],
                    tweet.lang
                ])

#internal method used by get_follower_ids and get_following_ids
def get_follower_or_following_ids(writer, user_id_list, method):

    for ein, twitter_id in user_id_list:
            ids = []
            #max value of count could be 5000
            print("###Processing user: %r" % twitter_id)
            for page in tweepy.Cursor(method, user_id = twitter_id, count = 2000).pages():
                ids.extend(page)
                print("     %r ids fetched.." % len(ids))
            writer.writerow([ein, twitter_id, len(ids), ids])


def get_user_follower_ids(output_csv_file, user_id_list):

    print("get_user_follower_ids() started...")
    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        w = csv.writer(file)
        w.writerow(["ein", "twitter_id", "len(follower_ids)", "follower_ids"])
        get_follower_or_following_ids(w, user_id_list, api.get_follower_ids)
        

def get_user_following_ids(output_csv_file, user_id_list):
    
    print("get_user_following_ids() started...")
    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        w = csv.writer(file)
        w.writerow(["ein", "twitter_id", "len(following_ids)", "following_ids"])
        get_follower_or_following_ids(w, user_id_list, api.get_friend_ids)

#return list of users screen name. each element of the list is a tuple: (ein, screen_name)
def get_screen_name_from_user_twitter_info(input_file_name):
    df = pd.read_csv(input_file_name)
    return list(zip(df.ein, df.screen_name))

def get_twitter_ids_from_user_twitter_info(input_file_name):
    df = pd.read_csv(input_file_name)
    return list(zip(df.ein, df.twitter_id))

def get_search_keys_from_name(name):
    name = name.lower()
    words = re.findall("\w+", name)
    words_len = len(words)
    search_keys = []
    for l in range(2,  words_len + 1):
        i = 0
        while i + l <= words_len:
            search_keys.append(' '.join(words[i:i+l]))
            i = i + 1
    return search_keys

def get_search_results(input_dta_file, output_csv_file):

    
    tweepy_api = Tweepy_Wrapper(api)
    search_result_dict = dict()
    df = pd.read_stata(input_dta_file)

    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        
        w = csv.writer(file)
        w.writerow(["ein", "org_name", "twitter", "search_key", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"])

        progress_counter = 0
        for index in df.index:

            progress_counter = progress_counter + 1
            org_name = df.at[index, "name"]
            ein = df.at[index, "ein"]
            twitter = df.at[index, "Twitter"]
            print("processing organization: " + org_name)
            search_keys = get_search_keys_from_name(org_name)
            unique_ids = set()
            for search_key in search_keys:
                while True:
                    try:
                        users = tweepy_api.search_user_by_key(search_key)
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(15 * 60 + randint(1, 100))
                        resetConnection()
                        tweepy_api.update_api(api)
                        
                    
                for user in users:
                    
                    if user.id not in unique_ids:
                        unique_ids.add(user.id)
                        w.writerow([
                            ein,
                            org_name,
                            twitter,
                            search_key,
                            user.id,
                            user.name,
                            user.created_at,
                            user.description.replace('\n', ' ').encode('utf-8'),
                            user.favourites_count,
                            user.friends_count,
                            user.followers_count,
                            user.listed_count,
                            user.location.replace('\n', ' ').encode('utf-8'),
                            user.screen_name,
                            user.statuses_count,
                            user.time_zone,
                            user.verified
                        ])
           
            if progress_counter % 100 == 0:
                print("%r record processed..." % progress_counter)



if __name__ == '__main__':
    
    #get user info from twitter
    # screen_name_list = get_user_screen_names(os.path.join(RESOURES_PATH, INPUT_FILE_NAME)) 
    # generate_twitter_user_info_file(screen_name_list, os.path.join(RESOURES_PATH, USER_INFO_FILE_NAME))

    #get user tweets
    # valid_screen_names = get_screen_name_from_user_twitter_info(os.path.join(RESOURES_PATH, USER_INFO_FILE_NAME))
    # get_user_tweets(os.path.join(RESOURES_PATH, ALL_TWEETS_FILE_NAME), 3, valid_screen_names[:10])

    #get user followers:
    # valid_twitter_ids = get_twitter_ids_from_user_twitter_info(os.path.join(RESOURES_PATH, USER_INFO_FILE_NAME))
    # get_user_follower_ids(os.path.join(RESOURES_PATH, USER_FOLLOWERS_FILE_NAME), valid_twitter_ids[:7])

    # #get user followings (friends):
    # valid_twitter_ids = get_twitter_ids_from_user_twitter_info(os.path.join(RESOURES_PATH, USER_INFO_FILE_NAME))
    # get_user_following_ids(os.path.join(RESOURES_PATH, USER_FOLLOWINGS_FILE_NAME), valid_twitter_ids[:10])

    for key in get_search_keys_from_name("BATON ROUGE FIRE PREVENTION COMMITTEE"):
        print(key)
    #get_search_results(os.path.join(RESOURES_PATH, INPUT_FILE_NAME), os.path.join(RESOURES_PATH, SEARCH_RESULT_FILE_NAME))