class Tweepy_Wrapper:
    
    def __init__(self, _api):
        self.api = _api
        self.cache = dict()
    
    def update_api(_api):
        self.api = _api
    
    def search_user_by_key(self, search_key):

        if search_key in self.cache:
            print("cache hit!")
            return self.cache[search_key]

        users = self.api.search_users(q = search_key, count = 20)
                        
        self.cache[search_key] = users
        return users


if __name__ == '__main__':
    
    import json
    import pandas as pd
    import os
    import re
    from urllib.parse import urlparse
    import csv
    import validators #for url validation


    PROJECT_PATH = r"F:\E\code\twitter-data-collector"
    RESOURES_PATH = os.path.join(PROJECT_PATH, "resources")
    USER_INFO_FILE_NAME = "user_info.csv"
    ALL_TWEETS_FILE_NAME = "all_tweets.csv"
    USER_FOLLOWERS_FILE_NAME = "user_follower_ids.csv"
    USER_FOLLOWINGS_FILE_NAME = "user_following_ids.csv"
    INPUT_FILE_NAME = "sm_org_17_18EBR2010ct_031021.dta"

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

    tweepy_wrapper = Tweepy_Wrapper(tweepy.API(auth, wait_on_rate_limit=True))
    for user in tweepy_wrapper.search_user_by_key("baton rouge"):
        print(user.id)

    for user in tweepy_wrapper.search_user_by_key("baton rouge"):
        print(user.id)




    