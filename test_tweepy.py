import json
import pandas as pd
import os
import re
from urllib.parse import urlparse
import csv
import validators
import Google_Search_Wrapper
import Bing_Search_Wrapper
import Tweepy_Wrapper
import time
from random import randint
from thefuzz import fuzz
import unicodedata
from cleanco import basename
import datetime
from Constants import *
import Helper


PROJECT_PATH = r"F:\E\code\twitter-data-collector"
USER_INFO_FILE_NAME = "user_info"
ALL_TWEETS_FILE_NAME = "all_tweets.csv"
USER_FOLLOWERS_FILE_NAME = "user_follower_ids.csv"
USER_FOLLOWINGS_FILE_NAME = "user_following_ids.csv"

SEARCH_RESULT_FILE_NAME = "user_search_result"
SIMILARITY_SCORE_FILE_NAME = "similarity_score"


def isUrlValid(url):
    if pd.isna(url):
        return False
    return True if validators.url(url) else False

#get user screen_name from twitter url
def get_screen_name_from_url(url):
    return re.sub('/', '', urlparse(url).path)

#return current timestamp as string YYYYMMDDHHMMSS
def get_current_timestamp():
    now = datetime.datetime.now()
    return now.strftime("%Y%m%d%H%M%S")


#return list of twitter (ein, screen_name)
def get_user_screen_names(input_file):
    # df = pd.read_stata(input_file)
    df = pd.read_excel(input_file)
    ein_column_name = "EIN"
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
                user = Tweepy_Wrapper.get_user_info(screen_name = screen_name)
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
                print(e)
                print("Exception of type " + str(type(e)) + " details: ein = " + str(ein) + " screen_name = " + str(screen_name))
        Tweepy_Wrapper.save_cache_to_pickle()


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

#param sorted_idf_dict (key = word, value = idf_score). it can be any dict as long as the key is word
#write all the hight frequcny words in the input dictioary key in a file
def write_high_frequent_words_in_org_names(sorted_idf_dict):
    with (
        open(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.HIGH_FREQUENT_WORDS_IN_ORG_NAMES_20K.value + Extension.TXT.value), "w") as f_frequent,
        open(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.RARE_WORDS_IN_ORG_NAMES_20K.value + Extension.TXT.value), "w") as f_rare
    ):
        for word, idf in sorted_idf_dict.items():
            if Helper.is_high_frequency_word(word):
                f_frequent.write(word + "\n")
            else:
                f_rare.write(word + "\n")

def get_twitter_search_result_for_single_keyword(input_file, output_csv_file):

    df = pd.read_excel(input_file)
    unique_org_names = set(df["NAME"].apply(lambda name : name.lower().strip()))
    
    #we are not using idf right now
    idf_dict = Helper.compute_IDF(unique_org_names)
    sorted_idf_dict = dict(sorted(idf_dict.items(), key=lambda item: item[1], reverse=True))


    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        
        w = csv.writer(file)
        w.writerow(["ein", "org_name", "twitter", "search_key", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"])

        progress_counter = 0
        for index in df.index:
            ein = df.at[index, "EIN"]
            progress_counter = progress_counter + 1
            org_name = df.at[index, "NAME"]
            twitter = df.at[index, "Twitter"]
            print("processing organization: " + org_name)
            search_keys = Helper.get_single_word_search_keys_from_name(org_name)
            unique_ids = set()
            for search_key in search_keys:
                while True:
                    try:
                        users = Tweepy_Wrapper.search_user_by_key(search_key)
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(15 * 60 + randint(1, 100))
                        Tweepy_Wrapper.update_api()
                        
                    
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


def get_search_results(input_file, output_csv_file, except_ein_list):

    
    df = pd.read_excel(input_file)

    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        
        w = csv.writer(file)
        w.writerow(["ein", "org_name", "twitter", "search_key", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"])

        progress_counter = 0
        for index in df.index:
            ein = df.at[index, "EIN"]
            if ein in except_ein_list:
                continue
            progress_counter = progress_counter + 1
            org_name = df.at[index, "NAME"]
            twitter = df.at[index, "Twitter"]
            print("processing organization: " + org_name)
            search_keys = get_search_keys_from_name(org_name)
            unique_ids = set()
            for search_key in search_keys:
                while True:
                    try:
                        users = Tweepy_Wrapper.search_user_by_key(search_key)
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(15 * 60 + randint(1, 100))
                        Tweepy_Wrapper.update_api()
                        
                    
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

#replace accented character with corresponding ascii, and remove other non-ascii
def replace_non_ascii(s):
    return unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode()

def remove_punctuation(s):
    return re.sub(r'[^\w\s\-]', '', s)

def remove_legal_control(s):
    return basename(s)

def lower_case(s):
    return s.lower() if isinstance(s, str) else ""

def clean_name(s):
    if pd.isnull(s):
        return ""
    s = lower_case(s)
    s = remove_legal_control(s)
    s = replace_non_ascii(s)
    s = remove_punctuation(s)
    return s
    

def compute_simililarity(input_file_name, output_file_name):
    df = pd.read_csv(input_file_name, encoding='utf-8')
    
    ##data cleaning
    #remove legal control terms
    df["org_name_cleaned"] = df["org_name"].apply(lambda s : clean_name(s))
    df["name_cleaned"] = df["name"].apply(lambda s : clean_name(s))

    #compute similarity using thefuzz
    df["fuzz_ratio"] = df.apply(lambda row : fuzz.ratio(row["org_name_cleaned"], row["name_cleaned"]), axis = 1)
    df["fuzz_partial_ratio"] = df.apply(lambda row : fuzz.partial_ratio(row["org_name_cleaned"], row["name_cleaned"]), axis = 1)
    df["fuzz_token_sort_ratio"] = df.apply(lambda row : fuzz.token_sort_ratio(row["org_name_cleaned"], row["name_cleaned"]), axis = 1)
    df["fuzz_token_set_ratio"] = df.apply(lambda row : fuzz.token_set_ratio(row["org_name_cleaned"], row["name_cleaned"]), axis = 1)
    write_df_to_csv(output_file_name, df)

#return a dictionary
#key = twitter_id
#value = set of eins' that belongs to that twitter_id
def create_twitter_id_dictionary(input_file):
    df = pd.read_csv(input_file)
    id_dict = dict()
    for index, row in df.iterrows():
        ein = row["ein"]
        twitter_id = row["twitter_id"]
        if twitter_id not in id_dict:
            id_dict[twitter_id] = set()
        ein_set = id_dict[twitter_id]
        ein_set.add(ein)
    return id_dict

def is_in_dict(id_dict, twitter_id, ein):
    if twitter_id not in id_dict:
        return 0
    return int(ein in id_dict[twitter_id])


#add a column "is_match" in similarity_score file. is_match = 1 if the search_result match with the manually found twitter handle
def update_is_match(id_dict, input_file_name):
    df = pd.read_csv(input_file_name)
    df["is_match"] = df.apply(lambda row : is_in_dict(id_dict, twitter_id=row["twitter_id"], ein = row["ein"]), axis = 1)
    df.to_csv(input_file_name)

#return 1 if given ein and twitter_id combo is in the search_result
def is_in_search_result(ein, twitter_id, similarity_score_df):
    return int(twitter_id in similarity_score_df[similarity_score_df["ein"] == ein]["twitter_id"].values)

def update_is_in_search_result(user_info_file_name, similarity_score_file_name):
    
    user_info_df = pd.read_csv(user_info_file_name)
    is_in_search_result_column = "is_in_search_result"
    if is_in_search_result_column not in user_info_df:
        user_info_df[is_in_search_result_column] = 0

    similarity_score_df = pd.read_csv(similarity_score_file_name)

    user_info_df[is_in_search_result_column] = user_info_df.apply(lambda row : 
                                            1 if row[is_in_search_result_column] == 1 
                                            else is_in_search_result(ein = row["ein"], twitter_id = row["twitter_id"], similarity_score_df = similarity_score_df), 
                                            axis=1)
    user_info_df.to_csv(user_info_file_name)



def get_except_list(similarity_score_file, user_info_file):

    similarity_score_df = pd.read_csv(similarity_score_file)
    found_eins = set(similarity_score_df["ein"].unique())
    user_info_df = pd.read_csv(user_info_file)
    failed_eins = user_info_df[user_info_df["is_in_search_result"] == 0]["ein"].unique()
    except_eins = found_eins.difference(failed_eins)
    return except_eins


def get_search_engine_search_query(org_name, location):
    location = location.lower()
    search_query = org_name.lower()
    if location not in search_query:
        search_query = search_query + " " + location
    search_query = search_query + " site:twitter.com"
    return search_query

"""
for given org_name and location return the search_engine query string
if location string does not exists in org_name string, then append it to org_name to form search_query
"""
def get_search_engine_search_query_for_website(org_name, location):
    location = location.lower()
    search_query = org_name.lower()
    if location not in search_query:
        search_query = search_query + " " + location
    return search_query

def get_google_search_results(input_file, output_csv_file):

    df = pd.read_excel(input_file, engine="openpyxl")

    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        
        w = csv.writer(file)
        w.writerow(["ein", "org_name", "twitter", "search_key", "google_url", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"])
        for index in df.index:
            if index % 200 == 0:
                print("%r record processed. " % index)
            org_name = df.at[index, "NAME"]
            ein = df.at[index, "EIN"]
            twitter = df.at[index, "Twitter"]
            search_key = get_search_engine_search_query(org_name, location = "baton rouge")
            urls = Google_Search_Wrapper.getGoogleSearchResultsByMe(search_key)
            screen_name_list = [] #cotains both the screen_name and corresponding google search result url
            screen_name_set = set()
            
            #get all the screen_names with google search
            for url in urls:
                try:
                    #if the url is a status url
                    if "status" in url and "twitter" in url:
                        status_id = Helper.get_status_id_from_status_url(url)
                        screen_name = Tweepy_Wrapper.get_screen_name_by_status_id(status_id)
                        screen_name = screen_name.lower()
                        if screen_name not in screen_name_set:
                            screen_name_list.append((screen_name, url))
                            screen_name_set.add(screen_name)

                    elif "twitter" in url:
                        screen_name = Helper.get_screen_name_from_profile_url(url)
                        screen_name = screen_name.lower()
                        if screen_name not in screen_name_set:
                            screen_name_list.append((screen_name, url))
                            screen_name_set.add(screen_name)
                except Exception as e:
                    print("url parsing error. ein: {%r} org_name: {%r} url: {%r}" % (ein, org_name, url))
            
            #collect user info of the screen_names

            for screen_name in screen_name_list:
                try:
                    user = Tweepy_Wrapper.get_user_info(screen_name[0])
                    w.writerow([
                        ein,
                        org_name,
                        twitter,
                        search_key,
                        screen_name[1], #this is the url
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
                except Exception as e:
                    print("user info fetch error. ein: {%r} org_name: {%r} screen_name: {%r}" % (ein, org_name, screen_name))


 
def get_twitter_pages_by_bing_search(input_file, output_csv_file):
    
    
    df = pd.read_excel(input_file, engine="openpyxl")

    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        
        w = csv.writer(file)
        w.writerow(["ein", "org_name", "twitter", "search_key", "bing_url", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"])
        for index in df.index:
            if index % 200 == 0:
                print("%r record processed. " % index)
            if index and index % 3 == 0:
                time.sleep(1.1)
            org_name = df.at[index, "NAME"]
            ein = df.at[index, "EIN"]
            twitter = df.at[index, "Twitter"]
            search_key = get_search_engine_search_query(org_name, location = "baton rouge")
            urls = Bing_Search_Wrapper.get_search_result(search_key=search_key)
            screen_name_list = [] #cotains both the screen_name and corresponding bing search result url
            screen_name_set = set()
            search_result_screen_name_set = set()
            
            #get all the screen_names with google search
            for url in urls:
                try:
                    #if the url is a status url
                    if Helper.is_twitter_status_url(url):
                        search_result_screen_name = Helper.get_screen_name_from_profile_url(url).lower()
                        if search_result_screen_name not in screen_name_set and search_result_screen_name not in search_result_screen_name_set:
                            status_id = Helper.get_status_id_from_status_url(url)
                            screen_name = Tweepy_Wrapper.get_screen_name_by_status_id(status_id)
                            ##for some users tweets are private. for them we can't get screen_name by tweet_id. Use the screen_name from search results
                            if not screen_name:
                                screen_name = search_result_screen_name
                            if screen_name not in screen_name_set:
                                screen_name_list.append((screen_name, url))
                                screen_name_set.add(screen_name)
                                search_result_screen_name_set.add(search_result_screen_name)

                    elif "twitter" in url and not Helper.is_twitter_hastag_url(url): #its a twitter profile_url
                        screen_name = Helper.get_screen_name_from_profile_url(url)
                        screen_name = screen_name.lower()
                        if screen_name not in screen_name_set:
                            screen_name_list.append((screen_name, url))
                            screen_name_set.add(screen_name)

                except Exception as e:
                    print("url parsing error. ein: {%r} org_name: {%r} url: {%r}" % (ein, org_name, url))
                    print(e)
            
            #collect user info of the screen_names

            for screen_name in screen_name_list:
                try:
                    user = Tweepy_Wrapper.get_user_info(screen_name[0])
                    w.writerow([
                        ein,
                        org_name,
                        twitter,
                        search_key,
                        screen_name[1], #this is the search result url
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
                except Exception as e:
                    print("user info fetch error. ein: {%r} org_name: {%r} screen_name: {%r}" % (ein, org_name, screen_name))

"""
for each organization, search bing and records the result urls
"""
def get_bing_search_result(input_file_name, output_csv_file):
    
    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        
        df = pd.read_excel(input_file_name)
        w = csv.writer(file)
        w.writerow(["ein", "org_name", "search_key", "search_result_url"])
        for index in df.index:
            if index % 100 == 0:
                print(f"{index} record processed..")
            org_name = df.at[index, "NAME"]
            ein = df.at[index, "EIN"]
            search_key = get_search_engine_search_query_for_website(org_name=org_name, location="baton rouge")
            urls = Bing_Search_Wrapper.get_search_result(search_key=search_key)
            for url in urls:
                w.writerow([ein, org_name, search_key, url])

def get_external_link_from_website(input_csv_file, output_csv_file, netloc_frequency_for_bing_search_csv_file):

    exclude_netlocs = set(["www.apartments.com",
                            "www.msn.com",
                            "www.zillo.com",
                            "www.salary.com",
                            "en-gb.facebook.com",
                            "www.instagram.com",
                            "www.voa.org",
                            "business.facebook.com",
                            "www.usnews.com",
                            "doctor.webmd.com",
                            "www.gofundme.com",
                            "local.yahoo.com",
                            "www.glassdoor.com",
                            "twitter.com",
                            "apps.apple.com",
                            "www.apartments.com",
                            "www.cnn.com",
                            "www.msn.com",
                            "www.apartmentguide.com",
                            "www.usbanklocations.com",
                            "www.independent.co.uk",
                            "www.secure.facebook.com",
                            "www.rent.com"])
    netloc_frequency_df = pd.read_csv(netloc_frequency_for_bing_search_csv_file, header=None, names=['netloc_url', 'frequency'])
    netloc_frequency_dict = dict(zip(netloc_frequency_df["netloc_url"], netloc_frequency_df["frequency"]))
    
    input_df = pd.read_csv(input_csv_file)
    unique_search_result_urls = input_df['search_result_url'].unique()
    print(len(unique_search_result_urls))
    #remove those urls whose netloc frequency is very high
    unique_search_result_urls = [ url for url in unique_search_result_urls if netloc_frequency_dict[Helper.get_netloc(url)] < 30 ]
    print(len(unique_search_result_urls))

    #remove those urls whose netloc is in exclude_netlocs
    unique_search_result_urls = [ url for url in unique_search_result_urls if Helper.get_netloc(url) not in exclude_netlocs ]
    print(len(unique_search_result_urls))
    
    import WebSiteCrawler
    crawled_urls = WebSiteCrawler.crawl_urls(unique_search_result_urls)
    crawled_urls_dict = dict(zip(unique_search_result_urls, crawled_urls)) #key = bing search result url #value = list of external links crawled from that url


    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        w = csv.writer(file)
        w.writerow(["ein", "org_name", "search_key", "search_result_url", "scrapped_external_link_from_search_result_url"])

        for index in input_df.index:
            if index % 300 == 0:
                print(f"{index} recored processed..")

            search_result_url = input_df.at[index, "search_result_url"]
            if search_result_url not in crawled_urls_dict:
                continue
            
            ein = input_df.at[index, "ein"]
            org_name = input_df.at[index, "org_name"]
            search_key = input_df.at[index, "search_key"]

            scrapped_external_links = crawled_urls_dict[search_result_url]

            for scrapped_external_link in scrapped_external_links:
                w.writerow([ein, org_name, search_key, search_result_url, scrapped_external_link])
    
def filter_twitter_links_from_exteral_links(input_file, output_file):

    df = pd.read_csv(input_file)

    external_link_column_name = "scrapped_external_link_from_search_result_url"
    df[external_link_column_name] = df[external_link_column_name].str.lower()
    #remove the external links, which are not twitter link
    df = df[df[external_link_column_name].str.fullmatch(re.compile(".*twitter.com/([^\?/]+)/?"))]

    #for an organization we there might be duplicate twitter link collected. dictionary is used to remove those duplicates
    from collections import defaultdict
    screen_name_dict = defaultdict(list) #key = ein, value = list of screen_name found from websites

    user_attr_list = ["ein", "org_name", "website_twitter_link", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"]

    with open(output_file, "w", newline = '', encoding = "utf-8") as file:
        w = csv.writer(file)
        w.writerow(user_attr_list)
        for index in df.index:
            if index and index % 300 == 0:
                print(f"{index} record processed..")
            ein = df.at[index, "ein"]
            org_name = df.at[index, "org_name"]
            wesite_twitter_link = df.at[index, "scrapped_external_link_from_search_result_url"]
            screen_name = Helper.get_screen_name_from_profile_url(wesite_twitter_link)
            if not screen_name:
                continue
            if ein in screen_name_dict and screen_name in screen_name_dict.get(ein):
                continue
            screen_name_dict[ein].append(screen_name)
            try:
                user = Tweepy_Wrapper.get_user_info(screen_name)
                w.writerow([
                    ein,
                    org_name,
                    wesite_twitter_link,
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
            except Exception as e:
                print("user info fetch error. ein: {%r} org_name: {%r} screen_name: {%r}" % (ein, org_name, screen_name))
        Tweepy_Wrapper.save_cache_to_pickle()

def get_ein_twitterids_dict(df):
    return {k: set(g["twitter_id"].tolist()) for k,g in df.groupby("ein")}

def get_twitter_info_dict(input_df):
    df = input_df.copy()
    df.set_index("twitter_id", drop=True, inplace=True)
    df = df[~df.index.duplicated(keep = 'first')]
    return df.to_dict(orient="index")

def merge_search_results(orig_file, 
                        twitter_search_result, 
                        twitter_single_keyword_search_result = None, 
                        bing_search_result = None, 
                        website_scrapped_result =None,
                        output_file = None):
    # ugly code :(
    twitter_info_dicts = []
    search_result_dicts = []
    df = pd.read_csv(twitter_search_result, encoding='utf-8')
    search_result_dicts.append(get_ein_twitterids_dict(df))
    twitter_info_dicts.append(get_twitter_info_dict(df))

    
    df = pd.read_csv(twitter_single_keyword_search_result, encoding='utf-8')
    dict_twitter_single_keyword_search_result = get_ein_twitterids_dict(df)
    search_result_dicts.append(get_ein_twitterids_dict(df))
    twitter_info_dicts.append(get_twitter_info_dict(df))

    df = pd.read_csv(bing_search_result, encoding='utf-8')
    dict_twitter_single_keyword_search_result = get_ein_twitterids_dict(df)
    search_result_dicts.append(get_ein_twitterids_dict(df))
    twitter_info_dicts.append(get_twitter_info_dict(df))

    df = pd.read_csv(website_scrapped_result, encoding='utf-8')
    dict_twitter_single_keyword_search_result = get_ein_twitterids_dict(df)
    search_result_dicts.append(get_ein_twitterids_dict(df))
    twitter_info_dicts.append(get_twitter_info_dict(df))

    #merge all the twitter_info_dicts
    twitter_info_dict = dict()
    for d in twitter_info_dicts:
        twitter_info_dict = twitter_info_dict | d

    

    attr_list = ["ein", "org_name", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"]

    with open(output_file, "w", newline = '', encoding = "utf-8") as file:
        w = csv.writer(file)
        w.writerow(attr_list)
        df = pd.read_excel(orig_file)
        for index in df.index:
            ein = df.at[index, "EIN"]
            org_name = df.at[index, "NAME"]
            found_twitter_ids = set()
            for search_result_dict in search_result_dicts:
                ids = search_result_dict.get(ein)
                if ids:
                    found_twitter_ids = found_twitter_ids.union(ids)
            if not found_twitter_ids:
                continue
        
            for twitter_id in found_twitter_ids:
                    twitter_info = twitter_info_dict[twitter_id]
                    w.writerow([
                        ein,
                        org_name,
                        twitter_id,
                        twitter_info["name"],
                        twitter_info["created_at"],
                        twitter_info["description"].deconde,
                        twitter_info["favourites_count"],
                        twitter_info["friends_count"],
                        twitter_info["followers_count"],
                        twitter_info["listed_count"],
                        twitter_info["location"],
                        twitter_info["screen_name"],
                        twitter_info["statuses_count"],
                        twitter_info["time_zone"],
                        twitter_info["verified"]
                    ])
    
    

if __name__ == '__main__':
    
    ##get user info from twitter
    ##get the basic user info for the organizations for which twitter account was found by natalie
    # screen_name_list = get_user_screen_names(os.path.join(Constants.RESOURCES_PATH.value,  FILENAME.CLEANED_INPUT_FILE.value+ Extension.XLSX.value))
    # generate_twitter_user_info_file(screen_name_list, 
    #     os.path.join(Constants.RESOURCES_PATH.value, FILENAME.USER_INFO_FILE.value + Extension.CSV.value))

    # get_twitter_search_result_for_single_keyword(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.CLEANED_INPUT_FILE.value + Extension.XLSX.value),
    #     os.path.join(Constants.RESOURCES_PATH.value, FILENAME.SINGLE_KEYWORD_TWITTER_SEARCH_RESULTS.value + Extension.CSV.value))


    # """
    # add column is_match in similarity_score csv. is_match = 1 if it's a match with manually found search result
    # step 1: create a dictionary with key = twitter_id and value = "set of eins" from user_info file
    # step 2: iterate over the rows of similarity_score.csv file and check the map to update the value of is_match
    # """
    # twitter_id_dict = dict()
    # user_info_file_name = "20220912135645_user_info-20220909_input_file_natalie.csv"
    # similary_score_file_name = "20220908-similarity_score-20220908_user_search_result.csv"
    # twitter_id_dict = create_twitter_id_dictionary(os.path.join(RESOURES_PATH, user_info_file_name))
    # print(len(twitter_id_dict))
    # update_is_match(twitter_id_dict, input_file_name=os.path.join(RESOURES_PATH, similary_score_file_name))

    

    """
    add a column is_in_search_result in user_info file.
    is_in_search_result = 1 if the organization was in any of the search result for that organization
    """
    # user_info_file_name = "20220912135645_user_info-20220909_input_file_natalie.csv"
    # similary_score_file_name = "20220908-similarity_score-20220908_user_search_result.csv"
    # update_is_in_search_result(os.path.join(RESOURES_PATH, user_info_file_name), 
    #                             os.path.join(RESOURES_PATH, similary_score_file_name))
    
    #####################################################################

    #get user tweets
    # valid_screen_names = get_screen_name_from_user_twitter_info(os.path.join(RESOURES_PATH, USER_INFO_FILE_NAME))
    # get_user_tweets(os.path.join(RESOURES_PATH, ALL_TWEETS_FILE_NAME), 3, valid_screen_names[:10])

    #get user followers:
    # valid_twitter_ids = get_twitter_ids_from_user_twitter_info(os.path.join(RESOURES_PATH, USER_INFO_FILE_NAME))
    # get_user_follower_ids(os.path.join(RESOURES_PATH, USER_FOLLOWERS_FILE_NAME), valid_twitter_ids[:7])

    # #get user followings (friends):
    # valid_twitter_ids = get_twitter_ids_from_user_twitter_info(os.path.join(RESOURES_PATH, USER_INFO_FILE_NAME))
    # get_user_following_ids(os.path.join(RESOURES_PATH, USER_FOLLOWINGS_FILE_NAME), valid_twitter_ids[:10])
    

    """
    search again for the organizations for which either no search result were found or their twitter page were found manually but not automatically
    step 1: find the eins' which will not be searched (except_ein_list).
    step 2: search for all eins except for those in except_ein_list
    """
    # user_info_file_name = "20220912135645_user_info-20220909_input_file_natalie.csv"
    # similary_score_file_name = "20220908-similarity_score-20220908_user_search_result.csv"
    # except_ein_list = get_except_list(similarity_score_file=os.path.join(RESOURES_PATH, similary_score_file_name),
    #                             user_info_file=os.path.join(RESOURES_PATH, user_info_file_name))
    # with open(os.path.join(RESOURES_PATH, "except_ein_list.txt"), "w") as file1:
    #     for ein in except_ein_list:
    #         file1.write(str(ein))
    #         file1.write("\n")

    # print(len(except_ein_list))
    # search_result_file_name = get_current_timestamp() + "_filtered_" + SEARCH_RESULT_FILE_NAME + EXTENSION_CSV
    # get_search_results(os.path.join(RESOURES_PATH, INPUT_FILE_NAME + EXTENSION_XLSX), os.path.join(RESOURES_PATH, search_result_file_name), except_ein_list=except_ein_list)

    #####################################################
    """
    compute similarity score for those newly found search result
    """
    # filtered_search_result_file_name = "20220912225645_filtered_user_search_result.csv"
    # filtered_similarity_score_file_name = get_current_timestamp() + "_filtered_" + SIMILARITY_SCORE_FILE_NAME + EXTENSION_CSV
    # compute_simililarity(os.path.join(RESOURES_PATH, filtered_search_result_file_name), os.path.join(RESOURES_PATH, filtered_similarity_score_file_name))

    #####################################################



    # """
    # update is_in_search_result in user_info file.
    # is_in_search_result = 1 if the organization was in any of the search result for that organization
    # """
    # user_info_file_name = "20220912135645_user_info-20220909_input_file_natalie.csv"
    # filtered_similary_score_file_name = "20220913001736_filtered_similarity_score.csv"
    # update_is_in_search_result(os.path.join(RESOURES_PATH, user_info_file_name), 
    #                             os.path.join(RESOURES_PATH, filtered_similary_score_file_name))
    
    #####################################################################

    #get_search_results(os.path.join(RESOURES_PATH, INPUT_FILE_NAME), os.path.join(RESOURES_PATH, SEARCH_RESULT_FILE_NAME))
    # compute_simililarity(os.path.join(RESOURES_PATH, SEARCH_RESULT_FILE_NAME), os.path.join(RESOURES_PATH, SIMILARITY_SCORE_FILE_NAME))

    # get_google_search_results(os.path.join(Constants.RESOURES_PATH.value, FILENAME.INPUT.value + Extension.XLSX.value), 
    #         os.path.join(Constants.RESOURES_PATH.value, FILENAME.GOOGLE_SEARCH.value + Extension.CSV.value))
    
    # get_twitter_pages_by_bing_search(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.INPUT.value + Extension.XLSX.value), 
    #         os.path.join(Constants.RESOURCES_PATH.value, FILENAME.BING_SEARCH.value + Extension.CSV.value))

    # get_bing_search_result(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.INPUT.value + Extension.XLSX.value), 
    #         os.path.join(Constants.RESOURCES_PATH.value, FILENAME.BING_SEARCH_FOR_WEBSITE.value + Extension.CSV.value))

    # get_external_link_from_website(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.BING_SEARCH_FOR_WEBSITE.value + Extension.CSV.value), 
    #         os.path.join(Constants.RESOURCES_PATH.value, FILENAME.EXTERNAL_LINK_FROM_BING_SEARCHED_WEBSITE.value + Extension.CSV.value),
    #         os.path.join(Constants.RESOURCES_PATH.value, FILENAME.NETLOCK_FREQUENCY_FOR_BING_SEARCH.value + Extension.CSV.value))

    # filter_twitter_links_from_exteral_links(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.EXTERNAL_LINK_FROM_BING_SEARCHED_WEBSITE.value + Extension.CSV.value),
    #     os.path.join(Constants.RESOURCES_PATH.value, FILENAME.TWITTER_LINK_DATA_FROM_BING_SEARCHED_WEBSITE.value + Extension.CSV.value))

    merge_search_results(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.CLEANED_INPUT_FILE.value + Extension.XLSX.value),
                        os.path.join(Constants.RESOURCES_PATH.value, FILENAME.TWITTER_SEARCH_RESULTS.value + Extension.CSV.value),
                        os.path.join(Constants.RESOURCES_PATH.value, FILENAME.SINGLE_KEYWORD_TWITTER_SEARCH_RESULTS.value + Extension.CSV.value),
                        os.path.join(Constants.RESOURCES_PATH.value, FILENAME.BING_SEARCH.value + Extension.CSV.value),
                        os.path.join(Constants.RESOURCES_PATH.value, FILENAME.TWITTER_LINK_DATA_FROM_BING_SEARCHED_WEBSITE.value + Extension.CSV.value),
                        os.path.join(Constants.RESOURCES_PATH.value, FILENAME.MERGED_SEARCH_RESULTS.value + Extension.CSV.value))
