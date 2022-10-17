import json
import pandas as pd
import os
import re
from urllib.parse import urlparse
import csv
import validators
import Google_Search_Wrapper
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
RESOURES_PATH = os.path.join(PROJECT_PATH, "resources")
USER_INFO_FILE_NAME = "user_info"
ALL_TWEETS_FILE_NAME = "all_tweets.csv"
USER_FOLLOWERS_FILE_NAME = "user_follower_ids.csv"
USER_FOLLOWINGS_FILE_NAME = "user_following_ids.csv"
INPUT_FILE_NAME = "20220909_input_file_natalie"
SEARCH_RESULT_FILE_NAME = "user_search_result"
SIMILARITY_SCORE_FILE_NAME = "similarity_score"

EXTENSION_CSV = ".csv"
EXTENSION_XLSX = ".xlsx"



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
                user = Tweepy_Wrapper.get_user(screen_name = screen_name)
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


def get_google_search_query(org_name, location):
    location = location.lower()
    search_query = org_name.lower()
    if location not in search_query:
        search_query = search_query + " " + location
    search_query = search_query + " site:twitter.com"
    return search_query

def get_google_search_results(input_file, output_csv_file):

    df = pd.read_excel(input_file)

    with open(output_csv_file, "w", newline = '', encoding = "utf-8") as file:
        
        w = csv.writer(file)
        w.writerow(["ein", "org_name", "twitter", "search_key", "twitter_id", "name", "created_at", "description", "favourites_count", "friends_count",
        "followers_count", "listed_count", "location", "screen_name", "statuses_count", "time_zone", "verified"])
        for index in df.index:
            if index % 100 == 0:
                print("%r record processed: " % index)
            org_name = df.at[index, "NAME"]
            ein = df.at[index, "EIN"]
            twitter = df.at[index, "Twitter"]
            search_key = get_google_search_query(org_name, location = "baton rouge")
            urls = Google_Search_Wrapper.getSearchResults(search_key, 10)
            screen_name_list = []
            screen_name_set = set()
            
            #get all the screen_names with google search
            for url in urls:
                try:
                    #if the url is a status url
                    if "status" in url:
                        status_id = Helper.get_status_id_from_status_url(url)
                        screen_name = Tweepy_Wrapper.get_screen_name_by_status_id(status_id)
                        screen_name = screen_name.lower()
                        if screen_name not in screen_name_set:
                            screen_name_list.append(screen_name)
                            screen_name_set.add(screen_name)

                    elif "twitter" in url:
                        screen_name = Helper.get_screen_name_from_profile_url(url)
                        screen_name = screen_name.lower()
                        if screen_name not in screen_name_set:
                            screen_name_list.append(screen_name)
                            screen_name_set.add(screen_name)
                except Exception as e:
                    print("url parsing error. ein: {%r} org_name: {%r} url: {%r}" % (ein, org_name, url))
            
            #collect user info of the screen_names
            print(screen_name_list)
            for screen_name in screen_name_list:
                try:
                    user = Tweepy_Wrapper.get_user_info(screen_name)
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
                except Exception as e:
                    print("user info fetch error. ein: {%r} org_name: {%r} screen_name: {%r}" % (ein, org_name, screen_name))

 

if __name__ == '__main__':
    
    # #get user info from twitter
    # screen_name_list = get_user_screen_names(os.path.join(RESOURES_PATH, INPUT_FILE_NAME + EXTENSION_XLSX))
    # user_info_file_name = get_current_timestamp() + "_" + USER_INFO_FILE_NAME + "-" + INPUT_FILE_NAME

    # generate_twitter_user_info_file(screen_name_list, 
    #     os.path.join(RESOURES_PATH, user_info_file_name + EXTENSION_CSV))

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

    get_google_search_results(os.path.join(Constants.RESOURES_PATH.value, FILENAME.SAMPLE_INPUT.value + Extension.XLSX.value), 
            os.path.join(Constants.RESOURES_PATH.value, FILENAME.GOOGLE_SEARCH.value + Extension.CSV.value))
    