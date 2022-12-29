import json
import tweepy
from Constants import *
import pickle
from UserDTO import *
import Helper
#read all keys from json file
with open("keys.json") as file:
    keysjson = json.load(file)

api_key = keysjson["api_key"]
api_key_secret = keysjson["api_key_secret"]
bearer_token = keysjson["bearer_token"]
access_token = keysjson["access_token"]
access_token_secret = keysjson["access_token_secret"]

#authentication and authrization of the api

def createAPI():
    auth = tweepy.OAuth1UserHandler(consumer_key = api_key, consumer_secret = api_key_secret, 
        access_token= access_token, access_token_secret= access_token_secret)
    return tweepy.API(auth, wait_on_rate_limit=True)

API = createAPI()
SEARCH_CACHE = dict()
USER_CACHE = dict()
STATUS_CACHE = dict()


#load user info cache from pickle file
def load_cache_from_pickle():
    if os.path.exists(Constants.TWEETER_USER_INFO_CACHE_FILE_PATH.value):
        global USER_CACHE
        with open(Constants.TWEETER_USER_INFO_CACHE_FILE_PATH.value, 'rb') as handle:
            USER_CACHE = pickle.load(handle)
    else:
        print("warning: user cache file not found..")

#save user info cahce to a pickle file
def save_cache_to_pickle():
    global USER_CACHE
    if len(USER_CACHE):
        with open(Constants.TWEETER_USER_INFO_CACHE_FILE_PATH.value, 'wb') as handle:
            print(f"saving user info cache to {Constants.TWEETER_USER_INFO_CACHE_FILE_PATH.value}")
            pickle.dump(USER_CACHE, handle)

load_cache_from_pickle()

def clear_cache():
    global USER_CACHE
    USER_CACHE.clear()

def update_api():
    global API
    API = createAPI()

def search_user_by_key(search_key):

    if search_key in SEARCH_CACHE:
        print("cache hit!: search_key = " + search_key)
        print("total search results = %r" % (len(SEARCH_CACHE[search_key])))
        return SEARCH_CACHE[search_key]

    users = API.search_users(q = search_key, count = 20)
                    
    SEARCH_CACHE[search_key] = users
    return users

def get_user_info(screen_name):

    if screen_name in USER_CACHE:
        print("cache hit! search_key = " + screen_name)
        return USER_CACHE[screen_name]
    user_info = API.get_user(screen_name = screen_name)
    USER_CACHE[screen_name] = user_info
    return user_info

def get_status(status_id):
    
    if status_id in STATUS_CACHE:
        return STATUS_CACHE[status_id]
    status = None
    try:
        status = API.get_status(status_id)
        STATUS_CACHE[status_id] = status
    except Exception as e:
        print(f"status_id = {status_id} error = {e}")
    return status

def get_screen_name_by_status_id(status_id):
    
    status = get_status(status_id)
    if status:
        if status.user:
            return status.user.screen_name
    return None

if __name__ == '__main__':
    
    # for user in search_user_by_key("baton rouge"):
    #     print(user.id)
        
    # screen_name = "chessmensch"
    # print(get_user_info(screen_name=screen_name))
    screen_name = "EBRSheriff"
    user  = get_user_info(screen_name=screen_name)
    # print(type(user.created_at))
    l = [user.id,user.name,user.created_at,user.description,user.favourites_count,user.friends_count,user.followers_count,user.listed_count,user.location,user.screen_name,user.statuses_count,user.time_zone,user.verified]
    print(l)
    save_cache_to_pickle()
    # clear_cache()
    # print("-----------------------")
    # user = get_user_info(screen_name=screen_name)
    # l = [user.id,user.name,user.created_at,user.description.replace('\n', ' '),user.favourites_count,user.friends_count,user.followers_count,user.listed_count,user.location.replace('\n', ' '),user.screen_name,user.statuses_count,user.time_zone,user.verified]
    # print(l)



    