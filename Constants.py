from enum import Enum
import os
import re
class Constants(Enum):

    PROJECT_PATH = r"F:\E\code\twitter-data-collector"
    RESOURCES_PATH = os.path.join(PROJECT_PATH, r"resources\20221130")
    LOG_PATH = os.path.join(RESOURCES_PATH, r"log")
    TWEETER_CACHE_FILE_PATH = os.path.join(RESOURCES_PATH, r"user_info_cache.csv")
    TWEETER_USER_INFO_CACHE_FILE_PATH = os.path.join(RESOURCES_PATH, r"user_info_cache.pickle")
    LOG_FORMAT = "[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
    MINIMUM_SEARCH_KEY_LENGTH = 3
    WEBCAL_URL_PREFIX = "webcal://"
    

class Extension(Enum):
    XLSX = ".xlsx"
    CSV = ".csv"
    LOG = ".log"
    TXT = ".txt"

class FILENAME(Enum):
    CLEANED_INPUT_FILE = "0_manually_cleaned_twitter"
    USER_INFO_FILE = "1_twitter_user_info"
    SAMPLE_INPUT = "sample_input_5"
    GOOGLE_SEARCH = "google_search_result"
    BING_SEARCH = "bing_search_result"
    BING_SEARCH_FOR_WEBSITE = "2a_bing_search_for_website"
    EXTERNAL_LINK_FROM_BING_SEARCHED_WEBSITE = "2b_external_link_from_bing_searched_website"
    TEST40_EXTERNAL_LINK_FROM_BING_SEARCHED_WEBSITE = "test_40_external_link_from_bing_searched_website"
    TEST_LOG_1 = "test_log_1"
    TEST_LOG_2 = "test_log_2"
    WEBPAGE_SCRAPPER_LOG = "website_crawler_log"
    HIGH_FREQUENCY_WORDS_20K = "20k"
    HIGH_FREQUENT_WORDS_IN_ORG_NAMES_20K = "high_frequent_words_in_org_names_using_20k_file"
    RARE_WORDS_IN_ORG_NAMES_20K = "rare_words_in_org_names_using_20k_file"
    SHORT_SEARCH_KEYS = "short_search_keys"
    SINGLE_KEYWORD_TWITTER_SEARCH_RESULTS = "3a_twitter_search_results_for_single_keyword"
    TWITTER_SEARCH_RESULTS = "20220908_user_search_result-20220909_input_file_natalie"
    NETLOCK_FREQUENCY_FOR_BING_SEARCH = "2c_netlock_frequency_for_bing_search" #this file contains the frequncy of the netlock for the bing search result(file: 2a)
    TWITTER_LINK_DATA_FROM_BING_SEARCHED_WEBSITE = "2d_twitter_link_data_from_bing_searched_website"
    MERGED_SEARCH_RESULTS = "4_merged_search_result"
    IS_TWITTER_IN_SEARCH_RESULT = "5_is_twitter_in_search_result" #added a new binary column in "1_twitter_user_info.csv"; this new column contains whether search result has the twitter handle
    

class PATTERN(Enum):
    PROFILE_URL = re.compile(".*.twitter.com/([^\?/]+).*")
    STATUS_URL = re.compile(".*.twitter.com/.*/status/([\d]+).*")
    HASHTAG_URL = re.compile(".*.twitter.com/hashtag/.*")
    GOOGLE_SEARCH_RESULT_URL = re.compile(r"/url\?q=([a-zA-Z0-9:/\._\-]+).*")
