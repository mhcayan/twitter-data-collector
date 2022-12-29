from Constants import PATTERN
import re
from ast import literal_eval
import urllib
import logging
from Constants import *
from Constants import FILENAME
from Constants import Extension

FREQUENT_WORD_SET = set()

#https://github.com/derekchuank/high-frequency-vocabulary/blob/master/20k.txt
#20k most frequent words were used as high frequency words
def load_high_frequency_words():
    try:
        with open(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.HIGH_FREQUENCY_WORDS_20K.value + Extension.TXT.value)) as f:
        
            for line in f:
                line = line.strip()
                if line:
                    FREQUENT_WORD_SET.add(line)
    except Exception as e:
        print("high")

load_high_frequency_words()

def is_high_frequency_word(word):
    word = word.strip().lower()
    return word in FREQUENT_WORD_SET

def get_single_word_search_keys_from_name(org_name):
    org_name = org_name.strip().lower()
    search_keys = list()
    for word in org_name.split():
        if not is_high_frequency_word(word) and len(word) >= Constants.MINIMUM_SEARCH_KEY_LENGTH.value:
            search_keys.append(word)
    return search_keys


def get_status_id_from_status_url(url):
    return PATTERN.STATUS_URL.value.findall(url)[0]

def get_screen_name_from_profile_url(url):
    screen_name = PATTERN.PROFILE_URL.value.findall(url)[0]
    return re.sub(r"\W+", '', screen_name)

def is_twitter_hastag_url(url):
    return bool(PATTERN.HASHTAG_URL.value.match(url))

def is_twitter_status_url(url):
    return bool(PATTERN.STATUS_URL.value.match(url))

#return the frequncy of the words in the given column as a dictionary
def get_column_word_count(df, column_name):
    values = df[column_name].str.lower()
    unique_values = values[~values.duplicated()]
    freq_dict = unique_values.str.split(expand = True).stack().value_counts().to_dict()
    return freq_dict

#given a string which is byteEncoded. return the decoded string
#input: "b'ab'"
#returns: "ab"
def decodeByteString(byteString):
    return literal_eval(byteString).decode()

def resolve(url):
    return urllib.request.urlopen(url).geturl()

#source: https://stackoverflow.com/a/11233293/6752274
def setup_logger(name, log_file, level=logging.INFO):

    handler = logging.FileHandler(log_file)        
    handler.setFormatter(logging.Formatter(Constants.LOG_FORMAT.value))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def compute_IDF(documents):
    document_word_set_list = []
    #convert each document to set of words
    for document in documents:
        document_word_set_list.append(set(document.split()))
    
    unique_words = set().union(*document_word_set_list)

    idf_dict = dict.fromkeys(unique_words, 0)

    
    for document_word_set in document_word_set_list:
        for word in document_word_set:
                idf_dict[word] += 1
    
    N = float(len(documents))

    import math
    for word, count in idf_dict.items():
        idf_dict[word] = math.log(N / count)
    return idf_dict

def compute_IDF_by_sklearn(documents):
    
    document_word_set_list = []
    #convert each document to set of words
    for document in documents:
        document_word_set_list.append(set(document.split(' ')))
    
    unique_words = set().union(*document_word_set_list)
    from sklearn.feature_extraction.text import TfidfVectorizer


    tf = TfidfVectorizer(use_idf=True)
    tf.fit_transform(documents)

    idf_dict = dict()
    for word in unique_words:
        idf_dict[word] = tf.idf_[tf.vocabulary_[word]]
    return idf_dict



if __name__ == "__main__":
    status_url = "https://twitter.com/@CAUW/status/1575228220991049728"
    # print(get_screen_name_from_profile_url(status_url))
    # print(is_twitter_status_url(status_url))
    # print(is_twitter_hastag_url("https://twitter.com/hashtag/@CAUW"))
    # print(decodeByteString("b'ab'"))
    # idf_dict = compute_IDF(["i eat rice", "you eat burger", "you eat fish"])
    # for key, value in idf_dict.items():
    #     print(f"{key} : {value}")
    # print(dict(sorted(idf_dict.items(), key=lambda item: item[1])))
    print(get_single_word_search_keys_from_name("A bdkfjDl union kz LKf supeR "))