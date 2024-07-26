from Constants import PATTERN
from Constants import Constants
import re
from ast import literal_eval
import urllib
import logging
from Constants import *
from Constants import FILENAME
from Constants import Extension
from urllib.parse import urlparse
from nltk.corpus import words
from nltk.stem.wordnet import WordNetLemmatizer

FREQUENT_WORD_SET = set()
Lematizer = WordNetLemmatizer()

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
        raise Exception("exception while loading high frequency words..")
        

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

    if not (byteString.startswith("b'") or byteString.startswith('b"')):
        return byteString
    return literal_eval(byteString).decode()

def resolve(url):
    return urllib.request.urlopen(url).geturl()

#source: https://stackoverflow.com/a/11233293/6752274
def setup_logger(name, log_file, level=logging.INFO):

    handler = logging.FileHandler(log_file, encoding='utf-8')        
    handler.setFormatter(logging.Formatter(Constants.LOG_FORMAT.value))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

def get_trailing_digits(s):
    # This regex pattern matches any digits at the end of the string. the digits can followed by one or more spaces
    pattern = r"(\d+)\s+$"
    
    match = re.search(pattern, s)
    
    if match:
        # Return the matched digits
        return match.group(0)
    else:
        # If no digits found at the end, return an empty string
        return ""

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

def get_netloc(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc

#returns the extension(e.g. .php) for the given input url
#https://stackoverflow.com/a/4776959/6752274
def get_url_extension(url):
    path = urlparse(url).path
    return os.path.splitext(path)[1]

def fix_webcal_url(url):
    return "http://" + url[len(Constants.WEBCAL_URL_PREFIX.value):];

ACRONYM_SKIP_WORDS = set([
    "a", "an", "the",  # Articles
    "and", "or", "but", "nor", "for", "yet", "so",  # Conjunctions
    "at", "by", "in", "of", "on", "to", "with", "from", "up", "down",  # Prepositions
    "over", "under", "between", "among", "through", "during", "before", "after", "around"
])


def generate_acronym(name):
    acronym = ''
    words = name.split()
    #single word name is considered as acronym
    if len(words) == 1:
        return name
    for word in words:
        if not word:
            continue
        word = Lematizer.lemmatize(word) #some plurals (e.g. professionals) can't be detected as english word, so we lemmatized it
        if word in ACRONYM_SKIP_WORDS:
            continue
        elif is_english_word(word):
            acronym += word[0]
        elif word.isnumeric():
            acronym += word
        elif word[0].islower():
            if len(words) < 3:
                acronym += word
            else:
                acronym += word[0]

    return acronym
        
NLTK_ENG_WORDS = set(word.lower() for word in words.words())

def is_english_word(word):

    return word in NLTK_ENG_WORDS

COMMON_ACRONYMED_WORDS = [
    ("baton rouge", 'br'),
    ("batonrouge", 'br'),
    ("louisiana", 'la'),
    ("medical", 'med')]

def get_common_acronymed_words():
    return COMMON_ACRONYMED_WORDS
    

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
    # print(get_single_word_search_keys_from_name("A bdkfjDl union kz LKf supeR "))
    # print(get_netloc("https://google.com"))
    # print(get_url_extension("http://lpdb.la.gov/Serving%20The%20Public/Programs/Baton%20Rouge%20Capital%20Conflict%20Office.php"))
    # print(fix_webcal_url("webcal://www.houstonorchidsociety.org/event/baton-rouge-orchid-society-show-and-sale/"))
    # print(is_english_word(""))
    # print(generate_acronym(name="a quick brown fox"))
    # print(generate_acronym('association for professionals in infection ctrl epidemiology'))
    print(generate_acronym("the nysge & of arizona"))
    print(generate_acronym("SIGMA GAMMA RHO SORORITY".lower()))
    print(generate_acronym("united brotherhood of carpenters joiners"))
    print(generate_acronym("UNITED COMMERCIAL TRAVELERS OF AMERICA".lower()))
    print(generate_acronym("east la company"))
    print(generate_acronym("AMERICAN LEGION AUXILIARY".lower()))
    print(generate_acronym("AMERICAN FEDERATION OF STATE COUNTY & MUNICIPAL EMPLOYEES".lower()))
    print(generate_acronym('labi foundation'))
    print(generate_acronym('laasdfbi foundation'))
    print(generate_acronym('left foundation'))
    # print(is_high_frequency_word("BUREAU"))
    
    





    



    