from Constants import PATTERN
import re

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

if __name__ == "__main__":
    status_url = "https://twitter.com/@CAUW/status/1575228220991049728"
    print(get_screen_name_from_profile_url(status_url))
    print(is_twitter_status_url(status_url))
    print(is_twitter_hastag_url("https://twitter.com/hashtag/@CAUW"))