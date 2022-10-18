from Constants import PATTERN

def get_status_id_from_status_url(url):
    return PATTERN.STATUS_URL.value.findall(url)[0]

def get_screen_name_from_profile_url(url):
    return PATTERN.PROFILE_URL.value.findall(url)[0]

#return the frequncy of the words in the given column as a dictionary
def get_column_word_count(df, column_name):
    values = df[column_name].str.lower()
    unique_values = values[~values.duplicated()]
    freq_dict = unique_values.str.split(expand = True).stack().value_counts().to_dict()
    return freq_dict