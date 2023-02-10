"""
a lot of the websites we found in our bing search result are common. we listed the websites that are quite frequent.
Crawling these website is meaningless. Since it is very unlikely that these websites are belongs a certain social organization of our interest.
Here we wil try to find the frequent websites found in our bing search
"""

import pandas as pd
import os
from Constants import *
from urllib.parse import urlparse
from collections import defaultdict
import csv

#this function returns the netloc of any given url
def get_netloc(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc

df = pd.read_csv(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.BING_SEARCH_FOR_WEBSITE.value + Extension.CSV.value))

#convert the bing search result url to netlock
netloc_list = df["search_result_url"].apply(lambda url : get_netloc(url))

#count the frequncy of each netlock 
netloc_freq = defaultdict(int)
for netloc in netloc_list:
    netloc_freq[netloc] += 1


#write the netloc frequency to a csv file
with open(os.path.join(Constants.RESOURCES_PATH.value, FILENAME.NETLOCK_FREQUENCY_FOR_BING_SEARCH.value + Extension.CSV.value), 'w', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file)
    for netloc, freq in dict(sorted(netloc_freq.items(), key = lambda item : item[1], reverse=True)).items():
        writer.writerow([netloc, freq])











