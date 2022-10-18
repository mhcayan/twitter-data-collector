from googlesearch import search
import time
import requests
import bs4
import re

TRY_COUNT = 2
GOOGLE_SEARCH_URL = "https://google.com/search?q="
GOOGLE_SEARCH_RESULT_URL_PATTERN = re.compile(r"/url\?q=([a-zA-Z0-9:/\._\-]+).*")
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)\
            AppleWebKit/537.36 (KHTML, like Gecko) Cafari/537.36'}
#returns the urls of the google search results for given query. limit
def getSearchResultsByGoogleSearchAPI(query, limit):

    try_count = TRY_COUNT
    while try_count:
        try:
            #num = num of results to get on each request
            #stop = index of the last search result. if stop = 5 then it will return results from 0 to 4(if num >= 5).
            return [url for url in search(query, tld = "com", num = 10, stop = limit)]
        except Exception as e:
            print("google search error. query: {%r}" % (query))
            print(e)
            time.sleep(5)

        try_count = try_count - 1

    
def getGoogleSearchResultsByMe(query):
    print("query: " + query)
    complete_url = GOOGLE_SEARCH_URL + query 
    try_count = TRY_COUNT
    url_list = []
    while try_count:
        try_count = try_count - 1
        res = requests.get(complete_url, headers=headers)
        print("response code: %r" % (res.status_code))
        if res.status_code == 429:
            print("status code: 429")
            try:
                retry_after = res.headers["Retry-After"]
                print("sleeping for %r" % (retry_after))
                time.sleep(int(retry_after) + 2)
            except Exception as e:
                print(e)
                time.sleep(5)
            continue

        soup = bs4.BeautifulSoup(res.text, "html.parser")
        a_tags = soup.findAll("a")

        for a_tag in a_tags:
            href = a_tag["href"]
            if href.startswith("/url") and "twitter" in href:
                m = GOOGLE_SEARCH_RESULT_URL_PATTERN.match(href)
                if m:
                    url = m.group(1)
                    if "twitter" in url:
                        url_list.append(url)
        break
    
    print("len(url_list): %r\n" % (len(url_list)))
    return url_list


if __name__ == '__main__':
    name = "lichess"
    name = name.lower()
    query = name + " site:twitter.com"
    urls = getGoogleSearchResultsByMe(query)
    print(urls)
"""
    import Helper
    for url in urls:
        print(url)
        if "status" in url:
            print(Helper.get_status_id_from_status_url(url))
        else:
            print(Helper.get_screen_name_from_profile_url(url))
        print("\n")
"""


