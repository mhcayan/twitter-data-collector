from googlesearch import search
import time

TRY_COUNT = 2
#returns the urls of the google search results for given query. limit
def getSearchResults(query, limit):

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

    


if __name__ == '__main__':
    urls = getSearchResults("doc-dhl inc baton rouge site:twitter.com", 10)
    for url in urls:
        print(url)


