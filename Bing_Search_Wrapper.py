import requests
import json
import time

with open("keys.json") as file:
    keysjson = json.load(file)


subscription_key = keysjson["bing_subscription_key"]
headers = {"Ocp-Apim-Subscription-Key": subscription_key}
search_url = "https://api.bing.microsoft.com/v7.0/search"
MAX_TRY_COUNT = 2

def get_search_result(search_key):
    print(subscription_key)
    urls = []
    params = {"q": search_key, "textDecorations": True, "textFormat": "HTML"}
    try_count = MAX_TRY_COUNT
    while try_count > 0:
        try:
            response = requests.get(search_url, headers=headers, params=params)
            response.raise_for_status()
            search_results = response.json()
            
            for v in search_results["webPages"]["value"]:
                urls.append(v["url"])
            break
        except Exception as e:
            print(e)
            time.sleep(5)
            try_count = try_count - 1
    return urls

if __name__ == "__main__":
    search_key = "LOUISIANA STATE MEDICAL SOCIETY".lower()
    urls = get_search_result(search_key=search_key)
    print(len(urls))
    for url in urls:
        print(url)