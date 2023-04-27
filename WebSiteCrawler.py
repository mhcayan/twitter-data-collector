from SiteUrlCrawler import SiteUrlCrawler
from urllib.parse import urlparse, urljoin
from queue import Queue
import WebpageScrapper
from multiprocessing import Pool
from Constants import Constants
from Helper import fix_webcal_url

def is_valid(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def is_internal_url(child_url, parent_url):
    parsed_parent_url = urlparse(parent_url)
    parsed_child_url = urlparse(child_url)
    return parsed_parent_url.netloc == parsed_child_url.netloc

#Given a website url returns all the unique urls(both external and internal) in that website(is all pages)
def crawl_url(url, max_depth = 2, external_only = True):
    print(f"crawling url : {url}")
    visited_urls = set()
    if not is_valid(url):
        return visited_urls
    q = Queue()
    visited_urls.add(url)
    q.put(url)
    current_queue_size = q.qsize()
    removed_node_count = 0    
    depth = 0
    while not q.empty():
        parent_url = q.get()
        removed_node_count += 1
        for child_url in WebpageScrapper.scrape_links(parent_url):
            if child_url.startswith(Constants.WEBCAL_URL_PREFIX.value): #for some(7) urls we got requests.exceptions.InvalidSchema. all of those starts with 'webcal'
                child_url = fix_webcal_url(child_url)
            if not is_valid(child_url):
                continue
            if child_url in visited_urls:
                continue
            visited_urls.add(child_url)
            if is_internal_url(child_url, parent_url):
                q.put(child_url)

        if removed_node_count == current_queue_size:
            depth += 1
            if depth == max_depth:
                break
            removed_node_count = 0
            current_queue_size = q.qsize()
    visited_urls.remove(url)
    if external_only:
        visited_urls = {visited_url for visited_url in visited_urls if not is_internal_url(url, visited_url)}
    return visited_urls

def crawl_urls(url_list):
    number_of_processes = 25
    p = Pool(number_of_processes)
    results = p.map(crawl_url, url_list)
    p.terminate()
    p.join()
    return results

if __name__ == "__main__":
    import time
    # url = "https://www.alilones.com/"
    # links = crawl_url(url, 2, external_only=True)
    # url = "https://bsalsu.wordpress.com/"
    
    url_list = ["https://www.alilones.com/", "https://bsalsu.wordpress.com/", "https://lsms.org/"]
    start_time = time.time()
    results = crawl_urls(url_list)
    print("elapsed time %.2f" % (time.time() - start_time))
    print("__________________")
    print(len(results))
    for result in results:
        print("---------------------------")
        for link in result:
            print(link)

