import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from time import time
import Helper
import logging
from Constants import *
from Helper import *

HEADERS = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"}
logger1 = setup_logger(FILENAME.WEBPAGE_SCRAPPER_LOG.value, os.path.join(Constants.LOG_PATH.value, FILENAME.WEBPAGE_SCRAPPER_LOG.value + Extension.LOG.value))

def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def format_url(url):
    parsed_url = urlparse(url)
    return parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path

REQUEST_TIMEOUT_IN_SEC = 100
EXCLUDE_EXTENSION = set([".pdf", ".xlsx", ".docx"])

def scrape_links(input_url):
    
    links = []
    input_url = format_url(input_url)
    if not is_valid_url(input_url):
        return links
    try:
        url_extention = Helper.get_url_extension(url = input_url).lower()
        if url_extention in EXCLUDE_EXTENSION:
            return links
        if url_extention == ".php":
            res = requests.get(url = input_url, timeout=REQUEST_TIMEOUT_IN_SEC)
        else:
            res = requests.get(url = input_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_IN_SEC)
            

        if res.status_code != 200:
            logger1.error("input_url: %s status_code: %d" % (input_url, res.status_code))
            return links

        soup = BeautifulSoup(res.text, 'html.parser')
        
        for link in soup.find_all('a', href = True):
            href = link.get("href")
            if href:
                href = urljoin(input_url, href)
                if is_valid_url(href):
                    links.append(href)
                    logger1.info(href)
                    # print(href)
    except Exception as e:
        logger1.exception("Exception for url: " + input_url)

    return links
            
if __name__ == "__main__":
    url1 = "https://www.alilones.com"
    url2 = "https://bsalsu.wordpress.com/"
    url3 = "https://www.amazon.com/Earbuds-Stereo-Headphones-Samsung-Galaxy/dp/B0BCPC5JRV/ref=pd_ci_mcx_mh_mcx_views_0?pd_rd_w=vTD8c&content-id=amzn1.sym.1bcf206d-941a-4dd9-9560-bdaa3c824953&pf_rd_p=1bcf206d-941a-4dd9-9560-bdaa3c824953&pf_rd_r=DGN39QCYTVHD4310E633&pd_rd_wg=3L6ls&pd_rd_r=f8dcfa3d-406c-4753-954b-f803d7454a96&pd_rd_i=B0BCPC5JRV&th=1"
    start_time = time()
    print(len(set(scrape_links(url1))))
    print(f"Time elapsed {time() - start_time}")


