import requests
from bs4 import BeautifulSoup, SoupStrainer
from urllib.parse import urlparse, urljoin
from time import time
import Helper
from Constants import *
from Helper import *
import ScrapeOps_Wrapper
import requests
import http
requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

logger1 = setup_logger(FILENAME.WEBPAGE_SCRAPPER_LOG.value, os.path.join(Constants.LOG_PATH.value, FILENAME.WEBPAGE_SCRAPPER_LOG.value + Extension.LOG.value))

RETRY_STATUS_CODE = {403, 522, 530, 504, 502, 520, 525, 524, 523, 406, 410}
IGNORE_AND_SCRAP_STATUS_CODE = {401,400,404,500,}
SKIP_STAUTS_CODE = {999, 40, 302, 402,409, 503}

def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def format_url(url):
    parsed_url = urlparse(url)
    return parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path

REQUEST_TIMEOUT_IN_SEC = 40
EXCLUDE_EXTENSION = set([".pdf", ".xlsx", ".docx", ".jpeg", ".png"])
MAX_TRY_COUNT = 3
VERIFY_SSL = True

def scrape_links(input_url):
    
    links = []
    input_url = format_url(input_url)
    if not is_valid_url(input_url):
        return links
    try:
        global VERIFY_SSL
        url_extention = Helper.get_url_extension(url = input_url).lower()
        if url_extention in EXCLUDE_EXTENSION:
            return links
        try_count = MAX_TRY_COUNT
        while try_count:
            try:

                res = requests.get(url = input_url, headers=ScrapeOps_Wrapper.get_random_header(), verify = VERIFY_SSL, timeout=REQUEST_TIMEOUT_IN_SEC)
                VERIFY_SSL = True
                logger1.info("input_url: %s status_code: %d" % (input_url, res.status_code))
                if res.status_code == 200 or (res.status_code in IGNORE_AND_SCRAP_STATUS_CODE):
                    break
                elif res.status_code in SKIP_STAUTS_CODE:
                    return links
                elif res.status_code in RETRY_STATUS_CODE:
                    try_count = try_count - 1
                    if try_count < 1:
                        return links
                else:
                    return links

            except requests.exceptions.SSLError as e:
                logger1.exception("exception ssl for url: " + input_url)
                try_count = try_count - 1
                if try_count < 1:
                    return links
                VERIFY_SSL = False
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.TooManyRedirects) as e:
                logger1.exception("exception connection for url: " + input_url)
                try_count = try_count - 1
                if try_count < 1:
                    return links

            except Exception as e:
                logger1.exception("exception other for url: " + input_url)
                return links
        
        for link in BeautifulSoup(res.content, "html.parser", parse_only=SoupStrainer('a', href=True)):
            href = link.get("href")
            if href is None or href.strip() is None:
                continue
            if href.startswith("javascript"):
                continue
            if not href.startswith("http"):
                href = urljoin(input_url, href)
            links.append(href)
            
    except Exception as e:
        logger1.exception("exception scraping url: " + input_url)

    return links
            
if __name__ == "__main__":
    urls = ["https://www.alilones.com",
        "https://bsalsu.wordpress.com/",
        "https://www.amazon.com/Earbuds-Stereo-Headphones-Samsung-Galaxy/dp/B0BCPC5JRV/ref=pd_ci_mcx_mh_mcx_views_0?pd_rd_w=vTD8c&content-id=amzn1.sym.1bcf206d-941a-4dd9-9560-bdaa3c824953&pf_rd_p=1bcf206d-941a-4dd9-9560-bdaa3c824953&pf_rd_r=DGN39QCYTVHD4310E633&pd_rd_wg=3L6ls&pd_rd_r=f8dcfa3d-406c-4753-954b-f803d7454a96&pd_rd_i=B0BCPC5JRV&th=1",
        "https://gojagsports.com/404-1.aspx?url=%2fsb_output.aspx",
        "https://ao.linkedin.com/company/society-of-peer-mentors",
        "https://www.goodshop.com/user/profile",
        "https://jcilm.com/youth-academy/",
        "https://www.acfusa.org/",
        "https://vrlaonline.org/about-vrla/",
        "https://batonrougechristianlifemagazine.com/2015/11/01/open-air-ministries-people-serving-people/",
        "https://www.sites01.lsu.edu/wp/cfsn/christian-faculty-staff-at-other-schools/",
        'webcal://www.houstonorchidsociety.org/event/baton-rouge-orchid-society-show-and-sale/',
        'https://www.calvarybr.org/who',
        "https://house.legis.louisiana.gov/llwc/default_LLWC_AboutUs",
        "https://ltrc.lsu.edu/",
        "https://cathla.org/Main/About/Publications.aspx",
        "https://batonrougechristianlifemagazine.com/2015/11/01/open-air-ministries-people-serving-people/",
        "https://www.gabrielswaggart.org/crossfire/team",
        "https://www.livinghopefellowshipde.org/blog"]
    start_time = time()
    url = urls[-1]
    if url.startswith(Constants.WEBCAL_URL_PREFIX.value):
        url = Helper.fix_webcal_url(url)
    
    links = set(scrape_links(url))
    print(len(links))
    print("------------")
    for l in links:
        print(l)
    # print(len(set(scrape_links(url))))
    print(f"Time elapsed {time() - start_time}")


