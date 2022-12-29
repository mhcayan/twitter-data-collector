import imp
from SiteUrlCrawler import SiteUrlCrawler
import Helper
website = "https://sites.google.com/site/louisianachess/chess-club-locations-info/baton-rouge-chess-club"
crawler = SiteUrlCrawler(website)
for url in crawler.crawl(SiteUrlCrawler.Mode.EXTERNAL):
    print("Found: " + url)
    print("Resolved: " + Helper.resolve(url))
