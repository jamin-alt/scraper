# run_all_spiders.py
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())

# List all your spider names
spiders = ['greens_spider', 'spar_spider', 'welbees_spider']

for spider_name in spiders:
    process.crawl(spider_name)

process.start()