# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PricelyItem(scrapy.Item):
    # define the fields for your item here like:
    product_id = scrapy.Field()
    name = scrapy.Field()
    image = scrapy.Field()
    price = scrapy.Field()
    url = scrapy.Field()
    vendor = scrapy.Field()
    category = scrapy.Field()
    confidence = scrapy.Field()
    canonical_product_id = scrapy.Field()
    scraped_at = scrapy.Field()