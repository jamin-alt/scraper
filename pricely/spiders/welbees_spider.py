import scrapy
import requests
from pricely.items import PricelyItem


class MyScraper:
    def __init__(self, api_url="https://welbees.mt/ajax/shop.php?action=load_product_detail"):
        self.api_url = api_url

    def get_api_data(self, code):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0"
        }
        payload = {"ProductCode": code}

        try:
            r = requests.post(self.api_url, headers=headers, data=payload, timeout=10)
            return r.json()
        except:
            return None



class WellbeesSpider(scrapy.Spider):
    name = "welbees_spider"
    vendor = "Welbees"

    categories = {
        "D-5430":"Baby",
        "D-5431":"Bakery",
        "D-5432":"Butcher Counter",
        "D-5433":"Chilled Food",
        "D-6854":"Christmas Hampers",
        "D-5447":"Clothes & Accessories",
        "D-5434":"Delicatessen",
        "D-5435":"Drinks",
        "D-5436":"Food Cupboard",
        "D-5438":"Frozen Food",
        "D-5439":"Fruit & Veg Counter",
        "D-5441":"Health & Beauty",
        "D-5442":"Healthy Section",
        "D-5443":"Home & Entertainment",
        "D-5444":"Household",
        "D-5429":"Others Not Shown On Website",
        "D-5445":"Pets",
        "D-5446":"Tobacco",
    }

    myscraper = MyScraper()

    def start_requests(self):
        for cat_id, cat_name in self.categories.items():

            url_name = cat_name.replace(" ", "%20")

            start_url = f"https://welbees.mt/shop/category/{cat_id}/{url_name}"

            yield scrapy.Request(
                start_url,
                callback=self.parse,
                meta={
                    "category": cat_name,
                    "cat_id": cat_id,
                    "page": 1,
                    "prev_first_code": None
                }
            )

    def parse(self, response):
        category = response.meta["category"]
        cat_id = response.meta["cat_id"]
        page = response.meta["page"]
        prev_first_code = response.meta["prev_first_code"]

        codes = response.css("[data-product-code]::attr(data-product-code)").getall()

        if not codes:
            return 

        current_first_code = codes[0]

        if prev_first_code == current_first_code:
            print(f"STOP: Category {category} reached duplicate page at page {page}")
            return

        for code in codes:
            data = self.myscraper.get_api_data(code)
            if not data:
                continue

            item = PricelyItem()
            item["name"] = data.get("description")
            item["image"] = data.get("image")
            item["price"] = data.get("selling_price_value")
            item["vendor"] = self.vendor
            item["category"] = category
            item["url"] = response.url

            yield item

        next_page_num = page + 1
        next_page_url = f"https://welbees.mt/shop/category/{cat_id}/{category.replace(' ', '%20')}?page={next_page_num}"

        yield scrapy.Request(
            next_page_url,
            callback=self.parse,
            meta={
                "category": category.tit,
                "cat_id": cat_id,
                "page": next_page_num,
                "prev_first_code": current_first_code
            }
        )
