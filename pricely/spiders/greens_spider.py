import scrapy
from bs4 import BeautifulSoup
from pricely.items import PricelyItem
import requests
import re

class ScraperSpider(scrapy.Spider):
    name = "greens_spider"
    categories_list = [
        "butcher",
        "bakery",
        "Baby",
        "chilledanddairy",
        "beverages",
        "fruitsandvegetables",
        "Confectionery",
        "fish",
        "Cosmetic",
        "Cosmetics",
        "Costetic",
        "Delicatessen",
        "flowersandplants",
        "frozenfoods",
        "Groceries",
        "pets",
        "winecellar",
        "Household",
        "homegarden",
        "health",
        "new"

    ]
    NumberOfRecords = 1000

    def get_image_url_and_item_url(self, html_content):
        self.items = {}
        self.soup = BeautifulSoup(html_content, "html.parser")
        self.img_tag = self.soup.find("img")
        self.item_url = self.soup.find("a")

        if self.img_tag and 'src' in self.img_tag.attrs:
            self.items['image_url'] = f"https://www.greens.com.mt{self.img_tag['src']}"
        if self.item_url and 'href' in self.item_url.attrs:
            self.items['item_url'] = f"https://www.greens.com.mt{self.item_url['href']}"
        return self.items

    def get_text_from_html(self, html_content):
        self.soup = BeautifulSoup(html_content, "html.parser")
        return self.soup.get_text(strip=True)
    
    def get_token(self, categories):
        session = requests.Session()

        # 1. Load page to get token
        url = f"https://www.greens.com.mt/products?cat={categories}"
        html = session.get(url).text

        # 2. Extract token
        pattern = r"productDisplay\('.*?', '.*?', '([^']+)'"
        match = re.search(pattern, html)

        if not match:
            raise Exception("Token not found")

        token = match.group(1)
        return token
    
    def start_requests(self):
        for cat in self.categories_list:
            yield scrapy.Request(
            url=f"https://www.greens.com.mt/apiservices/retail/sync/productlist?Agent=GREENS&Loc=SM&Eid=N/A&SearchCriteria=&page=1&NumberOfRecords={self.NumberOfRecords}&SortType=Position&SortDirection=Asc&Category={cat}&Category2=&Category3=&Type=&Cid=00000000-0000-0000-0000-000000000000&Cart=d6aa2afb-3ed2-4530-bde3-d7a7d1e12f95&SubType=&Brand=&ProductListType=products&Mobdev=False&Detailed=True",
            headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "authorization": f"Bearer {self.get_token(cat)}",
            "content-type": "application/json",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Chromium\";v=\"142\", \"Google Chrome\";v=\"142\", \"Not_A Brand\";v=\"99\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-requested-with": "XMLHttpRequest",
            "cookie": "ASP.NET_SessionId=ojjxztd2aorijnqoncfkhspo; _ga=GA1.3.289875259.1765282211; _gid=GA1.3.507632845.1765282211; _ga_957JM1FKEY=GS2.3.s1765380847$o4$g1$t1765380883$j24$l0$h0; _gat=1",
            "Referer": "https://www.greens.com.mt/products?cat=butcher"
            },
            callback=self.parse
        )

    def parse(self, response):
        self.api_data = response.json()
        self.vendor_tag = "Greens"

        for product in self.api_data['ProductList']:
            item  = PricelyItem()
            item['name'] = product["ProductDetails"]['PART_DESCRIPTION']
            product_image_and_url = self.get_image_url_and_item_url(product['Image'])
            if not product_image_and_url:
                product_image_and_url = {}
            item['image'] = product_image_and_url.get('image_url', '')
            item['url'] = product_image_and_url.get('item_url', '')

            item['price'] = product["ProductDetails"]['SALES_PRICE']
            item['category'] = product["ProductDetails"]["GROUP_1"]
            item['vendor'] = self.vendor_tag

            yield item