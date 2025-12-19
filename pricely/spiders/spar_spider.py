import scrapy
from bs4 import BeautifulSoup
from pricely.items import PricelyItem
import requests
import re

class ScraperSpider(scrapy.Spider):
    name = "spar_spider"
    categories_list = {
    # Baby - Accessories
    "baby dummies soothers": "GRP-G0001",
    "baby bottles": "GRP-G0002",
    "baby feeding utensils": "GRP-G0003",
    "baby cleaning accessories": "GRP-G0004",
    "baby mum to be": "GRP-G0005",
    "baby healthcare accessories": "GRP-G302",
    
    # Baby - Baby Food & Drinks
    "baby infant milk": "GRP-G0006",
    "baby cereals": "GRP-G0007",
    "baby rusks biscuits": "GRP-G0008",
    "baby meat veg jars": "GRP-G0009",
    "baby snacks": "GRP-G0010",
    "baby fruit jars": "GRP-G0011",
    "baby food fruit pouches": "GRP-G0012",
    "baby pasta": "GRP-G0013",
    
    # Baby - Nappies
    "baby nappies bed pads": "GRP-G0014",
    "baby pullup swim nappies": "GRP-G0015",
    
    # Baby - Baby Care
    "baby wash skin care": "GRP-G0016",
    "baby laundry": "GRP-G0017",
    "baby wipes": "GRP-G0018",
    "baby cotton wool buds sponges": "GRP-G0019",
    
    # Anti-Flu Products
    "disinfectants": "GRP-G331",
    "face masks": "GRP-G333",
    "multi surface wipes": "GRP-G330",
    
    # Bakery - Bread
    "fresh maltese bread": "GRP-G0020",
    "sliced breads buns rolls": "GRP-G0021",
    "fresh baked breads": "GRP-G0022",
    "dietary gluten free bread": "GRP-G0023",
    
    # Bakery - Cakes, Pastries & Bakes
    "baked croissants pastry savouries": "GRP-G0024",
    "traditional maltese cakes": "GRP-G0025",
    "traditional maltese confectionery": "GRP-G0026",
    "wrapped cakes": "GRP-G0028",
    "confectionary croissants sponges": "GRP-G0029",
    "donuts": "GRP-G340",
    "muffins": "GRP-G339",
    
    # Bakery - Wraps, Pita & Naan
    "naan pitta bread": "GRP-G0030",
    "croutons": "GRP-G0031",
    "wraps tortillas": "GRP-G0032",
    
    # Drinks - Beers & Ciders
    "beers lagers": "GRP-G0033",
    "bitters stouts": "GRP-G0034",
    "cider": "GRP-G0035",
    "non alcoholic beers ciders": "GRP-G335",
    
    # Drinks - Smoothies & Juice
    "chilled smoothies ice coffee": "GRP-G0037",
    "chilled fresh fruit juice": "GRP-G0038",
    "organic juice": "GRP-G0039",
    "long life juice": "GRP-G0040",
    
    # Drinks - Other
    "ice tea": "GRP-G0041",
    "ready made milkshake yogurt": "GRP-G0042",
    "soft drinks": "GRP-G0043",
    "soft drinks packs": "GRP-G0043B",
    "energy drinks": "GRP-G0044",
    "fruit milk squashes": "GRP-G0045",
    "vitamin herbal drinks": "GRP-G0046",
    "organic drinks": "GRP-G348",
    "sports drinks": "GRP-G320",
    
    # Drinks - Spirits & Liqueurs
    "brandy cognac": "GRP-G0047",
    "gin": "GRP-G0048",
    "liqueurs": "GRP-G0049",
    "mixers": "GRP-G0050",
    "sherry port": "GRP-G0051",
    "vodka": "GRP-G0052",
    "ready to serve": "GRP-G0053",
    "tequila other spirits": "GRP-G0054",
    "rum": "GRP-G0055",
    "whisky": "GRP-G0056",
    "vermouth": "GRP-G0057",
    "non alcoholic aperitifs": "GRP-G0321",
    
    # Drinks - Water
    "water flavoured": "GRP-G0058",
    "water sparkling": "GRP-G0059",
    "water still": "GRP-G0060",
    "water packs": "GRP-G0060B",
    
    # Drinks - Wine
    "champagne sparkling wine": "GRP-G0061",
    "dessert wine": "GRP-G0062",
    "red wine": "GRP-G0064",
    "rose wine": "GRP-G0065",
    "white wine": "GRP-G0067",
    "non alcoholic wines": "GRP-G336",
    
    # Food Cupboard - Baking & Spreads
    "cake decorations": "GRP-G0068",
    "baking mixes": "GRP-G0069",
    "cooking chocolate": "GRP-G0070",
    "flour yeast baking powder": "GRP-G0071",
    "home baking essences colouring": "GRP-G0072",
    "honey spreads nutella": "GRP-G0073",
    "jams marmalades": "GRP-G356",
    "jelly toppings": "GRP-G0074",
    "tinned milk panna custards": "GRP-G0075",
    
    # Food Cupboard - Breakfast
    "cereal bars": "GRP-G0076",
    "hot cereals oats porridge": "GRP-G0077",
    "traditional cereals": "GRP-G0078",
    
    # Food Cupboard - Tea & Coffee
    "fruit herbal organic tea": "GRP-G0079",
    "decaf classic teas": "GRP-G0080",
    "coffee beans ground instant": "GRP-G0081",
    "coffee sachets pods": "GRP-G0082",
    "decaf coffee": "GRP-G0083",
    "hot chocolate flavoured drinks": "GRP-G0084",
    "malted drinks": "GRP-G0085",
    
    # Food Cupboard - Condiments & Sauces
    "ready to serve sauces chutneys": "GRP-G0086",
    "besciamella creams pesto sugo": "GRP-G0087",
    "gravy stock cubes": "GRP-G0088",
    "herbs spices salt": "GRP-G0089",
    "oil vinegar": "GRP-G0090",
    "cooking sauce sachets": "GRP-G358",
    
    # Food Cupboard - Dried Fruit, Nuts & Pulses
    "seeds": "GRP-G0091",
    "dried fruit": "GRP-G0092",
    "nuts": "GRP-G0093",
    "dried pulses grains": "GRP-G0094",
    
    # Food Cupboard - World Foods
    "oriental": "GRP-G0096",
    "indian": "GRP-G0097",
    "mexican": "GRP-G0098",
    "arabic": "GRP-G301",
    "russian eastern europe": "GRP-G323",
    "nepalese": "GRP-G357",
    
    # Food Cupboard - Long-Life Milk
    "dairy longlife": "GRP-G0099",
    "non dairy milk": "GRP-G0100",
    
    # Food Cupboard - Health & Dietary
    "organic range": "GRP-G0101",
    "gluten free": "GRP-G350",
    
    # Food Cupboard - Pasta, Rice, Noodles
    "cous cous": "GRP-G0102",
    "noodles regular": "GRP-G0104",
    "pasta regular": "GRP-G0105",
    "pasta special diet": "GRP-G0106",
    "rice regular": "GRP-G0107",
    "rice special diet": "GRP-G0108",
    "noodles pot noodles": "GRP-G321",
    
    # Food Cupboard - Other
    "pate spreads": "GRP-G0109",
    "olives": "GRP-G0110",
    "capers": "GRP-G324",
    "relish chutneys pickles": "GRP-G322",
    
    # Food Cupboard - Biscuits & Snacks
    "biscuits": "GRP-G0111",
    "chewing gum": "GRP-G0112",
    "chocolates": "GRP-G0113",
    "crackers crispbreads": "GRP-G0114",
    "savoury snacks": "GRP-G0115",
    "sweets": "GRP-G0116",
    "traditional sweets": "GRP-G0117",
    "wafers": "GRP-G351",
    
    # Food Cupboard - Fitness & Health
    "speciality fitness": "GRP-G0118",
    "vitamins food supplements": "GRP-G325",
    
    # Food Cupboard - Sugar & Sweeteners
    "baking preserving sugar": "GRP-G0119",
    "brown sugar": "GRP-G0120",
    "icing sugar": "GRP-G0121",
    "sugar": "GRP-G0122",
    "sweeteners": "GRP-G0123",
    
    # Food Cupboard - Soups & Instant
    "instant snacks soups": "GRP-G0124",
    
    # Food Cupboard - Tins & Jars
    "mashed potatoes": "GRP-G0125",
    "baked beans tins": "GRP-G0126",
    "fish seafood tins": "GRP-G0127",
    "fruit tins": "GRP-G0128",
    "corned beef chopped ham": "GRP-G0129",
    "tomatoes tins jars": "GRP-G0130",
    "vegetables tins jars": "GRP-G0131",
    
    # Stationery & Party
    "batteries": "GRP-G0132",
    "stationery": "GRP-G0133",
    "lighters": "GRP-G0138",
    "party occasions items": "GRP-G311",
    "usb cables": "GRP-G390",
    "toys": "GRP-G397",
    
    # Fresh - Chilled Desserts
    "desserts snacks": "GRP-G0139",
    "cream desserts": "GRP-G0140",
    
    # Fresh - Chilled Food
    "pizza pasta sauces": "GRP-G0141",
    "pate dips dressing antipasto": "GRP-G0142",
    "fresh pastry": "GRP-G0154",
    
    # Fresh - Dairy, Eggs & Cheese
    "butter margarine spreads": "GRP-G0148",
    "cheese": "GRP-G0149",
    "cream": "GRP-G0150",
    "eggs": "GRP-G0151",
    "milk": "GRP-G0153",
    "yoghurts": "GRP-G0155",
    "cheese cream spreads": "GRP-G361",
    
    # Fresh - Fruit & Veg
    "fresh fruit": "GRP-G0156",
    "prepared fruit veg": "GRP-G0158",
    "salads prewashed": "GRP-G0172",
    "vegetables": "GRP-G0176",
    
    # Fresh - Meat, Fish & Poultry
    "bacon": "GRP-G0159",
    "burgers bbq meat": "GRP-G0161",
    "prepack hams cheese antipasti": "GRP-G0163",
    "caviar salmon fish": "GRP-G0164",
    "pork": "GRP-G0166",
    "sausages": "GRP-G0167",
    "vegetarian food": "GRP-G0168",
    "prepared meals": "GRP-G0169",
    
    # Fresh - Counters
    "fish seafood counter": "GRP-G313",
    "butcher counter": "GRP-G367",
    "deli cheese": "GRP-G370",
    "deli hams": "GRP-G371",
    "deli savouries olives": "GRP-G372",
    
    # Frozen - Ice Cream
    "ice cream packs tubs": "GRP-G0177",
    "ice cream single units": "GRP-G0178",
    
    # Frozen - Meat & Poultry
    "frozen burgers sausages": "GRP-G0179",
    "frozen meat": "GRP-G0180",
    "frozen poultry game": "GRP-G0181",
    "frozen fish seafood": "GRP-G0182",
    
    # Frozen - Pizza & Meals
    "frozen ready meals": "GRP-G0185",
    "frozen pizza bread": "GRP-G0186",
    "frozen pastizzi pies": "GRP-G0187",
    "frozen ravioli tortellini": "GRP-G0188",
    "frozen pastry sheets pizza bases": "GRP-G0189",
    
    # Frozen - Veg & Fruit
    "frozen potatoes fries chips": "GRP-G0190",
    "frozen fruit veg": "GRP-G0191",
    "frozen food of world": "GRP-G0192",
    "frozen vegetarian gluten free": "GRP-G0193",
    
    # Personal Care - Beauty & Skincare
    "body moisturisers": "GRP-G0197",
    "cleansers toners scrubs": "GRP-G0198",
    "cotton wool buds": "GRP-G0199",
    "facial moisturisers": "GRP-G200",
    "hair removal": "GRP-G201",
    "razors razor blades": "GRP-G202",
    "hand skin care": "GRP-G203",
    "lipcare": "GRP-G204",
    "make up accessories": "GRP-G205",
    "nail care": "GRP-G206",
    "suncare": "GRP-G207",
    "talcum powder": "GRP-G208",
    "nail beauty accessories": "GRP-G326",
    
    # Personal Care - Female Deodorants
    "female body sprays perfumes": "GRP-G210",
    "female roll on": "GRP-G211",
    "female sticks creams wipes": "GRP-G212",
    
    # Personal Care - Feminine Care
    "liners accessories intimate care": "GRP-G213",
    "towels tampons": "GRP-G214",
    
    # Personal Care - Haircare
    "2in1 shampoo conditioner": "GRP-G215",
    "hair colourants masks": "GRP-G216",
    "hair conditioners": "GRP-G217",
    "hair accessories": "GRP-G218",
    "kids shampoo conditioners": "GRP-G219",
    "hair shampoos": "GRP-G220",
    "styling products": "GRP-G221",
    
    # Personal Care - First Aid
    "family planning": "GRP-G224",
    "first aid": "GRP-G225",
    "adult pants bed sheets": "GRP-G242",
    
    # Personal Care - Male
    "mens bodyspray fragrance": "GRP-G227",
    "mens roll on sticks": "GRP-G228",
    "mens hair products": "GRP-G229",
    "mens shower gel": "GRP-G230",
    "mens skincare": "GRP-G231",
    "shaving foam gel aftershave": "GRP-G233",
    
    # Personal Care - Oral Care
    "dental floss": "GRP-G234",
    "denture care": "GRP-G235",
    "mouthwash": "GRP-G237",
    "toothbrushes": "GRP-G238",
    "toothpaste": "GRP-G239",
    
    # Personal Care - Soap & Handwash
    "bodywash bath foams": "GRP-G240",
    "childrens bath products": "GRP-G241",
    "liquid handwash sanitizers": "GRP-G243",
    "soap": "GRP-G244",
    "personal wipes": "GRP-G245",
    
    # Cleaning - Dishwasher
    "dishwasher salt machine cleaner": "GRP-G246",
    "dishwasher tabs rinse aid": "GRP-G247",
    
    # Cleaning - Dishwashing
    "dishwashing liquids": "GRP-G248",
    
    # Cleaning - Cloths & Gloves
    "cloths sponges dusters": "GRP-G249",
    "rubber gloves": "GRP-G250",
    "scourers sponge": "GRP-G251",
    
    # Cleaning - Cleaners
    "bleach disinfectants": "GRP-G252",
    "wc cleaners": "GRP-G253",
    "bathroom cleaners": "GRP-G254",
    "floor cleaners": "GRP-G255",
    "kitchen cleaners": "GRP-G256",
    "window cleaners": "GRP-G257",
    "multi purpose descalers": "GRP-G297",
    
    # Cleaning - Laundry
    "delicate special wash": "GRP-G258",
    "fabric conditioners": "GRP-G259",
    "fabric fresheners": "GRP-G260",
    "laundry soap": "GRP-G261",
    "laundry washing powder": "GRP-G262",
    "laundry washing tablets": "GRP-G263",
    "laundry washing liquid gel": "GRP-G264",
    "laundry washing machine cleaner": "GRP-G265",
    "laundry washing additives": "GRP-G266",
    
    # Household
    "cling film food bags foils": "GRP-G268",
    "air fresheners room sprays": "GRP-G269",
    "bin liners bins": "GRP-G270",
    "firelighters fuel matches": "GRP-G271",
    "disposables crockery cutlery": "GRP-G272",
    "insect rodent killer": "GRP-G273",
    "shoe care foot care": "GRP-G274",
    "kitchen roll": "GRP-G275",
    "napkins": "GRP-G276",
    "tissues": "GRP-G277",
    "toilet roll": "GRP-G278",
    "brooms mops pegs": "GRP-G279",
    "carrier bags": "GRP-G337",
    
    # Pets - Cat
    "cat litter": "GRP-G281",
    "cat canned food": "GRP-G282",
    "cat dry food treats": "GRP-G283",
    "cat pouches trays": "GRP-G284",
    
    # Pets - Dog
    "dog accessories toys": "GRP-G285",
    "dog shampoo conditioner": "GRP-G287",
    "dog canned food": "GRP-G288",
    "dog dry food treats": "GRP-G289",
    "dog pouches trays": "GRP-G290",
    "frozen dog food": "GRP-G369",
    
    # Seasonal - Halloween
    "halloween chocolates cakes": "GRP-G0292",
    
    # Seasonal - Christmas
    "christmas panettone pandoro": "GRP-G293",
    "christmas wines spirits": "GRP-G299",
    "christmas accessories gifts": "GRP-G318",
    "christmas seasonal preserves": "GRP-G329",
    "christmas toys board games": "GRP-G342",
    "christmas table tree decorations": "GRP-G346",
    "christmas chocolates candy": "GRP-G359",
    "christmas hampers": "GRP-G360",
    "christmas lights electrical": "GRP-G368",
    
    # Homeware
    "homeware kitchenware utensils": "GRP-G303",
    "bath towels face cloths": "GRP-G343",
    "ironmongery tools": "GRP-G344",
    
    # Clothing & Footwear
    "slippers sandals": "GRP-G327",
    "socks tights garments": "GRP-G328",

}

    NumberOfRecords = 1000

    def start_requests(self):
        for category_name, category_id in self.categories_list.items():
            self.category = category_name
            self.url = f"https://shop.spar.com.mt/category.php?categoryid={category_id}"
            yield scrapy.Request(
            url=str(self.url),
            
            headers = {
            "User-Agent": "Mozilla/5.0"
            },
            callback=self.parse
        )

    def parse(self, response):
        self.vendor_tag = "Spar"

        soup = BeautifulSoup(response.text, "html.parser")
        container = soup.find_all("div", class_="product-slide-entry")
        for i in container:
            item  = PricelyItem()
            item["name"] = i.find("div", class_="title").get_text(strip=True)
            image_box = i.find("div", class_="product-image")
            item["image"] = image_box.img["src"]
            price_box = i.find("div", class_="price")
            price = price_box.find("div", class_="current").text
            match = re.search(r"(\d+[.,]\d+)", price)
            if match:
                item["price"] = match.group(1)
            else:
                raise Exception

            item['category'] = self.category
            item['vendor'] = self.vendor_tag
            item["url"] = self.url
            print(item)
            yield item


#content-block > div.content-center.fixed-header-margin > div.content-push > div.information-blocks > div > div.col-md-9.col-md-push-3.col-sm-8.col-sm-push-4 > div.row.shop-grid.grid-view > div:nth-child(1) > div.product-slide-entry > div.title
# /html/body/div[2]/div[1]/div[2]/div[2]/div/div[1]/div[2]/div[1]/div[1]/div[3]