from itemadapter import ItemAdapter
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import re
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from pricely.settings import EMBEDDING_MODEL, MATCH_THRESHOLD

cat = {
    # ================= BUTCHER / MEAT =================
    (
        "butcher", "butcher counter", "meat",
        "bacon", "burgers bbq meat", "pork", "sausages",
        "prepack hams cheese antipasti", "corned beef chopped ham"
    ): "butcher",

    # ================= BAKERY =================
    (
        "bakery", "bread", "cakes", "pastries",
        "fresh maltese bread", "sliced breads buns rolls",
        "fresh baked breads", "dietary gluten free bread",
        "baked croissants pastry savouries",
        "traditional maltese cakes",
        "traditional maltese confectionery",
        "wrapped cakes", "confectionary croissants sponges",
        "donuts", "muffins",
        "naan pitta bread", "wraps tortillas", "croutons"
    ): "bakery",

    # ================= BABY =================
    (
        "baby",
        "baby dummies soothers", "baby bottles",
        "baby feeding utensils", "baby cleaning accessories",
        "baby mum to be", "baby healthcare accessories",
        "baby infant milk", "baby cereals",
        "baby rusks biscuits", "baby meat veg jars",
        "baby snacks", "baby fruit jars",
        "baby food fruit pouches", "baby pasta",
        "baby nappies bed pads", "baby pullup swim nappies",
        "baby wash skin care", "baby laundry",
        "baby wipes", "baby cotton wool buds sponges"
    ): "baby products",

    # ================= CHILLED & DAIRY =================
    (
        "chilledanddairy", "chilled food", "dairy",
        "butter margarine spreads", "cheese", "cream",
        "eggs", "milk", "yoghurts",
        "cheese cream spreads",
        "desserts snacks", "cream desserts",
        "pizza pasta sauces", "pate dips dressing antipasto",
        "fresh pastry"
    ): "chilled and dairy",

    # ================= BEVERAGES =================
    (
        "beverages", "drinks",
        "soft drinks", "soft drinks packs",
        "energy drinks", "sports drinks",
        "vitamin herbal drinks",
        "organic drinks",
        "ice tea",
        "ready made milkshake yogurt",
        "chilled smoothies ice coffee",
        "chilled fresh fruit juice",
        "organic juice", "long life juice",
        "fruit milk squashes",
        "water flavoured", "water sparkling",
        "water still", "water packs"
    ): "beverages",

    # ================= WINE & ALCOHOL =================
    (
        "winecellar", "wine", "beers", "ciders",
        "beers lagers", "bitters stouts",
        "cider", "non alcoholic beers ciders",
        "brandy cognac", "gin", "liqueurs",
        "mixers", "sherry port", "vodka",
        "ready to serve", "tequila other spirits",
        "rum", "whisky", "vermouth",
        "champagne sparkling wine", "dessert wine",
        "red wine", "rose wine", "white wine",
        "non alcoholic wines",
        "non alcoholic aperitifs"
    ): "wine and alcohol",

    # ================= FRUIT & VEGETABLES =================
    (
        "fruitsandvegetables", "fruit & veg counter",
        "fresh fruit", "prepared fruit veg",
        "salads prewashed", "vegetables"
    ): "fruit and veg",

    # ================= FLOWERS & PLANTS =================
    (
        "flowersandplants", "flowers", "plants"
    ): "flowers and plants",

    # ================= DELICATESSEN =================
    (
        "delicatessen", "deli", "deli counter",
        "deli cheese", "deli hams",
        "deli savouries olives",
        "prepack hams cheese antipasti"
    ): "food and delicatessen",

    # ================= HEALTH & SAFETY =================
    (
        "anti flu", "anti-flu",
        "face masks", "sanitizers",
        "hand sanitizers",
        "disinfectants", "multi surface wipes",
        "personal wipes"
    ): "health and safety",

    # ================= CONFECTIONERY =================
    (
        "confectionery", "sweets", "chocolate",
        "biscuits", "wafers", "crackers",
        "chewing gum", "chocolates",
        "traditional sweets"
    ): "confectionery",

    # ================= CLOTHING & FOOTWEAR =================
    (
        "clothing", "clothes", "clothes & accessories",
        "footwear", "shoes",
        "slippers", "sandals",
        "socks", "tights", "garments"
    ): "clothing and footwear",

    # ================= FISH & SEAFOOD =================
    (
        "fish", "seafood", "fish counter",
        "fish seafood tins",
        "caviar salmon fish",
        "fish seafood counter"
    ): "fish and seafood",

    # ================= GROCERIES / FOOD CUPBOARD =================
    (
        "groceries", "food cupboard",
        "pasta", "rice", "noodles",
        "pasta regular", "pasta special diet",
        "rice regular", "rice special diet",
        "noodles regular", "noodles pot noodles",
        "cous cous",
        "tins", "jars",
        "fruit tins", "vegetables tins jars",
        "tomatoes tins jars",
        "oil vinegar", "condiments", "sauces",
        "ready to serve sauces chutneys",
        "gravy stock cubes",
        "herbs spices salt",
        "baking mixes", "flour yeast baking powder",
        "sugar", "icing sugar", "brown sugar", "sweeteners",
        "honey spreads nutella", "jams marmalades",
        "dried fruit", "nuts", "seeds",
        "dried pulses grains",
        "oriental", "indian", "mexican",
        "arabic", "russian eastern europe", "nepalese"
    ): "groceries",

    # ================= FROZEN =================
    (
        "frozenfoods", "frozen", "frozen food",
        "ice cream", "ice cream packs tubs",
        "ice cream single units",
        "frozen burgers sausages", "frozen meat",
        "frozen poultry game",
        "frozen fish seafood",
        "frozen ready meals",
        "frozen pizza bread",
        "frozen pastizzi pies",
        "frozen ravioli tortellini",
        "frozen pastry sheets pizza bases",
        "frozen potatoes fries chips",
        "frozen fruit veg",
        "frozen vegetarian gluten free"
    ): "foods and frozen foods",

    # ================= HEALTH & BEAUTY =================
    (
        "cosmetic", "cosmetics", "costetic",
        "health & beauty", "personal care",
        "beauty", "skincare", "haircare",
        "oral care", "soap", "handwash",
        "feminine care", "shaving",
        "toothpaste", "toothbrushes",
        "mouthwash", "dental floss",
        "bodywash bath foams",
        "hair shampoos", "hair conditioners",
        "hair colourants masks",
        "styling products",
        "mens skincare", "mens shower gel",
        "female body sprays perfumes",
        "liners accessories intimate care",
        "towels tampons"
    ): "health and beauty",

    # ================= HOUSEHOLD & CLEANING =================
    (
        "household",
        "cleaning", "laundry",
        "dishwasher tabs rinse aid",
        "dishwashing liquids",
        "cloths sponges dusters",
        "rubber gloves",
        "bleach disinfectants",
        "wc cleaners", "bathroom cleaners",
        "floor cleaners", "kitchen cleaners",
        "window cleaners",
        "fabric conditioners",
        "laundry washing powder",
        "laundry washing liquid gel",
        "bin liners", "kitchen roll",
        "toilet roll", "tissues",
        "air fresheners room sprays"
    ): "household",

    # ================= PETS =================
    (
        "pets", "cat", "dog",
        "cat litter", "cat canned food",
        "cat dry food treats",
        "cat pouches trays",
        "dog accessories toys",
        "dog shampoo conditioner",
        "dog canned food",
        "dog dry food treats",
        "dog pouches trays",
        "frozen dog food"
    ): "pets",

    # ================= HOME & GARDEN =================
    (
        "homegarden", "home & entertainment",
        "homeware", "kitchenware", "utensils",
        "bath towels face cloths",
        "ironmongery tools"
    ): "home and garden",

    # ================= HEALTH / WELLNESS =================
    (
        "health", "healthy section",
        "dietary", "free from",
        "gluten free", "organic",
        "fitness", "vitamins",
        "supplements", "speciality fitness",
        "vitamins food supplements"
    ): "health and wellness",

    # ================= SEASONAL / NEW =================
    (
        "new", "seasonal",
        "halloween", "halloween chocolates cakes",
        "christmas", "christmas hampers",
        "christmas panettone pandoro",
        "christmas wines spirits",
        "christmas chocolates candy",
        "christmas toys board games",
        "christmas table tree decorations"
    ): "seasonal and new",

    # ================= STATIONERY & PARTY =================
    (
        "stationery", "office",
        "party supplies", "party occasions items",
        "batteries", "usb cables",
        "crafts", "toys"
    ): "stationery and party",

    # ================= TOBACCO =================
    (
        "tobacco", "cigarettes", "lighters"
    ): "tobacco",

    # ================= FALLBACK =================
    ("Others Not Shown On Website",): "others",
}

class PricelyPipeline:

    def slugify(self, value):
        """Convert product name into a safe ID string."""
        value = value.lower().strip()
        value = re.sub(r"[^a-z0-9]+", "-", value)
        return value.strip("-")

    def open_spider(self, spider):
        self.client = MongoClient(spider.settings.get("MONGO_URI"))
        self.db = self.client[spider.settings.get("MONGO_DATABASE")]
        self.store_products = self.db["store_products"]
        self.canonical_products = self.db["canonical_products"]

        self.embedding_model = spider.settings.get("EMBEDDING_MODEL")
        self.match_threshold = spider.settings.get("MATCH_THRESHOLD")
        self.CATEGORY_MAP = cat
        self.model = SentenceTransformer(self.embedding_model)


        # Unique product index
        self.canonical_products.create_index([("category", ASCENDING)])
        self.store_products.create_index([("canonical_product_id", ASCENDING)])
        self.store_products.create_index([("store", ASCENDING)])
        self.store_products.create_index("vendor")
        self.store_products.create_index("product_id", unique=True)

    def close_spider(self, spider):
        self.client.close()

    def normalize(self, text: str) -> str:
        self.text = text.lower()
        self.text = re.sub(r"[^a-z0-9 ]", " ", self.text)
        self.text = re.sub(r"\s+", " ", self.text).strip()
        return self.text
    
    def embed_text(self, text: str) -> np.ndarray:
        self.text = self.model.encode(text)
        return self.text
    
    def categorize(self, product):
        self.category = str(product.get("category")).lower()
        for keys, values in self.CATEGORY_MAP.items():
            if self.category in (k.lower() for k in keys):
                return values.lower()
            return "others"

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        name_slug = self.slugify(adapter.get("name"))
        vendor_slug = self.slugify(adapter.get("vendor"))

        product_id = f"{vendor_slug}-{name_slug}"

        self.category = self.categorize(adapter)
        self.text = self.normalize(adapter.get("name", "") + " " + self.category)

        self.result = self.find_best_match(self.text, self.category)
        if self.result[0]:  # Matched
            self.canonical_id = self.result[0]
            self.confidence = self.result[1]
        else:  # No match → create
            self.canonical_id = self.canonical_products.insert_one({
                "name": adapter["name"],
                "normalized_name": self.normalize(adapter["name"]),
                "category": self.category,
                "embedding": self.result[2].tolist(),
                "source": adapter.get("vendor"),
                "scraped_at":datetime.utcnow()

            }).inserted_id
            self.confidence = 1.0

        adapter["product_id"] = product_id
        adapter["canonical_product_id"] = self.canonical_id
        adapter["scraped_at"] = datetime.utcnow()
        adapter["confidence"] = self.confidence
        adapter["category"] = self.category

        self.store_products.update_one(
            {"product_id": product_id},
            {"$set": adapter.asdict()},
            upsert=True
        )

        return adapter

    def find_best_match(self, text, category):
        self.store_vec = self.embed_text(text)

        candidates = list(self.canonical_products.find({"category": category}))
        self.best_score = 0
        self.best_canonical = None

        for c in candidates:
            embedding = np.array(c["embedding"], dtype=np.float32)
            self.score = cosine_similarity([self.store_vec], [embedding])[0][0]
            if self.score > self.best_score:
                self.best_score = self.score
                self.best_canonical = c

        if self.best_score >= self.match_threshold:
            return self.best_canonical["_id"], float(self.best_score)

        return None, self.best_score, self.store_vec