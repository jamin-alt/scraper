from itemadapter import ItemAdapter
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import re
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from pricely.settings import EMBEDDING_MODEL, MATCH_THRESHOLD

cat = {
    # BUTCHER
    ("butcher", "butcher counter", "meat"): "butcher",
    # BAKERY
    ("bakery", "bread", "cakes", "pastries"): "bakery",
    # BABY
    ("baby",): "baby products",
    # CHILLED & DAIRY
    ("chilledanddairy", "chilled food", "dairy", "eggs", "cheese"): "chilled and dairy",
    # BEVERAGES
    ("beverages", "drinks", "soft drinks", "water", "juice", "energy drinks", "ice tea", "milkshake", "smoothies"): "beverages",
    # FRUIT & VEG
    ("fruitsandvegetables", "fruit & veg counter", "fruit", "vegetables", "salads"): "fruit and veg",
    # CONFECTIONERY
    ("confectionery", "sweets", "chocolate", "biscuits", "wafers", "crackers"): "confectionery",
    # FISH
    ("fish", "seafood", "fish counter"): "fish and seafood",
    # HEALTH & BEAUTY / COSMETICS
    ("cosmetic", "cosmetics", "costetic", "health & beauty", "personal care", "beauty", "skincare", "haircare", "oral care", "soap", "handwash", "feminine care", "shaving"): "health and beauty",
    # DELICATESSEN
    ("delicatessen", "deli", "deli counter", "prepack ham", "antipasti"): "food and delicatessen",
    # FLOWERS & PLANTS
    ("flowersandplants", "flowers", "plants"): "flowers and plants",
    # FROZEN
    ("frozenfoods", "frozen", "foods", "frozen food", "ice cream", "desserts"): "foods and frozen foods",
    # GROCERIES
    ("groceries", "food cupboard", "pasta", "rice", "noodles", "tins", "jars", "oil", "vinegar", "condiments", "sauces", "cooking ingredients", "baking", "sugar", "flour", "herbs", "spices", "world foods"): "groceries",
    # PETS
    ("pets", "cat", "dog", "pet food", "pet accessories"): "pets",
    # WINE & ALCOHOL
    ("winecellar", "wine", "beers", "ciders", "spirits", "liqueurs"): "wine and alcohol",
    # HOUSEHOLD
    ("household", "cleaning", "laundry", "toilet roll", "kitchen roll", "paper products", "bin liners", "air fresheners"): "household",
    # HOME & GARDEN
    ("homegarden", "home & entertainment", "homeware", "kitchenware", "utensils"): "home and garden",
    # HEALTH
    ("health", "healthy section", "dietary", "free from", "gluten free", "organic", "fitness", "vitamins", "supplements"): "health and wellness",
    # NEW / SEASONAL
    ("new", "christmas hampers", "christmas", "seasonal", "halloween"): "seasonal and new",
    # STATIONERY & PARTY
    ("stationery", "office", "party supplies", "batteries", "crafts", "toys"): "stationery and party",
    # CLOTHING & FOOTWEAR
    ("clothing", "footwear", "socks", "tights", "slippers", "sandals"): "clothing and footwear",
    # ANTI-FLU / HEALTH SAFETY
    ("anti-flu", "disinfectants", "face masks", "wipes", "sanitizers"): "health and safety",
    # OTHER
    ("tobacco", "cigarettes", "lighters"): "tobacco",
    ("Others Not Shown On Website",): "others"
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
        print(f"Category: {self.category}")
        for keys, values in self.CATEGORY_MAP.items():
            if self.category in (k.lower() for k in keys):
                return values.lower()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        name_slug = self.slugify(adapter.get("name"))
        vendor_slug = self.slugify(adapter.get("vendor"))

        product_id = f"{vendor_slug}-{name_slug}"

        self.category = self.categorize(adapter)
        print(self.category)
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
            print(f"SCORE: {self.score}")
            if self.score > self.best_score:
                self.best_score = self.score
                self.best_canonical = c

        if self.best_score >= self.match_threshold:
            return self.best_canonical["_id"], float(self.best_score)

        return None, self.best_score, self.store_vec