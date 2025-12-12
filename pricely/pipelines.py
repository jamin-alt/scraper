from itemadapter import ItemAdapter
from pymongo import MongoClient
from datetime import datetime
import re


class PricelyPipeline:

    def slugify(self, value):
        """Convert product name into a safe ID string."""
        value = value.lower().strip()
        value = re.sub(r"[^a-z0-9]+", "-", value)
        return value.strip("-")

    def open_spider(self, spider):
        self.client = MongoClient(spider.settings.get("MONGO_URI"))
        self.db = self.client[spider.settings.get("MONGO_DATABASE")]
        self.collection = self.db["products"]

        # Unique product index
        self.collection.create_index("product_id", unique=True)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        name_slug = self.slugify(adapter.get("name"))
        vendor_slug = self.slugify(adapter.get("vendor"))

        product_id = f"{vendor_slug}-{name_slug}"

        adapter["product_id"] = product_id
        adapter["scraped_at"] = datetime.utcnow()

        self.collection.update_one(
            {"product_id": product_id},
            {"$set": adapter.asdict()},
            upsert=True
        )

        return item