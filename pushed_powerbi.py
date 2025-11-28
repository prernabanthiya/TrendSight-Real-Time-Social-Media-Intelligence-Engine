import time
import requests
from pymongo import MongoClient
from datetime import datetime, timezone

# 🔹 Power BI API URL
POWERBI_URL = "https://api.powerbi.com/beta/66a9c36a-befc-45c4-a5fc-0b87350e99b9/datasets/76ce5e90-931d-4e56-851b-ecb2dfd4d81e/rows?experience=power-bi&key=fHcCHKjuvMzzyBK9BW%2Bu0c2AetPGGBLlxvpB0K2diyBC%2BiddkRyQNkEiwo9ZyyeNA27XIsuJqx3XJR%2BSvz2shg%3D%3D"

# 🔹 MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["twitter_etl"]
collection = db["curated_trending_enriched"]

print("🚀 Starting real-time push to Power BI...")

while True:
    docs = list(collection.find().sort("last_updated", -1).limit(10))

    rows = []
    for d in docs:
        last_updated = d.get("last_updated", None)

        # Ensure proper datetime string format
        if isinstance(last_updated, datetime):
            last_updated = last_updated.astimezone(timezone.utc).isoformat()
        elif isinstance(last_updated, str):
            last_updated = last_updated  # already string, keep as is
        else:
            last_updated = datetime.now(timezone.utc).isoformat()

        rows.append({
            "topic": d.get("topic", ""),
            "mention_count": d.get("mention_count", 0),
            "total_engagement": d.get("total_engagement", 0),
            "avg_sentiment": d.get("avg_sentiment", 0),
            "trend_score": d.get("trend_score", 0),
            "velocity": d.get("velocity", 0),
            "last_updated": last_updated
        })

    response = requests.post(POWERBI_URL, json={"rows": rows})
    print(f"Pushed {len(rows)} rows | Status Code: {response.status_code}")

    time.sleep(5)
