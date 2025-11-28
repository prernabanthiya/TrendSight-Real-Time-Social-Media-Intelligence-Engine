import requests
import pymongo
import time

# ----------------------------
# YOUR BEARER TOKEN (paste here)
# ----------------------------
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAPWm5gEAAAAADwP%2FB7PLYMFIXPhDRRQs6H%2FZ8s0%3DtzVMW3YNSt9YXVVUNfPnLul508adyligbaIZHFLIsczFUfbNM2"

# ---------- MongoDB Connection ----------
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["twitter_etl"]
collection = db["raw_tweets"]

# ---------- Twitter API URL --------------
search_query = "data engineering"   # change keyword if needed
url = f"https://api.twitter.com/2/tweets/search/recent?query={search_query}&tweet.fields=author_id,created_at,public_metrics&max_results=50"

headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

while True:
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        tweets = response.json().get("data", [])
        if tweets:
            for tweet in tweets:
                collection.insert_one(tweet)
                print("Inserted Tweet:", tweet["id"])
        else:
            print("No new tweets found.")
    else:
        print("Error:", response.status_code, response.text)

    time.sleep(20)  # fetch every 20 seconds
