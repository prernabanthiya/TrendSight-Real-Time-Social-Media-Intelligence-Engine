import pymongo
import pandas as pd
from textblob import TextBlob

# ---------------- MongoDB Connection ----------------
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["twitter_etl"]
raw_collection = db["raw_tweets"]
curated_collection = db["curated_trending"]

# ---------------- Fetch raw tweets ----------------
tweets_cursor = raw_collection.find()
tweets_list = list(tweets_cursor)

if not tweets_list:
    print("No tweets found in raw_tweets collection.")
    exit()

# ---------------- Convert to DataFrame ----------------
df = pd.DataFrame(tweets_list)

# Ensure these columns exist
df['text'] = df['text'].astype(str)
df['created_at'] = pd.to_datetime(df['created_at'])

# ---------------- Sentiment Analysis ----------------
df['sentiment'] = df['text'].apply(lambda x: TextBlob(x).sentiment.polarity)

# ---------------- Extract hashtags ----------------
df['hashtags'] = df['text'].str.findall(r"#(\w+)")

# ---------------- Aggregate Trending Topics ----------------
hashtags_exploded = df.explode('hashtags')
trending = hashtags_exploded.groupby('hashtags').agg({
    'text': 'count',
    'sentiment': 'mean'
}).reset_index()

trending = trending.rename(columns={
    'text': 'mention_count',
    'sentiment': 'avg_sentiment'
})

# Sort by mentions to get top trending
trending = trending.sort_values(by='mention_count', ascending=False).head(20)

# ---------------- Load into curated_trending ----------------
curated_collection.delete_many({})  # clear old data
curated_collection.insert_many(trending.to_dict('records'))

print("Curated trending topics loaded into MongoDB successfully!")