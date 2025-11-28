# transform_enriched.py
import re
import json
import time
import pymongo
from textblob import TextBlob
from langdetect import detect
import emoji
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import spacy
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import math
from pymongo import ReplaceOne

# Initialize NLP tools
nlp = spacy.load("en_core_web_sm")
lemmatizer = WordNetLemmatizer()
STOPWORDS = set(stopwords.words("english"))

# MongoDB config
MONGO_URI = "mongodb://localhost:27017/"
DB = "twitter_etl"
RAW = "raw_tweets"
CURATED = "curated_trending_enriched"

client = pymongo.MongoClient(MONGO_URI)
db = client[DB]
raw_col = db[RAW]
curated_col = db[CURATED]


# ------------------- Helper functions ------------------- #

def clean_text(text):
    if not text:
        return ""
    text = re.sub(r"http\S+|www.\S+", "", text)  # remove URLs
    text = emoji.replace_emoji(text, replace="")  # remove emojis
    text = re.sub(r"@\w+", "", text)  # remove mentions
    text = re.sub(r"[^\w\s#]", " ", text)  # remove punctuation but keep hashtags
    text = re.sub(r"\s+", " ", text).strip()  # normalize spaces
    return text


def extract_hashtags(text):
    return [h.lower() for h in re.findall(r"#(\w+)", text)]


def preprocess_tokens(text):
    tokens = nltk.word_tokenize(text.lower())
    tokens = [t for t in tokens if t.isalpha() and t not in STOPWORDS]
    tokens = [lemmatizer.lemmatize(t) for t in tokens]
    return tokens


def sentiment_score(text):
    try:
        return round(TextBlob(text).sentiment.polarity, 3)
    except Exception:
        return 0.0


def language_of_text(text):
    try:
        return detect(text)
    except Exception:
        return "unknown"


def is_possible_bot(meta):
    text = meta.get("text", "")
    if len(text) < 10 and any(ch.isdigit() for ch in meta.get("user_id", "")):
        return True
    return False


def compute_trend_velocity(topic_times):
    now = datetime.now(timezone.utc)
    window_recent = now - timedelta(minutes=5)
    window_past = now - timedelta(minutes=30)
    recent_count = sum(1 for t in topic_times if t >= window_recent)
    past_count = sum(1 for t in topic_times if window_past <= t < window_recent)
    if past_count == 0:
        return float(recent_count)
    else:
        return round((recent_count - past_count) / (past_count + 1), 3)


# ------------------- Main aggregation ------------------- #

def run_aggregation(window_minutes=5):
    print("Starting enriched transform... window:", window_minutes, "minutes")

    # fetch all raw tweets
    raw_cursor = raw_col.find({})
    docs = list(raw_cursor)
    if not docs:
        print("No raw docs found.")
        return

    # ensure each doc has a datetime object for aggregation
    for d in docs:
        try:
            if "ingest_time" in d:
                d["_ingest_dt"] = datetime.fromisoformat(d["ingest_time"].replace("Z", "+00:00"))
            elif "created_at" in d:
                d["_ingest_dt"] = datetime.fromisoformat(d["created_at"].replace("Z", "+00:00"))
            else:
                d["_ingest_dt"] = datetime.now(timezone.utc)
        except Exception:
            d["_ingest_dt"] = datetime.now(timezone.utc)

    # Filter to last 60 minutes
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=60)
    docs = [d for d in docs if d["_ingest_dt"] >= cutoff]

    # Aggregation containers
    topic_times = defaultdict(list)
    topic_counts = defaultdict(int)
    topic_engagement = defaultdict(int)
    topic_sentiments = defaultdict(list)
    topic_samples = defaultdict(list)

    for d in docs:
        text = d.get("text", "")
        cleaned = clean_text(text)
        hashtags = extract_hashtags(text)
        ts = d.get("_ingest_dt", datetime.now(timezone.utc))
        eng = 0
        pm = d.get("public_metrics") or {}
        eng = d.get("likes", 0) + 2 * d.get("comments", 0) + 3 * d.get("retweets", 0)
        if pm:
            eng = pm.get("like_count", 0) + 2 * pm.get("reply_count", 0) + 3 * pm.get("retweet_count", 0)
        sent = sentiment_score(cleaned)
        lang = language_of_text(cleaned) if cleaned else "unknown"

        for t in hashtags:
            topic_counts[t] += 1
            topic_engagement[t] += eng
            topic_sentiments[t].append(sent)
            topic_times[t].append(ts)
            if len(topic_samples[t]) < 5:
                topic_samples[t].append({
                    "text": cleaned,
                    "sent": sent,
                    "lang": lang,
                    "time": ts.isoformat()
                })

    # Prepare enriched documents
    enriched_docs = []
    for t, count in topic_counts.items():
        avg_sent = round(sum(topic_sentiments[t]) / len(topic_sentiments[t]), 3) if topic_sentiments[t] else 0.0
        velocity = compute_trend_velocity(topic_times[t])
        trend_score = round(count * (1 + math.log1p(topic_engagement[t])), 3)
        enriched_docs.append({
            "topic": f"#{t}",
            "mention_count": count,
            "total_engagement": topic_engagement[t],
            "avg_sentiment": avg_sent,
            "trend_score": trend_score,
            "velocity": velocity,
            "sample_tweets": topic_samples[t],
            "window_start": (datetime.now(timezone.utc) - timedelta(minutes=window_minutes)).isoformat() + "Z",
            "window_end": datetime.now(timezone.utc).isoformat() + "Z",
            "last_updated": datetime.now(timezone.utc).isoformat() + "Z"
        })

    # Upsert to curated collection
    bulk_ops = [ReplaceOne({"topic": doc["topic"]}, doc, upsert=True) for doc in enriched_docs]
    if bulk_ops:
        result = curated_col.bulk_write(bulk_ops)
        print("Upserted:", len(bulk_ops))
    else:
        print("No topics found in window.")


# ------------------- Entry point ------------------- #
if __name__ == "__main__":
    run_aggregation(window_minutes=5)