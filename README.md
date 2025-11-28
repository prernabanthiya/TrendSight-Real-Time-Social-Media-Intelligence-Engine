# TrendSight-Real-Time-Social-Media-Intelligence-Engine

## Overview

This project demonstrates a complete, production-style data engineering workflow that converts live Twitter data into meaningful, real-time business insights. It showcases the ability to design, build, and operate an end-to-end pipeline that handles data ingestion, processing, enrichment, and visualization using modern tools and best practices.

The system continuously fetches tweets from the Twitter API, stores them in MongoDB, performs NLP-based analysis and trend calculations, and streams the processed data into Power BI where it is visualized through an automatically updating dashboard.

This project reflects practical, job-ready skills in data engineering, real-time analytics, and system integration.

---

## What This Project Delivers

* Real-time collection of social media data
* Structured storage of raw and processed data
* Intelligent analysis of unstructured text
* Detection of trending topics and sentiment
* Automated live dashboard updates
* Clear separation of ETL responsibilities

The result is a system capable of monitoring online trends and public sentiment as they evolve.

---

## Architecture

```
Twitter API
     ↓
Ingestion Layer (Python)
     ↓
MongoDB - Raw Tweets
     ↓
Processing & Enrichment Engine
     ↓
MongoDB - Curated Analytics
     ↓
Power BI Streaming Dataset
     ↓
Live Power BI Dashboard
```

---

## Technical Stack

* Python
* Twitter API v2
* MongoDB
* Pandas
* TextBlob, spaCy, NLTK (NLP)
* REST APIs
* Power BI Service

---

## Pipeline Breakdown

### 1. Data Ingestion

A Python service pulls tweets using the Twitter API at regular intervals. Each tweet includes text, timestamp, and engagement metrics. The data is stored unchanged in MongoDB to preserve its original structure and ensure auditability.

### 2. Data Processing & Enrichment

Raw tweets are processed to extract meaningful features:

* Text normalization and cleaning
* Hashtag extraction (topic identification)
* Sentiment analysis per tweet
* Engagement calculation based on likes, replies, and retweets
* Trend detection using frequency and velocity analysis

Aggregated metrics per topic include:

* Mention Count
* Average Sentiment
* Total Engagement
* Trend Score
* Growth Velocity

These enriched results are stored in a curated collection optimized for analytics.

### 3. Real-Time Streaming to Power BI

The curated data is pushed to Power BI using its REST Streaming API, enabling continuous, automatic updates to dashboard visuals with no manual refresh required.

### 4. Visualization Layer

The Power BI dashboard provides:

* Live trending topics
* Sentiment distribution per topic
* Engagement comparison metrics
* Dynamic trend score indicators
* Real-time growth monitoring

This dashboard transforms raw social data into actionable insight.

---

## Why This Project Stands Out

* Demonstrates a full-cycle data pipeline (source → storage → processing → visualization)
* Implements real-time data delivery and dashboard automation
* Applies NLP techniques to real-world unstructured data
* Shows strong understanding of system design and data flow
* Mirrors real-world analytics systems used in industry

---

## Project Structure

```
├── ingest_tweets.py          # Collects tweets from Twitter API
├── transform_enriched.py     # Processes and enriches tweet data
├── push_to_powerbi.py        # Streams data to Power BI
├── transform_pandas.py       # Alternate processing workflow
├── requirements.txt
└── dashboard_snapshot.png
```

---

## Setup & Execution

### Requirements

* Python 3.8+
* MongoDB
* Twitter Developer Access
* Power BI Service Account

### Installation

```bash
pip install -r requirements.txt
```

### Environment Variables

```bash
TWITTER_BEARER_TOKEN=your_token
POWERBI_STREAM_URL=your_powerbi_url
```

### Running the Pipeline

```bash
python ingest_tweets.py
python transform_enriched.py
python push_to_powerbi.py
```

---

## Key Learning Outcomes

* Designing modular ETL pipelines
* Working with APIs and NoSQL databases
* Processing unstructured text data
* Real-time data streaming concepts
* Dashboard-driven analytics
* Practical application of NLP

---

## Potential Enhancements

* Kafka-based streaming architecture
* Cloud deployment (AWS / Azure)
* Transformer-based sentiment models
* Real-time alerting system
* Containerization using Docker

---

## Summary

This project highlights strong hands-on experience in building scalable data pipelines, integrating live external data sources, and transforming raw information into decision-ready insights. It reflects practical application of data engineering principles commonly used in production analytics systems.

It demonstrates readiness for roles in Data Engineering, Analytics Engineering, and Business Intelligence.

---


## Author

Developed by: Your Name
Email: [prernabanthiya4@email.com](mailto:prernabanthiya4@email.com)
LinkedIn: [prerna-banthiya](https://www.linkedin.com/in/prerna-banthiya/)
