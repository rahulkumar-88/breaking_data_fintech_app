# Streaming Pipeline Documentation

> **Real-Time Financial News Processing with Kafka & Spark Structured Streaming**  
> **LLM-Powered Sentiment Analysis and AstraDB Integration**

---
## Overview

The **Streaming Pipeline** processes real-time financial news to generate sentiment scores for **25 stocks**, enriching daily price predictions with market sentiment. It integrates:
- **NewsAPI** for financial news ingestion
- **Qwen-32b** for sentiment analysis (1-5 scale)
- **Apache Kafka** for message streaming
- **Spark Dataframe** for distributed processing
- **AstraDB (Cassandra)** for scalable NoSQL storage

### **Key Features**

 **Real-time Processing**: Stream financial news as it's published  
 **LLM-Powered Sentiment**: Qwen-32b analyzes news sentiment  
 **Scalable Storage**: AstraDB with multi-region support  
 **Batch Integration**: Sentiment data enriches training/inference pipelines  
 **Multi-Stock Support**: 25 stocks monitored simultaneously  

### **Business Value**

- **Market Sentiment Tracking**: Real-time news sentiment for trading decisions
- **Prediction Enhancement**: Sentiment features improve ML model accuracy
- **Historical Analysis**: Full sentiment history for backtesting
- **Alert Generation**: React to significant sentiment changes

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      STREAMING PIPELINE ARCHITECTURE                     │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│   NewsAPI        │  Financial news source
│   (REST API)     │  • Stock-specific queries
│                  │  • Real-time updates
│   25 stocks:     │  • Article metadata
│   AAPL, GOOG,    │  • Content extraction
│   TSLA, etc.     │
└────────┬─────────┘
         │ HTTP GET
         │ Polling (every N minutes)
         ▼
┌────────────────────────────────────┐
│   News Producer                    │
│   (Python Script)                  │
│                                    │
│   • Fetch news per stock           │
│   • Extract metadata               │
│   • Format JSON messages           │
│   • Publish to Kafka topic         │
└────────┬───────────────────────────┘
         │ Kafka Publish
         │ Topic: financial_news
         ▼
┌──────────────────────────────────────────────────┐
│   Apache Kafka 3.3+                              │
│   (Message Broker)                               │
│                                                  │
│   Topic: NASDAQ_<<stock_symbol>>                 │
│   Partitions: 1                                  │
│   Replication: 1 (local dev)                     │
│                                                  │
│   Message Schema:                                │
│   {                                              │
│     "stock_symbol": "AAPL",                      │
│     "title": "...",                              │
│     "description": "...",                        │
│     "content": "...",                            │
│     "url": "...",                                │
│     "published_at": "2025-11-26T10:30:00Z",      │
│     "source": "Bloomberg"                        │
│   }                                              │
└────────┬─────────────────────────────────────────┘
         │ Kafka Consume
         │ Subscribe: financial_news
         ▼
┌─────────────────────────────────────────────────────┐
│   Spark Structured Streaming                        │
│   (Consumer + Processor)                            │
│                                                     │
│     Read Kafka Stream                               │
│     Parse JSON Messages                             |
│     Call GPT Sentiment API                          │
│     Process Sentiment Scores                        │
│     Write to AstraDB                                │
│                                                     │
│   Processing Mode: Micro-batch (10 sec interval)    │
│   Parallelism: 8 cores (local[8])                   │
└────────┬────────────────────────────────────────────┘
         │
         │ ┌────────────────────────┐
         │ │  Qwen-32b              │
         ├─┤  (via Groq)            │
         │ │                        │
         │ │  Input: 4 news texts   │
         │ │  Output: 5,3,4,5       │
         │ │  (1-5 scale)           │
         │ │                        │
         │ │  Formula:              │
         │ │  • 1 = Negative        │
         │ │  • 2 = Somewhat -      │
         │ │  • 3 = Neutral         │
         │ │  • 4 = Somewhat +      │
         │ │  • 5 = Positive        │
         │ └────────────────────────┘
         │
         ▼ Write to AstraDB
┌───────────────────────────────────────────────────┐
│   AstraDB (Cassandra)                             │
│   (NoSQL Storage)                                 │
│                                                   │
│   Table: news_sentiment_1                         │
│                                                   │
│   Schema:                                         │
│   • stock_symbol (TEXT, Partition Key)            │
│   • published_at (TIMESTAMP, Clustering Key)      │
│   • description (TEXT)                            │
│   • news_url (TEXT)                               │
│   • news_summary (TEXT)                           │
│   • sentiment_score (INT)                         │
│   • created_at (TIMESTAMP)                        │
│                                                   │
│   Query Pattern:                                  │
│   SELECT * FROM news_sentiment_1                  │
│   WHERE stock_symbol = 'AAPL'                     │
│   AND published_at >= '2025-11-01'                │
└────────┬──────────────────────────────────────────┘
         │
         │ Daily Read (Airflow DAG)
         ▼
┌───────────────────────────────────────────────────┐
│   Sentiment Enrichment                            │
│   (Daily Batch Process)                           │
│                                                   │
│   • Read YFinance daily data                      │
│   • Query AstraDB for sentiment (per stock/date)  │
│   • Join price + sentiment                        │
│   • Calculate Scaled_sentiment: (score-0.9999)/4  │
│   • Add News_flag (binary indicator)              │
│   • Save to tmp/yfinance_enriched.parquet         │
│   • Load into Delta tables                        │
│                                                   │
│   File: src/compute_sentiment_columns.py          │
└───────────────────────────────────────────────────┘
```

---

## Data Flow

### **End-to-End Flow**

```
1. NewsAPI Query
   ↓
2. News Producer → Kafka Topic
   ↓
3. Spark Structured Streaming Consumer
   ↓
4. GPT Sentiment Analysis (Batch of 4)
   ↓
5. AstraDB Storage (news_sentiment_1 table)
   ↓
6. Daily Enrichment (Airflow DAG)
   ↓
7. Delta Lake Storage (stock_<SYMBOL> tables)
   ↓
8. Training/Inference Pipeline (ML features)
```

### **Data Transformation**

#### **Stage 1: News Ingestion** (NewsAPI → Kafka)

```json
Read data from NewsAPI and convert into a JSON file for Kafka prodcuer
```

#### **Stage 2: Sentiment Analysis** (GPT Processing)

```python
Call llm to get sebtimnent score on a scale of (1-5)
```

#### **Stage 3: Storage** (AstraDB)

```sql
Insert data into the AstraDB
```

#### **Stage 4: Daily Enrichment** (Batch Processing)

```python
 Read daily sentiment score for the selected stocks from AstraDB and enrich with the corresponding yfinance data.
 This output is fed to the trained model for inferencing
```


## Summary

### **Key Components**

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **News Source** | NewsAPI | Financial news ingestion |
| **Message Broker** | Kafka 3.3+ | Stream buffering & distribution |
| **Stream Processing** | Spark Structured Streaming | Real-time data transformation |
| **Sentiment Analysis** | Qwen-32b (via Groq) | AI-powered sentiment scoring |
| **Storage** | AstraDB (Cassandra) | Scalable NoSQL storage |
| **Batch Integration** | Python/Pandas | Daily enrichment for ML pipeline |

### **Data Flow Summary**

1. **News Ingestion**: NewsAPI → Kafka (every 15 min)
2. **Streaming**: Kafka → Spark → GPT → AstraDB (real-time)
3. **Enrichment**: AstraDB → Pandas → Delta Lake (daily batch)
4. **ML Pipeline**: Delta Lake → Training/Inference (Airflow)


### **Production Readiness**

 **Working**: Sentiment enrichment integrated in daily DAG  
 **Planned**: Full Kafka+Spark streaming deployment  
 **Operational**: AstraDB storage and querying functional  

---

---

**Last Updated**: November 26, 2025  
**Version**: 1.0.0  
**Status**: Production (Batch Enrichment), Planned (Full Streaming)
