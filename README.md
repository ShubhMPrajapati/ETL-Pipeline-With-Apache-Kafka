# **ETL Pipeline With Apache Kafka**  

### **Real-Time ETL Pipeline using Kafka, Spark, and Hive**  

A real-time ETL pipeline that fetches news from **NewsAPI**, processes it using **Spark Streaming**, and stores structured data in **Hive**.  
The pipeline uses **Kafka** for streaming, **Parquet** for storage, and enables real-time analytics with **Hive queries**.  

---

## **Pipeline Overview**  

The ETL pipeline is divided into three main stages:  

### **1️⃣ Producer: Fetching & Streaming News Articles**  
- The **Producer** is a Python script that collects articles from **NewsAPI** and streams them into a Kafka topic.  
- It uses an **API key** to fetch articles related to the keyword **"technology"**.  
- Articles are formatted in **JSON** to align with the pipeline structure.  
- The data is streamed into the Kafka topic **`finalProject`** on the server **`localhost:9092`**.  
- A **small delay** is added between each article to ensure smooth processing.  

---

### **2️⃣ First Spark Session: Processing & Storing Raw Data**  
- This session **processes real-time data** from Kafka and stores it as **JSON files**.  
- A **schema** is defined with fields like:  
  - `author`, `title`, `content`, `description`, `publishedAt`, `source_id`, `source_name`, `url`, `image_url`.  
- The session:  
  1. Reads data from the Kafka topic **`finalProject`**.  
  2. Converts the **binary data to JSON** using the schema.  
  3. Stores the JSON files in the **`raw_json`** directory.  
  4. Uses a **checkpoint directory** for **progress tracking** and **fault tolerance**.  

---

### **3️⃣ Second Spark Session: Cleaning & Storing Data in Hive**  
- Reads the **JSON files** from the first session.  
- Cleans and selects relevant fields:  
  - `author`, `content`, `description`, `publishedAt`, `source_id`, `source_name`, `title`, `url`, `image_url`.  
- Standardizes field names (e.g., renaming `source.id` to `source_id`).  
- Stores the **cleaned data** in a Hive table:  
  - **Database:** `news_database.db`  
  - **Table:** `news_articles`  
- The data is stored in **Parquet format** for efficient storage and querying.  
- A **checkpoint system** prevents **data duplication** or **loss**.  

---

### 🚀 **Summary**  
This ETL pipeline enables **real-time news processing** with **Kafka, Spark, and Hive**. It ensures:  
✅ **Seamless data streaming** via Kafka.  
✅ **Structured storage** in JSON and Hive (Parquet).  
✅ **Real-time analytics** with Hive queries.  
