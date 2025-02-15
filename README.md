# ETL-Pipeline-With-Apache-Kafka
Real-Time ETL Pipeline using Kafka, Spark, and Hive  A real-time ETL pipeline that fetches news from NewsAPI, processes it using Spark Streaming, and stores structured data in Hive. Uses Kafka for streaming, Parquet for storage, and enables real-time analytics with Hive queries.
Pipeline Overview
The ETL pipeline is divided into three main stages:
  Producer: Fetches news articles from NewsAPI and streams them into a Kafka topic.
            The Producer is a Python script that collects articles from NewsAPI and sends them to a Kafka topic. It uses an API key to connect to NewsAPI and fetches articles related to the keyword "technology." The articles are organized in JSON format to match the                    pipeline structure. They are then streamed to the Kafka topic named finalProject on the server localhost:9092. A small delay is added between sending each article to ensure smooth processing.
  First Spark Session: Processes the Kafka stream, structures the data, and saves it as JSON files.
            The first Spark session processes real-time data from Kafka and saves it as JSON files for later use. It defines a schema with fields like author, title, content, description, publishedAt, source details, URL, and image URL. The session reads data from the                 Kafka topic finalProject, converts the binary data to JSON using the schema, and stores it in the raw_json directory. A checkpoint directory ensures progress tracking and fault tolerance.
  Second Spark Session: Reads JSON files, cleans and selects relevant fields, and stores the data in Hive.
            This session reads the JSON files created in the first session, cleans the data, and saves it in Hive. It reads the files from the raw_json directory using the same schema. Relevant fields like author, content, description, publishedAt, source details, title,              URL, and image URL are selected. Field names are standardized, such as renaming source.id to source_id.
            The cleaned data is saved in a Hive table called news_articles within the news_database.db database. It is stored in Parquet format to enable efficient storage and querying. A checkpoint system is used to prevent data duplication or loss.
            
