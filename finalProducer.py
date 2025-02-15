import requests
from kafka import KafkaProducer
import json
import time

# Configuration
API_KEY = '24f42e76eca4468f8c1a02de79a29378'
TOPIC_NAME = 'finalProject'
KAFKA_SERVER = 'localhost:9092'
QUERY = 'technology'
NEWS_API_URL = 'https://newsapi.org/v2/everything'

# Initialize Kafka Producer with JSON serializer
def create_kafka_producer(server):
    return KafkaProducer(
        bootstrap_servers=server,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Function to fetch articles from NewsAPI
def fetch_articles(query, api_key):
    try:
        params = {'q': query, 'apiKey': api_key}
        response = requests.get(NEWS_API_URL, params=params)
        response.raise_for_status()
        articles = response.json().get('articles', [])
        
        # Transform articles to match the schema
        formatted_articles = [
            {
                "author": article.get("author"),
                "content": article.get("content"),
                "description": article.get("description"),
                "publishedAt": article.get("publishedAt"),
                "source": {
                    "id": article.get("source", {}).get("id"),
                    "name": article.get("source", {}).get("name")
                },
                "title": article.get("title"),
                "url": article.get("url"),
                "urlToImage": article.get("urlToImage")
            }
            for article in articles
        ]
        return formatted_articles
    except requests.exceptions.RequestException as e:
        print(f"Error fetching articles: {e}")
        return []

# Function to send articles to Kafka
def send_articles_to_kafka(producer, topic, articles):
    for article in articles:
        try:
            print(f"Sending article to Kafka: {article.get('title', 'No Title')}")
            producer.send(topic, article)
            time.sleep(1)
        except Exception as e:  
            print(f"Error sending article: {e}")

if __name__ == "__main__":
    print("Starting Kafka producer...")
    producer = create_kafka_producer(KAFKA_SERVER)

    while True:
        articles = fetch_articles(QUERY, API_KEY)
        if articles:
            send_articles_to_kafka(producer, TOPIC_NAME, articles)
        else:
            print("No articles to send.")
        time.sleep(60)  # Fetch every 60 seconds