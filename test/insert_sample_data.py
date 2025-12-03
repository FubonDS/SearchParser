from database import SessionLocal
from models import ParsedArticle, FailedArticle
from datetime import datetime

def insert_sample_data():
    db = SessionLocal()

    # -------------------------------
    # 插入 parsed_articles（成功文章）
    # -------------------------------
    articles = [
        {
            "url": "https://news.example.com/article1",
            "query": "vietnam economy",
            "title": "Vietnam Economy Shows Growth",
            "snippet": "The Vietnamese economy continues to expand...",
            "engine": "google",
            "published": datetime(2024, 5, 10),
            "score": 0.92,
            "text": "Full article content 1... ",
            "error": None
        },
        {
            "url": "https://news.example.com/article2",
            "query": "vietnam stock market",
            "title": "VN-Index Reaches New High",
            "snippet": "Stock market rises strongly as investors remain optimistic...",
            "engine": "bing",
            "published": datetime(2024, 5, 11),
            "score": 0.88,
            "text": "Full article content 2...",
            "error": None
        }
    ]

    for a in articles:
        db.add(ParsedArticle(**a))

    # -------------------------------
    # 插入 failed_articles（失敗文章）
    # -------------------------------
    failed = [
        {
            "url": "https://broken.example.com/404",
            "query": "vietnam inflation",
            "title": None,
            "snippet": None,
            "engine": "google",
            "published": None,
            "score": None,
            "text": None,
            "error": "HTTP 404 Not Found"
        },
        {
            "url": "https://timeout.example.com",
            "query": "vietnam gdp",
            "title": None,
            "snippet": None,
            "engine": "bing",
            "published": None,
            "score": None,
            "text": None,
            "error": "Timeout while fetching URL"
        }
    ]

    for f in failed:
        db.add(FailedArticle(**f))

    db.commit()
    db.close()
    print("Sample data inserted!")

if __name__ == "__main__":
    insert_sample_data()
