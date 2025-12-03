from db_handler import DBHandler

db = DBHandler(config_path="./configs/database.ini", section="sqlite")
db.create_tables()
table_list = db.get_header("failed_articles")
print(table_list)
result = db.get_data("failed_articles", order_by_list=["id DESC"], conditional_rule_list=[("query", "LIKE", "%vietnam gdp%")])
print(result)

from datetime import datetime
articles = [
        {
            "url": "https://ssss.example.com/article1",
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
            "url": "https://news.exsssample.com/article2",
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

# db.add_data("parsed_articles", articles, unique_columns="url")

# db.delete_data(
#     table="parsed_articles",
#     reference_column_list=["url"],
#     filter_list=[
#         {"url": "https://news.example.com/article1"},
#         {"url": "https://news.example.com/article2"},
#     ]
# )

sql = "SELECT * FROM parsed_articles;"

result = db._execute_sql(sql)
print(result)
