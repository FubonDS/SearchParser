import logging
from app.database.postgres_db.postgres_tools import PostgresHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_handler = PostgresHandler(config_path="./app/database/private/database.ini", logger=logger)

def get_db():
    """
    取得資料庫連線物件，在 FastAPI 內部使用 Dependency Injection。
    """
    return db_handler
