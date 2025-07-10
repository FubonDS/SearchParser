import re
from datetime import datetime
from typing import Dict, Optional

from ..database.postgres_db.postgres_tools import PostgresHandler

UNWANTED_WIKINEWS_TAIL = [
    "本篇报道已经存档，不能再作修訂。",
    "維基新聞上的文章帶有時效性",
    "请注意，新闻中列出的消息来源URL",
    "如果確實需要修正错误",
]

def clean_wikinews_tail(text: str) -> str:
    for marker in UNWANTED_WIKINEWS_TAIL:
        if marker in text:
            text = text.split(marker)[0].strip()
    return text

def extract_date_from_metadata(metadata: str) -> Optional[str]:
    """從 metadata 抽取日期（格式為 yyyy/mm/dd）"""
    if not metadata:
        return None
    match = re.search(r"\d{4}/\d{1,2}/\d{1,2}", metadata)
    if match:
        try:
            dt = datetime.strptime(match.group(), "%Y/%m/%d")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None
    return None

def parse_published_date(date_str: Optional[str]) -> Optional[datetime]:
    """將字串格式的 published 轉成 datetime 物件"""
    if not date_str:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S",  # e.g., 2022-09-01T12:23:00
        "%Y-%m-%d"            # e.g., 2008-06-20
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None

def get_parsed_article_if_exists(db: PostgresHandler, url: str) -> Optional[Dict]:
    sql = """
    SELECT * FROM parsed_articles WHERE url = %s LIMIT 1;
    """
    result = db._execute_sql(sql, [url])
    if result["data"]:
        return result["formatted_data"][0]  # ✅ 回傳 dict 格式
    return None    