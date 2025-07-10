import re
from datetime import datetime
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from ..utils.logger import logger

from .base import BaseParser


def parse_msn_article_json(article_json: dict) -> dict:
    title = article_json.get("title", "").strip()

    published_raw = article_json.get("publishedDateTime")
    if published_raw:
        published_date = datetime.fromisoformat(published_raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    else:
        published_date = ""

    body_html = article_json.get("body", "")
    soup = BeautifulSoup(body_html, "lxml")
    paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
    content = "\n".join(paragraphs)

    return {
        "title": title,
        "publish_time": published_date,
        "content": content
    }

def fetch_and_parse_msn_article(msn_url: str) -> dict:
    match = re.search(r'/ar-([A-Za-z0-9]+)', msn_url)
    if not match:
        logger
        raise ValueError("無法從網址中擷取文章 ID")

    article_id = match.group(1)
    api_url = f"https://assets.msn.com/content/view/v2/Detail/zh-tw/{article_id}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    response = requests.get(api_url, headers=headers, timeout=10)
    response.raise_for_status()

    article_json = response.json()
    return parse_msn_article_json(article_json)

class MSNParser(BaseParser):
    def can_handle(self, url: str) -> bool:
        return "msn.com" in url
    
    def parse(self, url: str):
        logger.info(f"[MSNParser] 解析 {url}")
        try:
            encoded_url = quote(url, safe=":/") if not re.fullmatch(r"[ -~]+", url) else url
            result = fetch_and_parse_msn_article(encoded_url)
            if "error" not in result:
                return {
                    "title": result["title"],
                    "text": result["content"],
                    "error": None
                }
            return {
                "title": "",
                "text": "",
                "error": result["error"]
            }
        except Exception as e:
            logger.error(f"[GenericParser] 解析失敗：{e}")
            return {
                "title": "",
                "text": "",
                "error": str(e)
            }            