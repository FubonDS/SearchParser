import re
import time
from typing import Dict, Optional
from urllib.parse import quote

import cloudscraper
from bs4 import BeautifulSoup

from utils.logger import logger

from .base import BaseParser


def fetch_ctee_article_full(url: str, retry: int = 3) -> dict:
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
    )

    for attempt in range(retry):
        try:
            response = scraper.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            title_tag = soup.find("h1", class_="main-title")
            title = title_tag.get_text(strip=True) if title_tag else ""

            date_tag = soup.find("li", class_="publish-date")
            time_tag = soup.find("li", class_="publish-time")
            date_str = date_tag.find("time").get_text(strip=True) if date_tag else ""
            time_str = time_tag.find("time").get_text(strip=True) if time_tag else ""
            publish_datetime = f"{date_str} {time_str}".strip()

            article = soup.find("article")
            if not article:
                logger.error("找不到 <article> 標籤")
                raise ValueError("找不到 <article> 標籤")

            paragraphs = article.find_all("p")
            content = "\n".join(
                p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)
            )

            return {
                "title": title,
                "content": content
            }

        except Exception as e:
            print(f"[第 {attempt+1} 次嘗試失敗]：{e}")
            if attempt < 2:
                time.sleep(2)
            else:
                return {"error": str(e)}
            
class CteeParser(BaseParser):
    def can_handle(self, url: str) -> bool:
        return "ctee.com.tw" in url
    
    def parse(self, url: str) -> Optional[Dict]:
        logger.info(f"[CteeParser] 解析 {url}")
        try:
            encoded_url = quote(url, safe=":/") if not re.fullmatch(r"[ -~]+", url) else url
            result = fetch_ctee_article_full(url=encoded_url)
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
            
            



