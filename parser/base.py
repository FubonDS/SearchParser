import re
from abc import ABC, abstractmethod
from typing import Dict, Optional
from urllib.parse import quote

from newspaper import Article

from ..utils.logger import logger
from ..utils.text_utils import clean_wikinews_tail


class BaseParser(ABC):
    @abstractmethod
    def can_handle(self, url: str) -> bool:
        pass

    @abstractmethod
    def parse(self, url: str) -> Optional[Dict]:
        pass
    

class GenericParser(BaseParser):
    def can_handle(self, url: str) -> bool:
        return True  # fallback parser
    
    def parse(self, url: str) -> Optional[Dict]:
        try:
            logger.info(f"[GenericParser] 嘗試解析: {url}")
            encoded_url = quote(url, safe=":/") if not re.fullmatch(r"[ -~]+", url) else url
            article = Article(encoded_url)
            article.download()
            article.parse()
            
            text = article.text
            published = article.publish_date
            if published:
                published = published.strftime("%Y-%m-%d")
            if "wikinews.org" in encoded_url:
                text = clean_wikinews_tail(text)
            
            return {
                    "title": article.title,
                    "published": published,
                    "text": text,
                    "error": None
                }   
        except Exception as e:
            logger.error(f"[GenericParser] 解析失敗：{e}")
            return {
                "title": "",
                "published": None,
                "text": "",
                "error": str(e)
            }
            
        
                    
        
        