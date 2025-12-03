from sqlalchemy.orm import declarative_base

Base = declarative_base()

from .parsed_article import ParsedArticle
from .failed_article import FailedArticle   

__all__ = [
    "Base",
    "ParsedArticle",
    "FailedArticle",
]
