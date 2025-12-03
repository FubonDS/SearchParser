from sqlalchemy import TIMESTAMP, Column, Float, Integer, Text

from . import Base


class ParsedArticle(Base):
    __tablename__ = "parsed_articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, unique=True)
    query = Column(Text)
    title = Column(Text)
    snippet = Column(Text)
    engine = Column(Text)
    published = Column(TIMESTAMP)
    score = Column(Float)
    text = Column(Text)
    error = Column(Text)
    
    def to_dict(self):
        return {
            "id": self.id,
            "url": self.url,
            "query": self.query,
            "title": self.title,
            "snippet": self.snippet,
            "engine": self.engine,
            "published": self.published.isoformat() if self.published else None,
            "score": self.score,
            "text": self.text,
            "error": self.error,
        }
