# parser/__init__.py
from .base import GenericParser
from .ctee_parser import CteeParser
from .msn_parser import MSNParser

PARSERS = [
    CteeParser(),
    MSNParser(),
    GenericParser(),  # fallback 最後使用
]


def parse_article(url: str):
    for parser in PARSERS:
        if parser.can_handle(url):
            return parser.parse(url)
