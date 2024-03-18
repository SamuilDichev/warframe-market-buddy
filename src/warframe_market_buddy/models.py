from dataclasses import dataclass
from typing import Optional


@dataclass
class Item:
    name: str
    url_name: str
    wiki_url: str
    tradable: bool
    trading_tax: int
    ducats: Optional[int]
