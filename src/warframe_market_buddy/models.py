from dataclasses import dataclass
from typing import Optional


@dataclass
class Item:
    name: str
    url_name: str
    wiki_url: Optional[str]  # Relics won't have this
    tradable: bool
    trading_tax: Optional[int]  # Non-tradable things won't have this
    ducats: Optional[int]  # Items not sellable to Baro Ki'Teer won't have this
