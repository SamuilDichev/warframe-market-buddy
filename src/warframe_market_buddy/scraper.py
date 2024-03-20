import asyncio
import logging
import os
from collections import deque
from typing import Dict
from urllib.parse import urljoin

import aiohttp

from warframe_market_buddy.constants import WARFRAME_MARKET_ITEMS
from warframe_market_buddy.db import AsyncDatabaseConnection
from warframe_market_buddy.models import Item
from warframe_market_buddy.util import setup_logging

LOGGER = logging.getLogger(__name__)


class Scraper:
    def __init__(self, item_output_queue: asyncio.Queue):
        self.item_output_queue = item_output_queue

    async def get_data(self, session, url):
        async with session.get(url) as response:
            r = await response.json()
            return r["data"]

    async def run(self, already_stored_items=set()):
        LOGGER.info("Starting scraper")
        async with aiohttp.ClientSession() as session:
            all_items = await self.get_data(session, WARFRAME_MARKET_ITEMS)
            # Start with items we haven't already stored, i.e. new items, then proceed to update old ones after that
            all_items.sort(key=lambda x: x["urlName"] in already_stored_items)
            all_items = deque(all_items)

            LOGGER.info("Got a list of %s items. Items to skip: %s", len(all_items), len(already_stored_items))

            processed_items = 0
            while all_items:
                item = all_items.popleft()
                item_sub_url = item["urlName"]
                processed_items += 1

                LOGGER.debug("Scraping %s (%s), left %s", item_sub_url, processed_items, len(all_items))
                if processed_items % 50 == 0:
                    LOGGER.info("Scraping %s (%s), left %s", item_sub_url, processed_items, len(all_items))

                try:
                    item_details = await self.get_data(session, urljoin(WARFRAME_MARKET_ITEMS, item_sub_url))
                except aiohttp.ContentTypeError:
                    LOGGER.debug("Failed scraping item %s, queueing it for retry", item_sub_url)
                    all_items.append(item)
                    continue

                full_item = Item(
                    item_details["i18n"]["en"]["name"],
                    item_details["urlName"],
                    item_details["i18n"]["en"].get("wikiLink"),
                    item_details["tradable"],
                    item_details.get("tradingTax"),
                    item_details.get("ducats"),
                )
                self.item_output_queue.put_nowait(full_item)
                LOGGER.debug("Success scraping %s", item_sub_url)


class ItemPersistence:
    def __init__(self, item_input_queue: asyncio.Queue, **db_kwargs: Dict):
        self._item_input_queue = item_input_queue
        self._stop_event = asyncio.Event()
        self.stopped_event = asyncio.Event()
        self._db_kwargs = db_kwargs

    async def get_existing_items(self):
        async with AsyncDatabaseConnection(**self._db_kwargs) as conn:
            items = await conn.fetch("SELECT url_name FROM items")
            return {item["url_name"] for item in items}

    async def run(self):
        processed_items = 0
        LOGGER.info("Starting item persistence")
        async with AsyncDatabaseConnection(**self._db_kwargs) as conn:
            while not (self._stop_event.is_set() and self._item_input_queue.empty()):
                item = await self._item_input_queue.get()
                processed_items += 1
                LOGGER.debug("Storing item %s (%s)", item.url_name, processed_items)
                if processed_items % 50 == 0:
                    LOGGER.info("Storing item %s (%s)", item.url_name, processed_items)

                await conn.execute(
                    """
                    INSERT INTO items(name, url_name, wiki_url, tradable, trading_tax, ducats)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (url_name) DO UPDATE SET
                    name = EXCLUDED.name,
                    wiki_url = EXCLUDED.wiki_url,
                    tradable = EXCLUDED.tradable,
                    trading_tax = EXCLUDED.trading_tax,
                    ducats = EXCLUDED.ducats;
                    """,
                    item.name,
                    item.url_name,
                    item.wiki_url,
                    item.tradable,
                    item.trading_tax,
                    item.ducats,
                )

        self.stopped_event.set()

    def stop(self):
        self._stop_event.set()


async def main(db_kwargs):
    item_queue = asyncio.Queue()
    scraper = Scraper(item_queue)
    item_persistence = ItemPersistence(item_queue, **db_kwargs)

    stored_url_names = await item_persistence.get_existing_items()
    scraper_task = asyncio.create_task(scraper.run(stored_url_names))
    writer_task = asyncio.create_task(item_persistence.run())

    done, pending = await asyncio.wait([scraper_task, writer_task], return_when=asyncio.FIRST_COMPLETED)
    assert scraper_task in done and writer_task in pending, "Something went wrong"
    for task in done | pending:
        if (exception := task.exception()) is not None:
            raise exception

    LOGGER.info("Finishing up scraping. Done %s, pending %s", done, pending)

    item_persistence.stop()
    await item_persistence.stopped_event.wait()
    LOGGER.info("Scraping and writing both done")


if __name__ == "__main__":
    setup_logging(root_level=logging.INFO)

    postgres_user = os.environ["POSTGRES_USER"]
    postgres_pass = os.environ["POSTGRES_PASSWORD"]
    postgres_host = os.environ["POSTGRES_HOST"]
    postgres_port = os.environ["POSTGRES_PORT"]
    postgres_db = os.environ["POSTGRES_DB"]

    database_url = f"postgresql://{postgres_user}:{postgres_pass}@{postgres_host}:{postgres_port}/{postgres_db}"
    db_kwargs = {"dsn": database_url}

    asyncio.run(main(db_kwargs))
