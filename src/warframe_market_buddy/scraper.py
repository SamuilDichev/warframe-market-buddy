import argparse
import asyncio
import configparser
import logging
from collections import deque
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

    async def run(self):
        LOGGER.info("Starting scraper")
        async with aiohttp.ClientSession() as session:
            all_items = await self.get_data(session, WARFRAME_MARKET_ITEMS)
            LOGGER.info("Got a list of %s items", len(all_items))
            all_items = deque(all_items)

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
                    item_details["i18n"]["en"]["wikiLink"],
                    item_details["tradable"],
                    item_details["tradingTax"],
                    item_details.get("ducats"),
                )
                self.item_output_queue.put_nowait(full_item)
                LOGGER.debug("Success scraping %s", item_sub_url)


class Writer:
    def __init__(self, item_input_queue: asyncio.Queue, host: str, port: int, user: str, database: str):
        self._item_input_queue = item_input_queue
        self._stop_event = asyncio.Event()
        self.stopped_event = asyncio.Event()
        self._host = host
        self._port = port
        self._user = user
        self._database = database

    async def run(self):
        processed_items = 0
        async with AsyncDatabaseConnection(self._host, self._port, self._user, self._database) as conn:
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


async def main(db_config):
    item_queue = asyncio.Queue()
    scraper = Scraper(item_queue)
    writer = Writer(item_queue, db_config["Host"], db_config["Port"], db_config["User"], db_config["Database"])

    scraper_task = asyncio.create_task(scraper.run())
    writer_task = asyncio.create_task(writer.run())

    done, pending = await asyncio.wait([scraper_task, writer_task], return_when=asyncio.FIRST_COMPLETED)
    assert scraper_task in done and writer_task in pending, "Something went wrong"

    writer.stop()
    await writer.stopped_event.wait()


if __name__ == "__main__":
    setup_logging(root_level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="path to config file")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    db_config = config["DB"]
    asyncio.run(main(db_config))
