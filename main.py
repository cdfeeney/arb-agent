import asyncio
import logging
import sys

from dotenv import load_dotenv

from src.agent.poller import PollingAgent
from src.config import load_config
from src.db.store import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s"
)
log = logging.getLogger(__name__)


async def main() -> None:
    load_dotenv()
    config = load_config("config.yaml")
    db = Database(config["database"]["path"])
    await db.init()
    agent = PollingAgent(config, db)

    feed_task: asyncio.Task | None = None
    if agent.btc_feed is not None:
        feed_task = asyncio.create_task(agent.btc_feed.run(), name="btc_feed")
        log.info("BTC feed task started")

    try:
        if "--once" in sys.argv:
            await agent._poll_once()
        else:
            await agent.run()
    finally:
        if feed_task is not None:
            agent.btc_feed.stop() if agent.btc_feed else None
            feed_task.cancel()
            try:
                await feed_task
            except (asyncio.CancelledError, Exception):
                pass


if __name__ == "__main__":
    asyncio.run(main())
