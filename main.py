import asyncio
import logging
import sys

from dotenv import load_dotenv

from src.agent.poller import PollingAgent
from src.config import load_config
from src.db.store import Database
from src.exec.safety import init_safety_schema, is_stopped

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
    await init_safety_schema(config["database"]["path"])
    stopped, reason = is_stopped()
    if stopped:
        log.warning(
            "STOP file present at startup: %s — real sends will be blocked "
            "until removed (use `python -m scripts.start`).",
            reason,
        )
    agent = PollingAgent(config, db)

    feed_task: asyncio.Task | None = None
    if agent.btc_feed is not None:
        feed_task = asyncio.create_task(agent.btc_feed.run(), name="btc_feed")
        log.info("BTC feed task started")

    monitor_task: asyncio.Task | None = None
    hot_task: asyncio.Task | None = None

    try:
        if "--once" in sys.argv:
            await agent._poll_once()
        else:
            # Position monitor runs as a separate task on its own cadence
            # (typically 15s) so we react to book moves on currently-held
            # positions without waiting for the slow ~2-3 min entry scan.
            monitor_task = asyncio.create_task(agent.monitor_loop(), name="monitor_loop")
            log.info("Position monitor task started")
            # Hot-pair loop (#24): re-poll books for recently-verified pairs
            # at sub-10s cadence so we catch convergence inside the cold
            # cycle window. Cold scan (~30s) feeds the hot list with newly
            # verified pairs each cycle.
            hot_task = asyncio.create_task(agent.hot_loop(), name="hot_loop")
            log.info("Hot-pair loop task started")
            await agent.run()
    finally:
        for task in (hot_task, monitor_task, feed_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if agent.btc_feed is not None:
            agent.btc_feed.stop()


if __name__ == "__main__":
    asyncio.run(main())
