import asyncio
import logging
import os
from dotenv import load_dotenv
from src.config import load_config
from src.db.store import Database
from src.agent.poller import PollingAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s"
)

async def main():
    load_dotenv()
    config = load_config("config.yaml")
    db = Database(config["database"]["path"])
    await db.init()
    agent = PollingAgent(config, db)
    await agent.run()

if __name__ == "__main__":
    asyncio.run(main())
