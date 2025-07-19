from faker import Faker
from aiokafka import AIOKafkaProducer
from datetime import datetime as dt
import random
import json
import asyncio
from collections import Counter
import logging
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

f = Faker()
TOPIC_NAME = "Test1"

counter = Counter()


async def user_data_producer(producer: AIOKafkaProducer):
    temp_users = [f"uid_{i}" for i in range(1, 10 + 1)]
    selected_user = random.choice(temp_users)
    data = {
        "state": f.state(),
        "user": selected_user,
        "age": random.randint(18, 70),
        "event_time": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    await producer.send(topic=TOPIC_NAME, value=data)
    counter[selected_user] += 1
    logger.info(f"data: {data}, total_rows: {sum(counter.values())}")


async def laon_data_producer(producer: AIOKafkaProducer):
    data = {
        "loan_id": uuid.uuid1(1).int,
        "funded_amnt": random.randint(1000, 2000),
        "paid_amnt": random.uniform(100, 1000),
        "addr_state": f.state_abbr(
            include_territories=False, include_freely_associated_states=False
        ),
    }
    await producer.send(topic=TOPIC_NAME, value=data)
    logger.info(f"data: {data}")


async def main(data_producer):
    producer = AIOKafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # manually start aiokafka
    await producer.start()
    counter = 0
    try:
        while counter <= 100:
            total_tasks = random.randint(1, 3)
            tasks = [data_producer(producer=producer) for _ in range(total_tasks)]
            await asyncio.gather(*tasks)
            await asyncio.sleep(random.uniform(0.5, 2.0))
            counter += total_tasks
            logger.info(f"Total data produced: {counter}")
    finally:
        await producer.flush()
        await producer.stop()


if __name__ == "__main__":
    # asyncio.run(main(data_producer=user_data_producer))
    asyncio.run(main(data_producer=laon_data_producer))
