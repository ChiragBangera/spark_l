from faker import Faker

# from kafka import KafkaProducer
from aiokafka import AIOKafkaProducer
import json
import random
import asyncio
import datetime

fake = Faker()


TOPIC = "random-data-test"


async def produce_data(producer):
    data = {
        "name": fake.name_male(),
        "email": fake.email(),
        "address": fake.address(),
        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "age": random.randint(10, 80),
    }

    await producer.send(topic=TOPIC, value=data)
    print(f"Producer {data}")


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await producer.start()

    try:
        while True:
            total_users = random.randint(1, 3)
            tasks = [produce_data(producer=producer) for _ in range(total_users)]
            await asyncio.gather(*tasks)
            await asyncio.sleep(random.uniform(0.5, 2.0))
            print(25 * "*")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
