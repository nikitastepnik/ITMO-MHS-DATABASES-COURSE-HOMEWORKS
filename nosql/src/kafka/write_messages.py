import asyncio
import datetime
import json

from aiokafka import AIOKafkaProducer
from pymongo import MongoClient

KAFKA_PARTITION_ID = 0
MONGO_DB_DOCS_OFFSET = 0
MONGO_DB_DOCS__LIMIT = 32

client = MongoClient('mongodb://root:pass@localhost:27017')
db = client['trips']
collection = db['trips_timings']

# https://habr.com/ru/companies/badoo/articles/333046/
# Для включения этой функции и получения семантики exactly-once
# для каждого раздела (то есть никакого дублирования,
# никакой потери данных и сохранение порядка доставки)
# просто укажите в настройках продюсера enable.idempotence=true.
kafka_producer_conf = {
    'bootstrap_servers': 'localhost:29092',
    'enable_idempotence': True,
    'acks': 'all',
}


def json_encoder(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()


async def send_to_kafka_from_mongo():
    producer = AIOKafkaProducer(**kafka_producer_conf)
    await producer.start()

    for document in collection.find().skip(
            MONGO_DB_DOCS_OFFSET
    ).limit(MONGO_DB_DOCS__LIMIT):
        document.pop('_id')
        serialized_document = json.dumps(document, default=json_encoder)
        # Представим, что у нас есть несколько реплик сервиса, тогда
        # для параллелизации отправки сообщений – мы можем писать в один
        # kafka topic, но в разные партиции.
        # Номер используемой партиции нужно явно задавать:
        # одна реплика сервиса – одна партиция.
        # Так же необходимо, чтобы каждая реплика сервиса обрабатывала только
        # свои документы mongoDb, для этого, в методе поиска find() – явно
        # зададим offset и limit для каждой реплики сервиса.
        await producer.send(
            'trips_topic', value=serialized_document.encode(),
            partition=KAFKA_PARTITION_ID,
        )
    await producer.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_to_kafka_from_mongo())
    loop.close()
