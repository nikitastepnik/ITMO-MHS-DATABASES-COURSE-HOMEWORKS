import json

import redis
from pykafka import KafkaClient

# подразумевается, что каждая реплика сервиса будет читать сообщения из kafka,
# используя свою партицию и consumer group
KAFKA_CONSUMER_GROUP = '0'

client = KafkaClient(hosts='localhost:29092')
topic = client.topics['trips_topic']


def send_to_redis_from_kafka():
    redis_client = redis.Redis(host='localhost', port=6379)
    consumer = topic.get_simple_consumer(
        consumer_group=KAFKA_CONSUMER_GROUP,
        auto_commit_enable=False,
    )
    for message in consumer:
        if message is not None:
            trips_data = json.loads(message.value.decode('utf-8'))
            trip_id = trips_data['trip_id']
            if redis_client.get(trip_id) is None:
                pu_location_id = trips_data['PULocationID']
                do_location_id = trips_data['DOLocationID']
                redis_client.incr('total_active_trips')
                redis_client.incr(f'active_trips_by_location:{pu_location_id}')
                redis_client.incr(f'{pu_location_id}:{do_location_id}')
                # устанавливаем срок жизни ключа равным 30 минутам
                redis_client.set(
                    f'active_trips_by_location:{pu_location_id}',
                    redis_client.get(f'active_trips_by_location:{pu_location_id}'),
                    ex=1800,
                )
                # Сохраняем offset для учета уже прочитанных сообщений
                consumer.commit_offsets()


send_to_redis_from_kafka()
