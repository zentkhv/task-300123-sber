from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random

p = Producer({'bootstrap.servers': '192.168.175.139:9092'})
fake = Faker()

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def receipt(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)


def main_fake():
    for i in range(3):
        data = {
            'user_id': fake.random_int(min=20000, max=100000),
            'user_name': fake.name(),
            'user_address': fake.street_address() + ' | ' + fake.city() + ' | ' + fake.country_code(),
            'platform': random.choice(['Mobile', 'Laptop', 'Tablet']),
            'signup_at': str(fake.date_time_this_month())
        }
        m = json.dumps(data)
        p.poll(1)
        p.produce('user-tracker', m.encode('utf-8'), callback=receipt)
        p.flush()
        time.sleep(3)


main_fake()
