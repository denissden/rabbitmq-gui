import asyncio
from operator import is_
import pika
import random
import time
import json
from rabbit import Rabbit


params = pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=pika.PlainCredentials(
                    username='guest',
                    password='guest'
                )
            )
connection = pika.BlockingConnection(params)
r = Rabbit(connection)


SPEED = 0.01
COUNT = 2000
MALFORMED_COUNT = 1
DELAYS = [1, 2, 5]
DRONE_TYPE = ['weather', 'monitoring', 'delivery']

ROUTING_KEY = 'event.drone.{0}.{1}.gps'
ROUTING_KEY_NO_INFO = 'event.drone.gps'
EXCHANGE = 'x_topic'
HEADERS_VS_TOPIC = 0.5 # 1 - headers; 0 - topic

def drone(info):
    _, lat, lon = info
    lat += random.random()
    lon += random.random()
    while True:
        yield lat, lon
        lat += random.random() * SPEED
        lon += random.random() * SPEED


def get_metadata(drone_type, country, is_headers, is_malformed): 
    if is_malformed:
        topic = ''.join(random.choice('QWERTYqwerty') for i in range(7))
        return (None, topic)

    headers = {
        "drone_type": drone_type,
        "country": country
    }
    topic = ROUTING_KEY.format(drone_type, country)
    if is_headers:
        return (headers, ROUTING_KEY_NO_INFO)
    else:
        return (None, topic)

async def start_drone(info, is_malformed=False):
    country, _, _ = info
    delay = random.choice(DELAYS)
    drone_type = random.choice(DRONE_TYPE)
    is_headers = random.random() < HEADERS_VS_TOPIC

    print(
        'Starting {0} drone in {1} at {2}, {3} reporting every {4} seconds.'.format(drone_type, *info, delay),
        f'Metadata is stored in {"headers" if is_headers else "routing key"}.',
        'The drone is malformed.' if is_malformed else '',
        sep=' '
    )
    for lat, lon in drone(info):
        body = json.dumps({
            "lat": lat,
            "lon": lon,
            "time": time.time()
        })
        headers, routing_key = get_metadata(drone_type, country, is_headers, is_malformed)
        r.publish(EXCHANGE, routing_key, headers, body)
        await asyncio.sleep(delay)

countries = [
    ('ru', 55.7558, 37.6173),
    ('de', 52.5200, 13.4050),
    ('us', 36.7783, -119.4179),
    ('uk', 51.509865, -0.118092),
    ]

loop = asyncio.get_event_loop()
routines = []
for i in range(COUNT):
    info = random.choice(countries)
    routines.append(start_drone(info))
for i in range(MALFORMED_COUNT):
    info = random.choice(countries)
    routines.append(start_drone(info, is_malformed=True))
try:
    loop.run_until_complete(
        asyncio.gather(*routines)
    )
finally:
    loop.close()
