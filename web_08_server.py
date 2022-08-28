import json
import pika
import pika.spec
import random
import time


USERS = {
    'john13': {
        'name': 'John Evelinne',
        'born': 1992,
        'city': 'New York'
    },
    'pink_kitty': {
        'name': 'Ann Downing',
        'born': 2000,
        'city': 'Berlin'
    },
    'h4x0r': {
        'name': 'Ivan Skhodnenko',
        'born': 1989,
        'city': 'Voronezh'
    }
}

params = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    credentials=pika.PlainCredentials(
        username='guest',
        password='guest'
    )
)


connection = pika.BlockingConnection(params)
channel = connection.channel()

def on_message(ch, 
    method: pika.spec.Basic.Deliver, 
    properties: pika.spec.BasicProperties, 
    body):

    request = json.loads(body)
    login = request['login']

    user = None
    if login in USERS:
        user = USERS[login]

    time.sleep(random.randint(0, 5))

    json_response = json.dumps(user)
    props = pika.BasicProperties(correlation_id=properties.correlation_id)
    # отправка
    channel.basic_publish(
        exchange="",
        routing_key=properties.reply_to, # название очереди
        properties=props,
        body=json_response 
    )   


channel.basic_consume(
    queue='q_users',
    on_message_callback=on_message
)

while True:
    connection.process_data_events()
    time.sleep(0.1)