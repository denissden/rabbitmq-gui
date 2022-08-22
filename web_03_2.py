from turtle import pen
import pika
import pika.spec
import random
import time


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

    first_exchange = properties.headers['x-first-death-exchange']
    death_count = properties.headers['x-death'][0]['count']

    print(method.routing_key)
    print(body.decode('utf-8'))
    print('Death: ', death_count)

    time.sleep(1)

    if death_count < 6:
        channel.basic_publish(
            exchange=first_exchange,
            routing_key=method.routing_key,
            properties=properties,
            body=body
        )
    else:
        print("Message died")

    channel.basic_ack(method.delivery_tag, multiple=True)

channel.basic_qos(prefetch_count=3)
channel.basic_consume(
    queue='q_dead',
    on_message_callback=on_message
)

while True:
    connection.process_data_events()