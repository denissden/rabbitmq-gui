import pika
import pika.spec
import random
import time
import uuid


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

info = channel.queue_declare('', exclusive=True, auto_delete=True)
queue_name = info.method.queue


def on_message(ch, 
    method: pika.spec.Basic.Deliver, 
    properties: pika.spec.BasicProperties, 
    body):

    print(properties.correlation_id,  body.decode())
    # получение

channel.basic_consume(
    queue=queue_name,
    on_message_callback=on_message
)

# отправка
logins = ['john13', 'asd', 'asdsad', 'pink_kitty', 'h4x0r']
for login in logins:
    guid = uuid.uuid4()
    print(guid)

    props = pika.BasicProperties(
        correlation_id=str(guid),
        reply_to=queue_name
        )
    channel.basic_publish(
            exchange='x_rpc',
            routing_key='users.get.rpc', # название очереди
            properties=props,
            body='{ "login": ' + f'"{login}"' + ' }'
    ) 

while True:
    connection.process_data_events()
    time.sleep(0.1)