import email
import pika
import pika.spec
import random


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

RESULTS = {
    'email': 0
}

def on_message(ch, 
    method: pika.spec.Basic.Deliver, 
    properties: pika.spec.BasicProperties, 
    body):
    print(body.decode('utf-8'))
    if body.decode('utf-8') == 'email':
        RESULTS['email'] += 1
    ch.basic_ack(method.delivery_tag)

channel.basic_consume(
    queue='q_test',
    on_message_callback=on_message
)

# send
for i in range(5):
    channel.basic_publish("", 'q_test', body='email')

while True:
    connection.process_data_events()