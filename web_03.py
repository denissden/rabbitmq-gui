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


def save(body):
    if random.random() > 0.5:
        raise Exception()

def on_message(ch, 
    method: pika.spec.Basic.Deliver, 
    properties: pika.spec.BasicProperties, 
    body):
    print(method.routing_key)
    print(body.decode('utf-8'))

    try:
        save(body)
    except Exception:
        print("Error!!")
        if method.redelivered:
            print("Deleted")
            channel.basic_nack(method.delivery_tag, requeue=False)
        else:
            channel.basic_nack(method.delivery_tag, requeue=True)
        return

    if method.delivery_tag % 1 == 0:
        channel.basic_ack(method.delivery_tag, multiple=True)

channel.basic_consume(
    queue='q_broken',
    on_message_callback=on_message
)

while True:
    connection.process_data_events()