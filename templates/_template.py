import pika
import pika.spec
import json
import time
import random
import threading

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

lock = threading.Lock()

def _on_message(ch, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, body):
    def wrapper():
        return on_message(ch, method, properties, body)

    t = threading.Thread(target=wrapper)
    t.start()
    t.join(0)
    
def consume(queue, auto_ack, prefetch):
    print(f"Consuming on {queue}, {auto_ack=} {prefetch=}, {DELAY=}")

    channel.basic_qos(prefetch_count=prefetch)
    channel.basic_consume(
        queue=queue,
        auto_ack=AUTO_ACK,
        on_message_callback=_on_message
    )

    while True:
        with lock:
            connection.process_data_events()


QUEUE = ''
AUTO_ACK = False
PREFETCH = 10

def on_message(ch, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, body):

    #############
    # CODE HERE #
    #############
    
    time.sleep(1)

    if AUTO_ACK:
        return

    with lock:   
        channel.basic_ack(method.delivery_tag)

if __name__ == '__main__':
    consume(QUEUE, AUTO_ACK, PREFETCH)


    