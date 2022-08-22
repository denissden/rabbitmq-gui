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

DELAY = 0.5
MODE = 'ack'
AUTO_ACK = False

def _on_message(ch, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, body):
    text = f'{method.routing_key} ({method.exchange or "Default"})'
    text += ' '
    # text += json.dumps(properties.headers)
    text += ' '
    text += f'{method.delivery_tag=}'
    text += ' '
    text += f'{"Redelivered" if method.redelivered else ""}'
    print(text)

    def wrapper():
        return wait_delay(method)

    t = threading.Thread(target=wrapper)
    t.start()
    t.join(0)
    

def wait_delay(method):
    time.sleep(DELAY)

    if AUTO_ACK:
        return
    
    mode = MODE
    
    if MODE == 'random':
        mode = random.choice(['ack', 'nack', 'nack_requeue'])

    with lock:
        if mode == 'ack':
            channel.basic_ack(method.delivery_tag)
        elif mode == 'reject':
            channel.basic_reject(method.delivery_tag)
        elif mode == 'nack':
            channel.basic_nack(method.delivery_tag, requeue=False)
        elif mode == 'nack_requeue':
            channel.basic_nack(method.delivery_tag, requeue=True)


def consume(queue, auto_ack, prefetch):
    print(f"Consuming on {queue}, {auto_ack=} {MODE=} {prefetch=}, {DELAY=}")

    channel.basic_qos(prefetch_count=prefetch)
    channel.basic_consume(
        queue=queue,
        auto_ack=AUTO_ACK,
        on_message_callback=_on_message
    )

    while True:
        with lock:
        # try:
            connection.process_data_events()
        # except Exception as e:
        #     print('Channel exception:', e)
        #     break

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser("Consume on a queue")
    parser.add_argument('-q', '--queue', required=True, default='')
    parser.add_argument('-a', '--auto-ack', type=bool, required=False, default=False)
    parser.add_argument('-d', '--delay', type=float, required=False, default=0.5)
    parser.add_argument('-m', '--mode', required=False, default='ack', choices=['ack', 'reject', 'nack', 'nack_requeue', 'random'])
    parser.add_argument('-p', '--prefetch', type=int, required=False, default=10)

    args = parser.parse_args()

    DELAY = args.delay
    MODE = args.mode
    AUTO_ACK = args.auto_ack
    consume(args.queue, args.auto_ack, args.prefetch)


    