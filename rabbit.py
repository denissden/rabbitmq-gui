from dataclasses import dataclass
import threading
import time
from typing import Any, Tuple
import pika
import pika.spec


def get_headers(text: str):
    headers = {}
    for line in text.split('\n'):
        kv = line.split(':', maxsplit=1)
        key = kv[0].strip()
        value = kv[1].strip() if len(kv) >= 2 else ''
        if key:
            headers[key] = value
    return headers


class Rabbit:
    def __init__(self, connection=None) -> None:
        self.connection = connection
        if connection is None:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self._new_channel()
        
        self.lock = threading.Lock()
        self.on_message = lambda _: ...
    
    @property
    def connection_params(self):
        return self.connection._impl.params
    
    def _new_channel(self):
        self.channel = self.connection.channel()

    def publish(self, exchange, routing_key, headers_text, body):
        headers = get_headers(headers_text)
        with self.lock:
            self.channel.basic_publish(
                exchange=exchange, 
                routing_key=routing_key, 
                body=body, 
                properties=pika.BasicProperties(
                    headers=headers
                )
            )

    def consume(self, q_name: str, retries=1) -> Tuple[bool, str]:
        try:
            self.channel.basic_consume(
                q_name, 
                on_message_callback=self._on_message_received, 
                auto_ack=True)
            return True, None
        except pika.exceptions.ChannelClosedByBroker as e:
            self._new_channel()
            return False, str(e)
        except IndexError as e:
            if retries > 0:
                return self.consume(q_name, retries - 1)
            return False, 'Some pika bug, sorry!'
            

    def _start_consuming(self):
        i = 0
        while True:
            with self.lock:
                try:
                    self.connection.process_data_events()
                except Exception as e:
                    print('Channel exception:', e)
                    break
            time.sleep(0.1)
            i += 1

    def start_consuming(self):
        t = threading.Thread(target=self._start_consuming)
        t.start()
        t.join(0)

    def _on_message_received(self, ch, method, properties, body):
        print(ch, method, properties, body)
        m = Message(self.channel, method, properties, body)
        self.on_message(m)

    def terminate(self):
        self.channel.stop_consuming()
        self.channel.close()
        self.connection.close()

@dataclass()
class Message:
    channel: pika.adapters.blocking_connection.BlockingChannel
    method: pika.spec.Basic.Deliver
    properties: pika.spec.BasicProperties
    body: bytes

    def text(self):
        return self.body.decode('utf-8')
