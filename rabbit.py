from dataclasses import dataclass
import threading
import time
from typing import Any, Tuple
import pika
import pika.spec


def get_headers(text: str):
    """
    Turns a string of multiple lines into dict. 
    Key and value are reparated by first ':'.
    """
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
        self.default_connection = False
        self.connection = connection
        if connection is None:
            self.default_connection = True
            self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self._new_channel()
        
        self.lock = threading.Lock()
        self.on_message = lambda _: ...
    
    @property
    def connection_params(self):
        return self.connection._impl.params
    
    def _new_channel(self):
        self.channel = self.connection.channel()

    def publish(self, exchange, routing_key, headers, body) -> Tuple[bool, str]:
        """
        Publish a message.
        Consume error starts a new channel closing the previous one.
        """
        if type(headers) == str:
            headers = get_headers(headers)
        elif type(headers) != dict and headers is not None:
            raise TypeError('Headers are not dict or string')

        with self.lock:
            try:
                self.channel.basic_publish(
                    exchange=exchange, 
                    routing_key=routing_key, 
                    body=body, 
                    properties=pika.BasicProperties(
                        headers=headers
                    )
                )
                return True, None
            except pika.exceptions.ChannelClosedByBroker as e:
                self._new_channel()
                return False, str(e)
            except pika.exceptions.StrealLostError as e:
                self._new_channel()
                return False, str(e)

    def consume(self, q_name: str, prefetch=100, retries=1) -> Tuple[bool, str]:
        """
        Consume on a queue.
        Consume error starts a new channel closing the previous one.
        """
        try:
            self.channel.basic_qos(prefetch_count=prefetch)
            self.channel.basic_consume(
                q_name, 
                on_message_callback=self._on_message_received, 
                auto_ack=False)
            return True, None
        except pika.exceptions.ChannelClosedByBroker as e:
            self._new_channel()
            return False, str(e)
        except IndexError as e:
            if retries > 0:
                return self.consume(q_name, retries=retries - 1)
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
        """
        Start consuming on another thread.
        """
        t = threading.Thread(target=self._start_consuming)
        t.start()
        t.join(0)

    def _on_message_received(self, ch, method, properties, body):
        m = Message(self.channel, method, properties, body)
        self.channel.basic_ack(m.method.delivery_tag)
        self.on_message(m)

    def terminate(self):
        """
        Stop consuming and close a channel.
        Closes only default connections. Connection provided in __init__ can be used by multiple instances.
        """
        try:
            self.channel.stop_consuming()
        except pika.exceptions.ChannelClosedByBroker as e:
            print(e)
        self.channel.close()

        if self.default_connection:
            self.connection.close()

@dataclass()
class Message:
    """
    Received message
    """
    channel: pika.adapters.blocking_connection.BlockingChannel
    method: pika.spec.Basic.Deliver
    properties: pika.spec.BasicProperties
    body: bytes

    def text(self):
        return self.body.decode('utf-8')
