import threading
import time
from uuid import uuid4
import pika
import pika.spec
import json
import asyncio

class RpcClient:
    

    def __init__(self, params: pika.ConnectionParameters, timeout=5) -> None:
        self.timeout = timeout
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        declare_response = self.channel.queue_declare('', auto_delete=True)
        self.reply_queue = declare_response.method.queue

        self.waiting = {}
        self.lock = threading.Lock()
        self.loop = asyncio.get_event_loop()

        thread = threading.Thread(target=self._start_consuming)
        thread.daemon = True
        thread.start()
    
    def _start_consuming(self):
        self.channel.basic_consume(
            self.reply_queue, 
            on_message_callback=self._on_message,
            auto_ack=True)

        while True:
            with self.lock:
                self.connection.process_data_events()
            time.sleep(0.1)
    
    def _on_message(self, 
        ch, 
        method: pika.spec.Basic.Deliver, 
        properties: pika.spec.BasicProperties, 
        body):

        future = self.waiting.pop(properties.correlation_id, None)
        print('Received response', properties.correlation_id)
        if future is not None:
            self.loop.call_soon_threadsafe(future.set_result, body)
    
    def send_rpc(self, exchange, routing_key, body):
        future = asyncio.Future()
        corr_id = str(uuid4())

        properties = pika.BasicProperties()
        properties.correlation_id = corr_id
        properties.reply_to = self.reply_queue

        self.waiting[corr_id] = future

        with self.lock:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                properties=properties,
                body=body
            )

        asyncio.create_task(self._timeout(corr_id))
        return future
    
    async def _timeout(self, corr_id):
        await asyncio.sleep(self.timeout)
        future = self.waiting.pop(corr_id, None)

        if future is not None:
            print('Timeout', corr_id)
            self.loop.call_soon_threadsafe(future.set_result, None)
    

    
    
params = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    credentials=pika.PlainCredentials(
        username='guest',
        password='guest'
    )
)


async def main():
    rpc = RpcClient(params)
    async def print_info(login):
        request = { 'login': login }
        json_request = json.dumps(request)
        print(json_request)
        result = await rpc.send_rpc('x_topic', 'getUsers.rpc', json_request)
        print('Response', result)

    while True:
        login = input('Login: ')
        # asyncio.run(print_info(login))
        await print_info(login)

asyncio.run(main())