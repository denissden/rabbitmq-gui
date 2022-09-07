#! /usr/bin/python

import pika
import pika.spec
import pika.connection
import pika.channel
import argparse
from connect import add_connection_args, create_params

def get_headers(headers_list: list[str] | None):
    headers = {}
    if headers_list is None:
        return headers
    for line in headers_list:
        kv = line.split(':', maxsplit=1)
        key = kv[0].strip()
        value = kv[1].strip() if len(kv) >= 2 else None
        if key:
            headers[key] = value
    return headers

parser = argparse.ArgumentParser("Publish messages to RabbitMQ")
parser.add_argument('-e', '--exchange', type=str, required=True, default='')
parser.add_argument('-r', '--routing-key', type=str, required=True)
parser.add_argument('-u', '--user-id', help='user_id value in properties', type=str, required=False, default=None)
parser.add_argument('-b', '--body', type=str, required=False, default='')
parser.add_argument('-H', '--header', action='append', type=str, required=False)
add_connection_args(parser)

args = parser.parse_args()

user_id = None

params = create_params(args)

def on_open(connection: pika.connection.Connection):
    connection.channel(on_open_callback=on_channel_open)

def on_open_error(connection, error):
    print("Could not open connection")
    print(connection)
    print(error)
    connection.ioloop.stop()

def on_channel_open(channel: pika.channel.Channel):
    channel.add_on_close_callback(on_channel_close)
    properties = pika.BasicProperties(
        content_type='text/plain',
        headers=get_headers(args.header),
        user_id=user_id
    )
    channel.basic_publish(
        exchange=args.exchange, 
        routing_key=args.routing_key, 
        properties=properties,
        body=args.body)
    
    connection.close()

def on_channel_close(channel: pika.channel.Channel, ex):
    print(ex)
    connection.ioloop.stop()

connection = pika.SelectConnection(parameters=params,
                                   on_open_callback=on_open,
                                   on_open_error_callback=on_open_error)
connection.ioloop.start()