#! /usr/bin/python

import cProfile
from cmath import pi
import sys
import time
import pika
import pika.spec
import pika.connection
import pika.channel
import argparse
from threading import Thread

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
parser.add_argument('-s', '--connection-string', help="e.x. 'amqp://guest:guest@localhost:5672/%2F'", required=False, default=None)
parser.add_argument('-c', '--connection', required=False, default='localhost:5672')
parser.add_argument('-a', '--auth', '--authorisation', required=False, default='guest:guest')
parser.add_argument('-v', '--vhost', required=False, default='/')
parser.add_argument('-e', '--exchange', type=str, required=True, default='')
parser.add_argument('-r', '--routing-key', type=str, required=True)
parser.add_argument('-b', '--body', type=str, required=False, default='')
parser.add_argument('-H', '--header', action='append', type=str, required=False)

args = parser.parse_args()

host, port = args.connection.split(':', maxsplit=1)
port = int(port)

username, password = args.auth.split(':', maxsplit=1)

if args.connection_string is None:
    params = pika.ConnectionParameters(
                    host=host,
                    port=port,
                    virtual_host=args.vhost,
                    credentials=pika.PlainCredentials(
                        username=username,
                        password=password
                    )
                )
else:
    params = pika.URLParameters(args.connection_string)

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