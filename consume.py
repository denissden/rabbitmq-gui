#! /usr/bin/python

import pika
import pika.spec
import pika.connection
import pika.channel
import argparse
import logging
from connect import add_connection_args, create_params

logging.basicConfig(level=logging.INFO, format='%(levelname) -10s %(asctime)s %(name)s : %(message)s')

parser = argparse.ArgumentParser("Publish messages to RabbitMQ")
parser.add_argument('-q', '--queue', type=str, required=True, default='')
add_connection_args(parser)

args = parser.parse_args()

host, port = args.connection.split(':', maxsplit=1)
port = int(port)

params = create_params(args)

def on_open(connection: pika.connection.Connection):
    logging.info('Connected')
    connection.channel(on_open_callback=on_channel_open)

def on_open_error(connection, error):
    logging.error('Could not open connection: %s %s', 
        connection, 
        error)
    connection.ioloop.stop()

def on_channel_open(channel: pika.channel.Channel):
    channel.add_on_close_callback(on_channel_close)
    channel.add_on_cancel_callback(on_cancel_consume)

    channel.basic_consume(args.queue, on_message_callback=on_message)
    
    # connection.close()

def on_message(channel: pika.channel.Channel, 
    method: pika.spec.Basic.Deliver, 
    properties: pika.spec.BasicProperties, 
    body):
    print('Message # %s - %s: %s; login = %s' % 
        (method.delivery_tag,
        method.routing_key,
        body.decode('utf-8'),
        properties.user_id))

    channel.basic_ack(method.delivery_tag)

def on_cancel_consume(method):
    logging.info('Received cancel from broker')
    connection.close()

def on_channel_close(channel: pika.channel.Channel, ex):
    logging.info('Channel closed: %s', ex)
    connection.ioloop.stop()

connection = pika.SelectConnection(parameters=params,
                                   on_open_callback=on_open,
                                   on_open_error_callback=on_open_error)
try:
    connection.ioloop.start()
except KeyboardInterrupt:
    logging.info('Received KeyboardInterrupt')
    connection.close()