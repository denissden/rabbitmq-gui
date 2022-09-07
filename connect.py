#! /usr/bin/python

from argparse import ArgumentParser
import os.path
import pika.connection
import pika
from pika.credentials import ExternalCredentials
import ssl

def path_exists(parser, arg):
    if not os.path.exists(arg):
        parser.error(f'Path {arg} does not exist')
    else:
        return arg
    
def add_connection_args(parser: ArgumentParser):
    parser.add_argument('-c', '--connection', required=False, default='localhost:5672')
    parser.add_argument('-a', '--auth', '--authorisation', required=False)
    parser.add_argument('-v', '--vhost', required=False, default='/')
    parser.add_argument('-CA', '--CAcert', type=lambda x: path_exists(parser, x), required=False)
    parser.add_argument('-C', '--cert', type=lambda x: path_exists(parser, x), required=False)
    parser.add_argument('-K', '--key', type=lambda x: path_exists(parser, x), required=False)

def create_params(args) -> pika.connection.Connection:
    host, port = args.connection.split(':', maxsplit=1)
    port = int(port)

    ca_cert = args.CAcert
    cert = args.cert
    key = args.key

    ssl_credentials = all((cert, key))
    
    ssl_options = None
    credentials = None
    username = None

    if ca_cert:
        context = ssl.create_default_context(cafile=ca_cert)
        if ssl_credentials:
            context.load_cert_chain(cert, key)
        print(context.__dict__)
        
        ssl_options = pika.SSLOptions(context, host)
    
    if args.auth is not None:
        username, password = args.auth.split(':', maxsplit=1)
        credentials = pika.PlainCredentials(
                            username=username,
                            password=password
                        )
    
    param_args = {
        'host': host,
        'port': port,
        'virtual_host': args.vhost,
    }

    if credentials:
        param_args['credentials'] = credentials

    if ssl_options:
        param_args['ssl_options'] = ssl_options
        if ssl_credentials:
            param_args['credentials'] = ExternalCredentials()

    print(param_args)

    return pika.ConnectionParameters(**param_args)
    

    


        




