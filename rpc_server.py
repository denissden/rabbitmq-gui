import pika
import pika.spec
import json

USERS = {
    'john13': {
        'name': 'John Evelinne',
        'born': 1992,
        'city': 'New York'
    },
    'pink_kitty': {
        'name': 'Ann Downing',
        'born': 2000,
        'city': 'Berlin'
    },
    'h4x0r': {
        'name': 'Ivan Skhodnenko',
        'born': 1989,
        'city': 'Voronezh'
    }
}

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

def on_message(ch, 
    method: pika.spec.Basic.Deliver, 
    properties: pika.spec.BasicProperties, 
    body):

    # получаем запрос и находим пользователя
    request = json.loads(body)
    login = request['login']
    result = USERS.get(login, None)
    body = json.dumps(result)
    print(properties.correlation_id, body)

    # отправляем ответ обратно в сервис
    props = pika.BasicProperties()
    props.correlation_id = properties.correlation_id
    channel.basic_publish(
        exchange='', 
        routing_key=properties.reply_to, 
        properties=props,
        body=body)
    channel.basic_ack(method.delivery_tag, multiple=False)

channel.basic_consume(
    queue='q_users',
    on_message_callback=on_message
)

while True:
    connection.process_data_events()