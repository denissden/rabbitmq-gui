import pika

params = pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=pika.PlainCredentials(
                    username='guest',
                    password='guest'
                )
            )


