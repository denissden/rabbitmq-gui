from enum import Enum, auto
from sre_constants import SUCCESS


class Locale(Enum):
    LANGUAGE = auto()
    ERROR = auto()
    TITLE_MAIN = auto()
    CONSUME_FROM_QUEUE = auto()
    QUEUE = auto()
    CONSUME = auto()
    EXCHANGE = auto()
    ROUTING_KEY = auto()
    HEADERS = auto()
    BODY = auto()
    PUBLISH = auto()
    PUBLISH_ONE = auto()
    PUBLISH_MANY = auto()
    RECENT_MESSAGES = auto()
    HOST = auto()
    PORT = auto()
    USER = auto()
    PASSWORD = auto()
    CONNECT = auto()
    CONNECT_TO_HOST = auto()
    CONNECTION_SUCCESS = auto()
    CONNECTION_ERROR = auto()

    def __str__(self) -> str:
        self.str_called = True
        return Locale.get(self)
    
    en = {
        LANGUAGE: 'Language (restarts all windows)',

        ERROR: 'Error!',

        TITLE_MAIN: 'RabbitMQ Graphical client',
        CONSUME_FROM_QUEUE: 'Consume from a queue',
        QUEUE: 'Queue',
        CONSUME: 'Consume',


        EXCHANGE: 'Exchange',
        ROUTING_KEY: 'Routing key',
        HEADERS: 'Headers',
        BODY: 'Content',

        PUBLISH: 'Publish',
        PUBLISH_ONE: 'Publish a message',
        PUBLISH_MANY: 'Publish messages',
        RECENT_MESSAGES: 'Recent messages',
        HOST: 'Host',
        PORT: 'Port',
        USER: 'User',
        PASSWORD: 'Password',
        CONNECT: 'Connect',
        CONNECT_TO_HOST: 'Connect to a RabbitMQ host',
        CONNECTION_SUCCESS: 'Connection success',
        CONNECTION_ERROR: 'Connection error',
    }
    ru = {
        LANGUAGE: 'Язык (обновляет все окна)',

        ERROR: 'Ошибка!',

        TITLE_MAIN: 'RabbitMQ - графический клиент',
        CONSUME_FROM_QUEUE: 'Подключиться к очереди',
        QUEUE: 'Очередь',
        CONSUME: 'Подключиться',

        EXCHANGE: 'Обменник',
        ROUTING_KEY: 'Топик',
        HEADERS: 'Хедеры',
        BODY: 'Содержимое',

        PUBLISH: 'Отправить',
        PUBLISH_ONE: 'Отправить сообщение',
        PUBLISH_MANY: 'Отправить сообщения',
        RECENT_MESSAGES: 'Недавние сообщения',
        HOST: 'Адрес',
        PORT: 'Порт',
        USER: 'Пользователь',
        PASSWORD: 'Пароль',
        CONNECT: 'Подключиться',
        CONNECT_TO_HOST: 'Подключиться к серверу RabbitMQ',
        CONNECTION_SUCCESS: 'Подключено',
        CONNECTION_ERROR: 'Ошибка подключения',
    }

    @classmethod
    def set(self, loc: dict):
        global _current_locale
        _current_locale = loc

    @classmethod
    def get(self, key: 'Locale') -> str:
        key_value = key.value
        if type(key_value) == dict:
            return r'%wrong member%'
        return _current_locale.value.get(key_value, r'%no text%')

_current_locale = Locale.en

if __name__ == '__main__':
    print(Locale.ERROR)
    Locale.set(Locale.ru)
    print(Locale.ERROR)