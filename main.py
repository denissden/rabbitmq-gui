from abc import ABC, abstractmethod
from datetime import datetime
import json
from re import T
from typing import Dict
import PySimpleGUI as sg
from enum import Enum, auto
from rabbit import Rabbit, Message
import pika

class Keys(Enum):
    START_NEW_SEND = auto()
    START_NEW_CON = auto()
    START_CONNECT = auto()
    SEND_EXCH = auto()
    SEND_ROUTING_KEY = auto()
    SEND_HEADERS = auto()
    SEND_CONTENT = auto()
    SEND_BUTTON = auto()
    CON_TEXT_QUEUE = auto()
    CON_LISTBOX_MESSAGES = auto()
    CON_QUEUE = auto()
    CON_CONSUME = auto()
    CON_LAST_ROUTING_KEY = auto()
    CON_LAST_HEADERS = auto()
    CON_LAST_CONTENT = auto()
    CON_LAST_BUTTON = auto()
    CON_TEXT_LOG = auto()
    CONNECT_HOST = auto()
    CONNECT_PORT = auto()
    CONNECT_USER = auto()
    CONNECT_PASSWORD = auto()
    CONNECT_BUTTON = auto()




WINDOW_MAP: Dict[sg.Window, 'Window'] = {}
RABBIT_CONNECTION = None

class Window(ABC):
    @abstractmethod
    def show(self):
        pass

    @abstractmethod
    def process_events(self, event, values):
        pass

    def _show(self):
        WINDOW_MAP[self.window] = self
    
    def _close(self):
        del WINDOW_MAP[self.window]
        self.window.close()

class StartWindow(Window):
    def __init__(self) -> None:
        size = 20, 1
        self.layout = [
            [sg.Text('RabbitMQ Graphical client')],
            [sg.Button('Connect', size=size, key=Keys.START_CONNECT)],
            [sg.Button('Send a message', size=size, key=Keys.START_NEW_SEND)],
            [sg.Button('Consume a queue', size=size, key=Keys.START_NEW_CON)],
        ]

    def show(self):
        self.window = sg.Window("RabbitMQ graphical client", self.layout, finalize=True)
        self._show()
    
    def process_events(self, event, values):
        if event == Keys.START_NEW_SEND:
            SendWindow().show()
        elif event == Keys.START_NEW_CON:
            ConsumeWindow().show()
        elif event == Keys.START_CONNECT:
            ConnectWindow().show()

class SendWindow(Window):
    def __init__(self) -> None:
        name_size = 10, 1
        self.layout = [
            [sg.Text('Send messages')],
            [sg.Text('Exchange', size=name_size), sg.In(size=(25, 1), enable_events=True, key=Keys.SEND_EXCH)],
            [sg.Text('Routing key', size=name_size), sg.In(size=(25, 1), enable_events=True, key=Keys.SEND_ROUTING_KEY)],
            [sg.Text('Headers', size=name_size), sg.Multiline(size=(25, 4), enable_events=True, key=Keys.SEND_HEADERS)],
            [sg.Text('Content', size=name_size), sg.Multiline(size=(25, 2), enable_events=True, key=Keys.SEND_CONTENT)],
            [sg.Button('Send message', key=Keys.SEND_BUTTON)],
        ]

        self.rabbit = Rabbit(RABBIT_CONNECTION)
        self.info = f'{self.rabbit.connection_params.host}:{self.rabbit.connection_params.port}'

    def show(self):
        self.window = sg.Window(f'Publish a message ({self.info})', self.layout, finalize=True)
        self._show()
    
    def process_events(self, event, values):
        print(event, values)
        if event == Keys.SEND_BUTTON:
            self.rabbit.publish(
                values[Keys.SEND_EXCH],
                values[Keys.SEND_ROUTING_KEY],
                values[Keys.SEND_HEADERS],
                values[Keys.SEND_CONTENT],
            )

class ConsumeWindow(Window):
    def __init__(self) -> None:
        self.main_layout = [
            [sg.Text('Queue:', enable_events=True, key=Keys.CON_TEXT_QUEUE)],
            [sg.In(size=(20, 1), enable_events=True, key=Keys.CON_QUEUE), sg.Button('Consume', key=Keys.CON_CONSUME)],
            [sg.Listbox(size=(25, 10), values=[], enable_events=True, key=Keys.CON_LISTBOX_MESSAGES)],
        ]

        self.log_layout = [
            [sg.Text('Recent messages:')],
            [sg.Multiline(size=(25, 14), key=Keys.CON_TEXT_LOG)]
        ]

        self.layout = [
            [sg.Col(self.main_layout), sg.VSep(), sg.Col(self.log_layout)]
        ]

        self.rabbit = Rabbit(RABBIT_CONNECTION)
        self.messages: list[Message] = []
        self.messages_log: list[str] = []

        self.rabbit.start_consuming()
        self.rabbit.on_message = self._on_message
        self.info = f'{self.rabbit.connection_params.host}:{self.rabbit.connection_params.port}'

    def show(self):
        self.window = sg.Window(f'Consume on queue ({self.info})', self.layout, finalize=True)
        self._show()

    def process_events(self, event, values):
        print(event, values)
        if event == Keys.CON_CONSUME:
            q_name = values[Keys.CON_QUEUE]
            success, error = self.rabbit.consume(q_name)
            text = self.window[Keys.CON_TEXT_QUEUE]
            if success:
                text.update(q_name)
                self.window.set_title(f'{q_name} ({self.info})')
            else:
                text.update('Error! ' + error)
        # Close window event
        if event == None:
            self.rabbit.terminate()
            self._close()
    
    def _on_message(self, m: Message):
        self.messages.append(m)

        listbox = self.window[Keys.CON_LISTBOX_MESSAGES]
        listbox.update(datetime.now().strftime('%H:%M:%S') + ': ' + (msg.text() or '_no_content_') for msg in self.messages)

        log = self.window[Keys.CON_TEXT_LOG]
        self.messages_log.append(self._format_message(m))
        log.update("\n\n".join(self.messages_log))
    
    def _format_message(self, m: Message):
        return "\n".join([
            datetime.now().strftime('%H:%M:%S'),
            f'{m.method.routing_key} ({m.method.exchange or "Default"})',
            json.dumps(m.properties.headers, indent=2),
            m.text()
        ])

class ConnectWindow(Window):
    def __init__(self) -> None:
        name_size = 10, 1
        in_size = 25, 1
        self.layout = [
            [sg.Text('Send messages')],
            [sg.Text('Host', size=name_size), sg.In(default_text='localhost', size=in_size, enable_events=True, key=Keys.CONNECT_HOST)],
            [sg.Text('Port', size=name_size), sg.In(default_text='5672', size=in_size, enable_events=True, key=Keys.CONNECT_PORT)],
            [sg.Text('User', size=name_size), sg.In(default_text='guest', size=in_size, enable_events=True, key=Keys.CONNECT_USER)],
            [sg.Text('Password', size=name_size), sg.In(default_text='guest', size=in_size, enable_events=True, key=Keys.CONNECT_PASSWORD)],
            [sg.Button('Connect', key=Keys.CONNECT_BUTTON)],
        ]

        self.rabbit = Rabbit()

    def show(self):
        self.window = sg.Window("Connect to a RabbitMQ host", self.layout, finalize=True)
        self._show()
    
    def process_events(self, event, values):
        print(event, values)
        if event == Keys.CONNECT_BUTTON:
            params = pika.ConnectionParameters(
                host=values[Keys.CONNECT_HOST],
                port=values[Keys.CONNECT_PORT],
                credentials=pika.PlainCredentials(
                    username=values[Keys.CONNECT_USER],
                    password=values[Keys.CONNECT_PASSWORD]
                )
            )
            try:
                connection = pika.BlockingConnection(params)
                print(connection)
                global RABBIT_CONNECTION
                RABBIT_CONNECTION = connection
                self.window.set_title('Connection success')
            except Exception as e:
                self.window.set_title('Connection error')
                print('Connection error!', e)

def main_loop():
    StartWindow().show()
    while True:
        win, event, values = sg.read_all_windows()

        print(win, event, values)
        window_to_process = WINDOW_MAP[win]
        window_to_process.process_events(event, values)

main_loop()