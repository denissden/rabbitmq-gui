from abc import ABC, abstractmethod
from datetime import datetime
import json
from re import T
from typing import Dict
import PySimpleGUI as sg
from enum import Enum, auto
from rabbit import Rabbit, Message
from localization import Locale
import pika

class Keys(Enum):
    LANG_EN = auto()
    LANG_RU = auto()
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
    """
    Window base class.
    """
    @abstractmethod
    def show(self):
        """
        Create and show a window here.
        Set (finalize=True) to show a window instantly.
        """
        pass

    @abstractmethod
    def process_events(self, event, values):
        """
        PySimpleGUI event for this particular window.
        """
        pass

    @abstractmethod
    def init_layout(self):
        """
        Set self.layout here.
        Call in __init__.
        """
        pass

    def _show(self):
        WINDOW_MAP[self.window] = self
    
    def _close(self):
        del WINDOW_MAP[self.window]
        self.window.close()
    
    def restart_ui(self):
        """
        Restarts the window and creates layout again.
        """
        self.init_layout()
        self._close()
        self.show()

class StartWindow(Window):
    def __init__(self) -> None:
        self.init_layout()

    def init_layout(self):
        size = 20, 1
        self.layout = [
            [sg.Text(Locale.TITLE_MAIN)],
            [sg.Button(Locale.CONNECT, size=size, key=Keys.START_CONNECT)],
            [sg.Button(Locale.PUBLISH_ONE, size=size, key=Keys.START_NEW_SEND)],
            [sg.Button(Locale.CONSUME_FROM_QUEUE, size=size, key=Keys.START_NEW_CON)],
            [sg.Text(Locale.LANGUAGE)],
            [
                sg.Button('en', key=Keys.LANG_EN), 
                sg.Button('ru', key=Keys.LANG_RU),
            ],
        ]

    def show(self):
        self.window = sg.Window(Locale.TITLE_MAIN, self.layout, finalize=True)
        self._show()
    
    def process_events(self, event, values):
        if event == Keys.START_NEW_SEND:
            SendWindow().show()
        elif event == Keys.START_NEW_CON:
            ConsumeWindow().show()
        elif event == Keys.START_CONNECT:
            ConnectWindow().show()
        
        elif event == Keys.LANG_EN:
            Locale.set(Locale.en)
            self._refresh_all_windows()
        elif event == Keys.LANG_RU:
            Locale.set(Locale.ru)
            self._refresh_all_windows()
        
        if event is None and len(WINDOW_MAP) == 1:
            self._close()
        
    def _refresh_all_windows(self):
        windows_to_update = list(WINDOW_MAP.values())
        for window in windows_to_update:
            window.restart_ui()

class SendWindow(Window):
    def __init__(self) -> None:
        self.init_layout()

        self.rabbit = Rabbit(RABBIT_CONNECTION)
        self.info = f'{self.rabbit.connection_params.host}:{self.rabbit.connection_params.port}'


    def init_layout(self):
        name_size = 10, 1
        self.layout = [
            [sg.Text(Locale.PUBLISH_MANY)],
            [sg.Text(Locale.EXCHANGE, size=name_size), sg.In(size=(25, 1), enable_events=True, key=Keys.SEND_EXCH)],
            [sg.Text(Locale.ROUTING_KEY, size=name_size), sg.In(size=(25, 1), enable_events=True, key=Keys.SEND_ROUTING_KEY)],
            [sg.Text(Locale.HEADERS, size=name_size), sg.Multiline(size=(25, 4), enable_events=True, key=Keys.SEND_HEADERS)],
            [sg.Text(Locale.BODY, size=name_size), sg.Multiline(size=(25, 2), enable_events=True, key=Keys.SEND_CONTENT)],
            [sg.Button(Locale.PUBLISH_ONE, key=Keys.SEND_BUTTON)],
        ]

    def show(self):
        self.window = sg.Window(f'{Locale.PUBLISH_ONE} ({self.info})', self.layout, finalize=True)
        self._show()
    
    def process_events(self, event, values):
        print(event, values)
        if event == Keys.SEND_BUTTON:
            success, error = self.rabbit.publish(
                values[Keys.SEND_EXCH],
                values[Keys.SEND_ROUTING_KEY],
                values[Keys.SEND_HEADERS],
                values[Keys.SEND_CONTENT],
            )
            if success:
                self.window.set_title(Locale.MESSAGE_SENT)
            else:
                self.window.set_title(f'{Locale.ERROR} {error}')
        
        if event is None:
            self._close()

class ConsumeWindow(Window):
    def __init__(self) -> None:
        self.init_layout()

        self.rabbit = Rabbit(RABBIT_CONNECTION)
        self.messages: list[Message] = []
        self.messages_log: list[str] = []

        self.rabbit.start_consuming()
        self.rabbit.on_message = self._on_message
        self.info = f'{self.rabbit.connection_params.host}:{self.rabbit.connection_params.port}'
        self.title = f'{Locale.CONSUME_FROM_QUEUE} ({self.info})'


    def init_layout(self):
        self.main_layout = [
            [sg.Text(f'{Locale.QUEUE}:', enable_events=True, key=Keys.CON_TEXT_QUEUE)],
            [sg.In(size=(20, 1), enable_events=True, key=Keys.CON_QUEUE), sg.Button(Locale.CONSUME, key=Keys.CON_CONSUME)],
            [sg.Listbox(size=(25, 10), values=[], enable_events=True, key=Keys.CON_LISTBOX_MESSAGES)],
        ]

        self.log_layout = [
            [sg.Text(f'{Locale.RECENT_MESSAGES}:')],
            [sg.Multiline(size=(25, 14), autoscroll=True, key=Keys.CON_TEXT_LOG)]
        ]

        self.layout = [
            [sg.Col(self.main_layout), sg.VSep(), sg.Col(self.log_layout)]
        ]

    def show(self):
        self.window = sg.Window(self.title, self.layout, finalize=True)
        self._show()

    def process_events(self, event, values):
        print(event, values)
        if event == Keys.CON_CONSUME:
            q_name = values[Keys.CON_QUEUE]

            # queues are added to a channel until there is an error
            # after that Rabbit will create a new channel with no queues
            success, error = self.rabbit.consume(q_name)
            text = self.window[Keys.CON_TEXT_QUEUE]
            if success:
                text.update(q_name)
                self.title = f'{q_name} ({self.info})'
                self.window.set_title(self.title)
            else:
                text.update(f'{Locale.ERROR} {error}')
        # Close window event
        if event == None:
            self.rabbit.terminate()
            self._close()
    
    def _on_message(self, m: Message):
        self.messages.append((m, datetime.now()))
        self.messages_log.append(self._format_message(m))
        self._update_ui()
       
    def _update_ui(self):
        """
        Update messages log text and listbox values.
        """
        listbox = self.window[Keys.CON_LISTBOX_MESSAGES]
        stripped_messages = self.messages[-10:]
        listbox.update(msg_time.strftime('%H:%M:%S') + ': ' + (msg.text() or '_no_content_') for msg, msg_time in stripped_messages)
        
        log = self.window[Keys.CON_TEXT_LOG]
        log.update("\n\n".join(self.messages_log))
    
    def _format_message(self, m: Message):
        return "\n".join([
            datetime.now().strftime('%H:%M:%S'),
            f'{m.method.routing_key} ({m.method.exchange or "Default"})',
            json.dumps(m.properties.headers, indent=2),
            m.text()
        ])
    
    def restart_ui(self):
        super().restart_ui()
        self._update_ui()

class ConnectWindow(Window):
    def __init__(self) -> None:
        self.init_layout()

        self.rabbit = Rabbit()

    def init_layout(self):
        name_size = 10, 1
        in_size = 25, 1
        self.layout = [
            [sg.Text(Locale.PUBLISH_MANY)],
            [sg.Text(Locale.HOST, size=name_size), sg.In(default_text='localhost', size=in_size, enable_events=True, key=Keys.CONNECT_HOST)],
            [sg.Text(Locale.PORT, size=name_size), sg.In(default_text='5672', size=in_size, enable_events=True, key=Keys.CONNECT_PORT)],
            [sg.Text(Locale.USER, size=name_size), sg.In(default_text='guest', size=in_size, enable_events=True, key=Keys.CONNECT_USER)],
            [sg.Text(Locale.PASSWORD, size=name_size), sg.In(default_text='guest', size=in_size, enable_events=True, key=Keys.CONNECT_PASSWORD)],
            [sg.Button(Locale.CONNECT, key=Keys.CONNECT_BUTTON)],
        ]

    def show(self):
        self.window = sg.Window(Locale.CONNECT_TO_HOST, self.layout, finalize=True)
        self._show()
    
    def process_events(self, event, values):
        print(event, values)
        if event == Keys.CONNECT_BUTTON:
            # Minimal info needed to connect
            params = pika.ConnectionParameters(
                host=values[Keys.CONNECT_HOST],
                port=values[Keys.CONNECT_PORT],
                credentials=pika.PlainCredentials(
                    username=values[Keys.CONNECT_USER],
                    password=values[Keys.CONNECT_PASSWORD]
                )
            )

            # Create a global connection for Rabbit instances to use
            try:
                connection = pika.BlockingConnection(params)
                print(connection)
                global RABBIT_CONNECTION
                RABBIT_CONNECTION = connection
                self.window.set_title(Locale.CONNECTION_SUCCESS)
            except Exception as e:
                self.window.set_title(Locale.CONNECTION_ERROR)
                print(Locale.CONNECTION_ERROR, e)
        
        if event is None:
            self._close()

def main_loop():
    StartWindow().show()
    while WINDOW_MAP:
        win, event, values = sg.read_all_windows()

        print(win, event, values)
        window_to_process = WINDOW_MAP[win]
        window_to_process.process_events(event, values)

    RABBIT_CONNECTION.close()

if __name__ == '__main__':
    Locale.set(Locale.en)
    main_loop()