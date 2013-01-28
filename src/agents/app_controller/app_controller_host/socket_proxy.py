
import event_source
import zsocket
import vitals
import os

class SocketProxy(object):
    """
        The SocketProxy object creates PUSH/PULL sockets to
        provide the application with a commmunication path
        for events and control messages.
        This object creates a PULL socket to receive EVENT messages.

        All events are parsed and re-issued through an EventSource
        object to ensure the messages are well formed.
    """
    def __init__(self, user_name, application_name):
        self.user_name = user_name
        self.application_name = application_name
        self.socket_address = "event-proxy-%d.ipc" % os.getpid()
        self.socket_event_pull = zsocket.ZSocketServer(zmq.PULL,
                                                       "ipc",
                                                       self.socket_address)
        self.socket_event_pull.bind()

        self.event_interface = Interface(self.process_event_rx_msg)
        self.event_interface.add_socket(self.socket_event_pull)

    def process_event_rx_msg(self, msg):
        pass
