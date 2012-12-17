"""
"""
import log
import time
from discovery import Discover
from interface import *


class LogCollector(Interface):

    """
        Log Collector class.  Implementation of a class which gathers
        log messages from all log sources.
    """
    def __init__(self, location="tcp://localhost:9932"):
        Interface.__init__(self, zmq.SUB, location, True)

        # Configure our discovery notifier object to notify us
        # when a log collector is discovered.  We will connect
        # up to all log collectors and publish messages to
        # them.
        self.discovery = Discover()
        self.service = {'name': 'LOG_COLLETOR', 'location': location}
        self.discovery.register_service(self.service)

        # Start up the interface
        self.start()

    @overrides(Interface)
    def process_msg(self, msg):
        pass

    @overrides(Interface)
    def process_protocol(self, msg):
        print "MSG: " + msg[0]


def test1():
    c = LogCollector()
    while True:
        time.sleep(1)


if __name__ == '__main__':
    pass
