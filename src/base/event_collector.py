"""
    EventCollector
"""
import zsocket
import zhelpers
import interface
import zmq
import time
import location
import discovery
import types
import system
from local_log import *


class EventCollector():

    def __init__(self, event_types,
                       event_cback,
                       user_name="",
                       application_name=""):
        assert(event_cback is not None)
        assert(isinstance(event_cback, types.FunctionType) or
               isinstance(event_cback, types.MethodType))
        assert(isinstance(event_types, types.ListType))

        self.event_types = event_types
        self.user_name = user_name
        self.application_name = application_name
        self.event_cback = event_cback

        self.interface = interface.Interface(self.msg_cback)
        self.dc = discovery.DiscoveryClient(self.service_add,
                                            self.service_remove)

    @staticmethod
    def event_msg_parse(msg):
        event_type, sep, msg = msg.partition(" ")
        if event_type == "":
            Llog.LogError("Invalid event type received!")
            return None

        event_name, sep, msg = msg.partition(" ")
        if event_name == "":
            Llog.LogError("Invalid event name received!")
            return None

        timestamp, sep, msg = msg.partition(" ")
        if timestamp == "":
            Llog.LogError("Invalid timestamp received!")
            return None

        user_name, sep, msg = msg.partition(" ")
        if user_name == "":
            Llog.LogError("Invalid user name received!")
            return None

        app_name, sep, msg = msg.partition(" ")
        if app_name == "":
            Llog.LogError("Invalid application name received!")
            return None

        event = {'type': event_type,
                 'name': event_name,
                 'timestamp': timestamp,
                 'user_name': user_name,
                 'application_name': app_name,
                 'contents': msg}
        return event

    def msg_cback(self, msg):

        event = EventCollector.event_msg_parse(msg)
        if event is None:
            Llog.LogError("Could not parse event message!: " + str(msg))
            return
        self.event_cback(event)

    def service_add(self, service):
        # We have received a service location broadcast
        # message.  We now have to decide whether it is
        # an EVENT service, and if we would like to subscribe
        # to it, given our username and app_name settings.
        # If we have an empty username and/or appname, we
        # will subscribe to every EVENT service.

        if service.service_name != "EVENT":
            return

        if self.user_name != "":
            # We have been configured with a non-empty username.
            # Only subscribe to the service if it matches.
            if service.user_name != self.user_name:
                return

        if self.application_name != "":
            # We have been configured with a non-empty appname.
            # Only subscribe to the service if it matches.
            if service.application_name != self.application_name:
                return

        # We have found a matching service.  We must now get
        # the location of this service and open up a SUB socket
        # to it.  Once open, we must subscribe to the events
        # of interest.
        Llog.LogInfo("Subscribing to EVENT source: " + str(service))

        addr_info = location.parse_location(service.location)
        if addr_info is None:
            Llog.LogError("Invalid location: " + service.location)
            return

        zsock = zsocket.ZSocketClient(zmq.SUB,
                                      "tcp",
                                      addr_info['address'],
                                      addr_info['port'])
        assert(zsock is not None)
        zsock.connect()
        self.interface.add_socket(zsock)
        for event_type in self.event_types:
            if event_type == "*":
                # subscribe to everything, i.e the empty-string
                zsock.subscribe("")
            else:
                zsock.subscribe(str(event_type))

    def service_remove(self, service):
        zsocket = self.interface.find_socket_by_location(service.location)
        if zsocket is None:
            Llog.LogError("Cannot find zsocket: " + service.location)
            return

        self.interface.remove_socket(zsocket)
        zsocket.close()


def test1():

    user_name = "ddoucette"
    app_name = "mytestapp2"
    module_name = "event"
    source = EventSource("VALUE", "utilization", user_name, app_name)

    class MyTestClass():
        def __init__(self, user_name, app_name):
            self.got_message = False
            self.user_name = user_name
            self.app_name = app_name
            collector = EventCollector("VALUE",
                                       self.event_rcv_cback,
                                       user_name,
                                       app_name)

        def event_rcv_cback(self, event):
            assert(event['type'] == "VALUE")
            Llog.LogDebug("Got event: "
                          + event['type']
                          + " "
                          + event['contents'])
            self.got_message = True

    mtc = MyTestClass(user_name, app_name)

    # Give my test class a chance to receive a beacon from the event
    # source
    time.sleep(15)

    # Now send an event and wait a second to let the collector receive it
    source.send(12)
    time.sleep(1)

    assert(mtc.got_message is True)
    print "PASSED"


if __name__ == '__main__':

    from event_source import *
    test1()
