"""
    EventSource
    EventCollector

    Classes implementing basic event broadcast and collection.

    Event sources are advertised using the DiscoveryClient class which
    broadcasts the existence of an event source, as well as the username/
    application name owning the event source.

    Events are distributed via a PUB server.  The format of each event
    packet is as follows:

    <event type> <timestamp> <username> <application name> [contents]

    By placing the event type first in the packet, it allows the event
    collector to filter event messages using event type and the built-in
    subscription functionalities with a ZMQ PUB/SUB socket.

    The event channels are by design per-username/application, so there is
    no need to filter using these fields.  If the listener does not want
    events for this username/application_name, they should not have
    connected to this server.

"""
import zsocket
import zhelpers
import interface
import zmq
import time
import location
import discovery
import types
from local_log import *


class EventSource():

    """
    """
    LOG = "LOG"
    BOOLEAN = "BOOLEAN"
    VALUE = "VALUE"
    STRING = "STRING"

    event_types = [LOG, BOOLEAN, VALUE, STRING]

    port_range = [7000, 8000]

    user_name = ""
    application_name = ""
    ip_addr = None
    zsocket = None
    interface = None
    discovery = None

    def __init__(self, event_name, event_type):
        assert(event_type in EventSource.event_types)
        assert(event_name is not None)
        self.event_type = event_type
        self.event_name = event_name

    def __create_event_msg(self):

        timestamp = time.strftime("%x-%X")
        msg = " ".join([self.event_type,
                        self.event_name,
                        timestamp,
                        EventSource.user_name,
                        EventSource.application_name])
        return msg

    def send_value(self, value):
        assert(self.event_type == EventSource.VALUE)
        msg = self.__create_event_msg()
        msg = " ".join([msg, str(value)])
        self.interface.push_in_msg(msg)

    def send_log(self, contents):
        assert(self.event_type == EventSource.LOG)
        msg = self.__create_event_msg()
        msg = " ".join([msg, contents])
        self.interface.push_in_msg(msg)

    def send_boolean(self, boolean):
        assert(self.event_type == EventSource.BOOLEAN)
        assert(isinstance(boolean, types.BooleanType))
        msg = self.__create_event_msg()
        msg = " ".join([msg, str(boolean)])
        self.interface.push_in_msg(msg)

    def send_string(self, string):
        assert(self.event_type == EventSource.STRING)
        msg = self.__create_event_msg()
        msg = " ".join([msg, string])
        self.interface.push_in_msg(msg)

    @staticmethod
    def Init(user_name, application_name, bind_addr="*"):
        assert(EventSource.zsocket is None)
        assert(EventSource.interface is None)
        assert(EventSource.ip_addr is None)

        EventSource.user_name = user_name
        EventSource.application_name = application_name

        EventSource.zsocket = zsocket.ZSocketServer(zmq.PUB,
                                                    "tcp",
                                                    bind_addr,
                                                    EventSource.port_range)
        EventSource.zsocket.bind()
        EventSource.interface = interface.Interface()
        EventSource.interface.add_socket(EventSource.zsocket)

        if EventSource.ip_addr is None:
            EventSource.ip_addr = zhelpers.get_local_ipaddr()

        # Create a discovery object to broadcast the existence
        # of this event
        service_location = location.create_location("tcp",
                                                    EventSource.ip_addr,
                                                    EventSource.zsocket.port)

        EventSource.discovery = discovery.DiscoveryServer(
                                                EventSource.user_name,
                                                EventSource.application_name,
                                                "EVENT",
                                                service_location)
        assert(EventSource.discovery is not None)

    @staticmethod
    def Create(event_name, event_type):
        return EventSource(event_name, event_type)


class EventCollector():

    def __init__(self, event_types,
                       event_cback,
                       user_name="",
                       application_name=""):
        assert(event_cback is not None)

        for event_type in event_types:
            assert(event_type in EventSource.event_types)

        self.event_types = event_types
        self.user_name = user_name
        self.application_name = application_name
        self.event_cback = event_cback

        self.interface = interface.Interface(self.msg_cback)
        self.dc = discovery.DiscoveryClient(self.service_add,
                                            self.service_remove)

    def msg_cback(self, msg):
        pieces = msg.split()
        if len(pieces) < 6:
            Llog.LogError("Invalid EVENT message received! <" + msg + ">")
            return

        event_type = pieces[0]
        event_name = pieces[1]
        timestamp = pieces[2]
        user_name = pieces[3]
        application_name = pieces[4]
        contents = pieces[5]

        if event_type not in self.event_types:
            Llog.LogError("Received event type <"
                          + event_type + "> without a subscription!")
            return

        event = {'type': event_type,
                 'name': event_name,
                 'timestamp': timestamp,
                 'user_name': user_name,
                 'application_name': application_name,
                 'contents': contents}
        self.event_cback(event)

    def service_add(self, service):
        if service.service_name == "EVENT" and \
           service.user_name == self.user_name:
            if service.application_name != "" and \
                service.application_name != self.application_name:
                # service is for another user
                return

            # We have found a matching service.  We must now get
            # the location of this service and open up a SUB socket
            # to it.  Once open, we must subscribe to the events
            # of interest.
            Llog.LogInfo("Found service: " + str(service))

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
                zsock.subscribe(str(event_type))

    def service_remove(self, service):
        zsocket = self.interface.find_socket_by_location(service.location)
        if zsocket is None:
            Llog.LogError("Cannot find zsocket: " + service.location)
            return

        self.interface.remove_socket(zsocket)
        zsocket.close()


def test1():

    source = EventSource.Create("utilization", EventSource.VALUE)

    class MyTestClass():
        def __init__(self, user_name, app_name):
            self.got_message = False
            self.user_name = user_name
            self.app_name = app_name
            collector = EventCollector([EventSource.VALUE],
                                       self.event_rcv_cback,
                                       user_name,
                                       app_name)

        def event_rcv_cback(self, event):
            assert(event['type'] == EventSource.VALUE)
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
    source.send_value(12)
    time.sleep(1)

    assert(mtc.got_message is True)
    print "PASSED"


def test2():

    # Create 3 sources, but only collect for one of the event
    # types.
    # Ensure we get all the events we sign up to collect

    source1 = EventSource.Create("utilization", EventSource.VALUE)
    source2 = EventSource.Create("mylog", EventSource.LOG)
    source3 = EventSource.Create("empty", EventSource.BOOLEAN)

    class MyTestClass():
        def __init__(self, user_name, app_name):
            self.got_message = False
            self.user_name = user_name
            self.app_name = app_name
            collector = EventCollector([EventSource.VALUE],
                                       self.event_rcv_cback,
                                       user_name,
                                       app_name)

        def event_rcv_cback(self, event):
            assert(event['type'] == EventSource.VALUE)
            Llog.LogDebug("Got event: "
                          + event['type']
                          + " "
                          + event['contents'])
            self.got_message = True

    mtc = MyTestClass(user_name, app_name)

    # Give my test class a chance to receive a beacon from the event
    # sources
    time.sleep(15)

    # Now send an event out each source.
    source2.send_log("hello world")
    source1.send_value(12)
    source3.send_boolean(True)
    time.sleep(1)

    assert(mtc.got_message is True)
    print "PASSED"


def test3():

    # Subscribe to multiple sources.  Ensure we receive all events.

    source1 = EventSource.Create("utilization", EventSource.VALUE)
    source2 = EventSource.Create("mylog", EventSource.LOG)
    source3 = EventSource.Create("empty", EventSource.BOOLEAN)
    source4 = EventSource.Create("progress", EventSource.STRING)

    class MyTestClass():
        def __init__(self, user_name, app_name):
            self.got_value = False
            self.got_boolean = False
            self.got_string = False
            self.user_name = user_name
            self.app_name = app_name
            collector = EventCollector([EventSource.VALUE,
                                        EventSource.BOOLEAN,
                                        EventSource.STRING],
                                       self.event_rcv_cback,
                                       user_name,
                                       app_name)

        def event_rcv_cback(self, event):
            Llog.LogDebug("Received event: "
                          + event['type']
                          + " "
                          + event['contents'])
            if event['type'] == EventSource.VALUE:
                self.got_value = True
            elif event['type'] == EventSource.BOOLEAN:
                self.got_boolean = True
            elif event['type'] == EventSource.STRING:
                self.got_string = True
            else:
                Llog.LogError("Receive invalid event type: " + event['type'])
                assert(False)

    mtc = MyTestClass(user_name, app_name)

    # Give my test class a chance to receive a beacon from the event
    # sources
    time.sleep(15)

    # Now send an event out each source.
    source2.send_log("hello world")
    source1.send_value(12)
    source3.send_boolean(True)
    source4.send_string("made it here...")
    source4.send_string("")
    time.sleep(1)

    assert(mtc.got_boolean is True)
    assert(mtc.got_value is True)
    assert(mtc.got_string is True)
    print "PASSED"


if __name__ == '__main__':

    user_name = "ddoucette"
    app_name = "mytestapp2"

    EventSource.Init(user_name, app_name)

    #test1()
    #test2()
    test3()
