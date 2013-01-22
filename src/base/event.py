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
import system

from local_log import *


class EventSource(object):

    """
    """
    port_range = [7000, 8000]

    user_name = ""
    application_name = ""
    ip_addr = None
    zsocket = None
    interface = None
    discovery = None

    def __init__(self, event_name, event_type):
        assert(isinstance(event_name, types.StringType))
        assert(isinstance(event_type, types.StringType))

        self.event_type = event_type
        self.event_name = event_name

    def create_event_msg(self):

        timestamp = time.strftime("%x-%X")
        msg = " ".join([self.event_type,
                        self.event_name,
                        timestamp,
                        EventSource.user_name,
                        EventSource.application_name])
        return msg

    @staticmethod
    def Init(bind_addr="*"):
        # There is a single event socket for the application instance.
        assert(EventSource.zsocket is None)
        assert(EventSource.interface is None)
        assert(EventSource.ip_addr is None)

        EventSource.user_name = system.System.GetUserName()
        EventSource.application_name = system.System.GetApplicationName()

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

        # After creating the socket and the discovery server, we need
        # to pause here for a minute to allow remote services to get
        # our initial discovery beacon and subscribe.  If we dont wait here
        # our software may immediately send some start-up events which
        # will most definitely be lost because no one has had a chance
        # to subscribe
        time.sleep(3)


class EventLog(EventSource):

    def __init__(self, name):
        EventSource.__init__(self, name, EventSource.LOG)

    def send(self, contents):
        msg = self.create_event_msg()
        msg = " ".join([msg, contents])
        self.interface.push_in_msg(msg)


class EventValue(EventSource):

    def __init__(self, name):
        EventSource.__init__(self, name, EventSource.VALUE)

    def send(self, value):
        msg = self.create_event_msg()
        msg = " ".join([msg, str(value)])
        self.interface.push_in_msg(msg)


class EventBoolean(EventSource):

    def __init__(self, name):
        EventSource.__init__(self, name, EventSource.BOOLEAN)

    def send(self, boolean):
        assert(isinstance(boolean, types.BooleanType))
        msg = self.create_event_msg()
        msg = " ".join([msg, str(boolean)])
        self.interface.push_in_msg(msg)


class EventString(EventSource):

    def __init__(self, name):
        EventSource.__init__(self, name, EventSource.STRING)

    def send(self, strmsg):
        assert(isinstance(strmsg, types.StringType))
        msg = self.create_event_msg()
        msg = " ".join([msg, strmsg])
        self.interface.push_in_msg(msg)


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

    def msg_cback(self, msg):

        event_type, sep, msg = msg.partition(" ")
        if event_type == "":
            Llog.LogError("Invalid event type received!")
            return

        event_name, sep, msg = msg.partition(" ")
        if event_name == "":
            Llog.LogError("Invalid event name received!")
            return

        timestamp, sep, msg = msg.partition(" ")
        if timestamp == "":
            Llog.LogError("Invalid timestamp received!")
            return

        user_name, sep, msg = msg.partition(" ")
        if user_name == "":
            Llog.LogError("Invalid user name received!")
            return

        app_name, sep, msg = msg.partition(" ")
        if app_name == "":
            Llog.LogError("Invalid application name received!")
            return

        event = {'type': event_type,
                 'name': event_name,
                 'timestamp': timestamp,
                 'user_name': user_name,
                 'application_name': app_name,
                 'contents': msg}
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
            zsock.subscribe(str(event_type))

    def service_remove(self, service):
        zsocket = self.interface.find_socket_by_location(service.location)
        if zsocket is None:
            Llog.LogError("Cannot find zsocket: " + service.location)
            return

        self.interface.remove_socket(zsocket)
        zsocket.close()


def test1():

    source = EventValue("utilization")

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
    source.send(12)
    time.sleep(1)

    assert(mtc.got_message is True)
    print "PASSED"


def test2():

    # Create 3 sources, but only collect for one of the event
    # types.
    # Ensure we get all the events we sign up to collect

    source1 = EventValue("utilization")
    source2 = EventLog("mylog")
    source3 = EventBoolean("empty")

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
    source2.send("hello world")
    source1.send(12)
    source3.send(True)
    time.sleep(1)

    assert(mtc.got_message is True)
    print "PASSED"


def test3():

    # Subscribe to multiple sources.  Ensure we receive all events.

    source1 = EventValue("utilization")
    source2 = EventLog("mylog")
    source3 = EventBoolean("empty")
    source4 = EventString("progress")

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
    source2.send("hello world")
    source1.send(12)
    source3.send(True)
    source4.send("made it here...")
    source4.send("")
    time.sleep(1)

    assert(mtc.got_boolean is True)
    assert(mtc.got_value is True)
    assert(mtc.got_string is True)
    print "PASSED"


if __name__ == '__main__':

    user_name = "ddoucette"
    app_name = "mytestapp2"
    module_name = "event"

    system.System.Init(user_name, app_name, module_name)
    EventSource.Init()

    test1()
    test2()
    test3()
