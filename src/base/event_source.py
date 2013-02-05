"""
    EventSource

    Classes implementing basic event broadcast

    Event sources are advertised using the DiscoveryClient class which
    broadcasts the existence of an event source, as well as the user_name/
    application name owning the event source.

    Events are distributed via a PUB server.  The format of each event
    packet is as follows:

    <event type> <timestamp> <user_name> <application name> [contents]

    By placing the event type first in the packet, it allows the event
    collector to filter event messages using event type and the built-in
    subscription functionalities with a ZMQ PUB/SUB socket.

    The event channels are by design per-user_name/application, so there is
    no need to filter using these fields.  If the listener does not want
    events for this user_name/application_name, they should not have
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


class EventSocket(object):
    """
        Create the SUB server and interface to process and
        send all EVENT messages.
    """
    port_range = [7000, 8000]
    init_sleep_period = 5

    def __init__(self, user_name, application_name):

        self.user_name = user_name
        self.application_name = application_name

        self.zsocket = zsocket.ZSocketServer(zmq.PUB,
                                             "tcp",
                                             "*",
                                             self.port_range,
                                             "EVENT")
        self.zsocket.bind()
        self.interface = interface.Interface()
        self.interface.add_socket(self.zsocket)
        self.ip_addr = zhelpers.get_local_ipaddr()

        # Create a discovery object to broadcast the existence
        # of this event on this socket.
        service_location = location.create_location("tcp",
                                                    self.ip_addr,
                                                    self.zsocket.port)

        self.discovery = discovery.DiscoveryServer(
                                                user_name,
                                                application_name,
                                                "EVENT",
                                                service_location)
        assert(self.discovery is not None)

        # After creating the socket and the discovery server, we need
        # to pause here for a minute to allow remote services to get
        # our initial discovery beacon and subscribe.  If we dont wait here
        # our software may immediately send some start-up events which
        # will most definitely be lost because no one has had a chance
        # to subscribe
        time.sleep(self.init_sleep_period)

    def send(self, msg):
        self.interface.push_in_msg(msg)


class EventSource(object):

    """
        The EventSource class is the base class for all event type
        objects.
        The EventSource class creates the socket layer and discovery
        mechanisms for the event and provides an API to send event
        messages.
    """
    sockets = []

    def __init__(self, event_name, event_type, user_name, application_name):
        assert(isinstance(event_name, types.StringType))
        assert(isinstance(event_type, types.StringType))

        self.event_type = event_type
        self.event_name = event_name
        self.user_name = user_name
        self.application_name = application_name

        self.socket = EventSource.GetSocket(user_name, application_name)
        assert(self.socket is not None)

    def send(self, contents):
        assert(isinstance(contents, types.ListType))
        timestamp = time.strftime("%x-%X")
        msg = {'message':[self.event_type,
                          self.event_name,
                          timestamp,
                          self.user_name,
                          self.application_name] + contents}
        self.socket.send(msg)

    @staticmethod
    def GetSocket(user_name, application_name):
        # We need to avoid creating a unique socket for each event source
        # created.  This would be alot of sockets, and alot of discovery
        # sources broadcasting.  It is just not necessary.
        # Instead, we create a socket for each unique user_name/app_name.
        # Below, we search our list of previously created sockets for
        # a socket already created for this username/appname.  If one
        # does not exist, create it.
        for socket in EventSource.sockets:
            if socket.user_name == user_name and \
               socket.application_name == application_name:
                return socket

        # Create a new socket for this user/app
        socket = EventSocket(user_name, application_name)
        EventSource.sockets.append(socket)
        return socket

