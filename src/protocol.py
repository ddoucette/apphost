"""
    Protocol interface class.
    This class provides basic socket connectivity and discovery facilities.
"""
import zmq
import udplib
import re
from local_log import *


class Protocol():

    """
        The protocol class is an abstract class, containing both interfaces and
        implementation.
        The protocol class itself is threadless, simply providing
        code and methods for the creation of sockets for communication.
    """
    socket_types = ["UDP_BROADCAST",
                    "ZMQ_PUB",
                    "ZMQ_SUB",
                    "ZMQ_ROUTER",
                    "ZMQ_DEALER"]

    """
        Constructor
        protocol_headers - Contains a list of strings
                           with which the protocol can validate
                           incoming messages.
                           The specified strings will be removed
                           from the message before delivery to the caller.
    """
    def __init__(self, service_name, socket_type, protocol_headers=[]):

        assert(socket_type in Protocol.socket_types)

        self.ctx = None
        self.service_name = service_name
        self.socket_type = socket_type
        self.protocol_headers = protocol_headers

        self.socket = None
        self.udp = None
        self.zmq_ctx = None
        self.service_location = ""
        self.service_protocol = ""
        self.service_address = ""
        self.service_port = 0

        # The protocol object contains a discovery object.  At least
        # in the case where the protocol is a server.  This is
        # especially strange, given that the Discovery class is a
        # sub-class of this class...
        self.discovery = None

        # The protocol object maintains pointers to the send/recv
        # methods of the socket we are using.  This saves a socket
        # type lookup on each access.
        self.f_send = None
        self.f_send_multipart = None
        self.f_recv = None
        self.f_recv_multipart = None

    def __del__(self):
        self.close()

    def close(self):
        # Close our discovery service
        if self.discovery is not None:
            self.discovery.close()

        self.discovery = None

    def __check_location(self, service_location):

        # Check to see if the location matches tcp|udp://X.X.X.X:port
        m = re.match(
                    r'(tcp|udp)://([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)',
                    service_location)
        if m is None:
            # Check to see if the location matches tcp:domainname:port
            m = re.match(
                        r'(tcp)://([0-9a-zA-Z\-]+):([0-9]+)',
                        service_location)
            if m is None:
                # Check to see if the location matches tcp:*:port
                m = re.match(
                            r'(tcp)://(\*+):([0-9]+)',
                            service_location)
                if m is None:
                    return False

        self.service_protocol = m.group(1)
        self.service_address = m.group(2)
        self.service_port = int(m.group(3))
        return True

    def __create_socket(self, service_location, subscription):
        assert(self.udp is None)
        assert(self.zmq_ctx is None)
        assert(self.socket is None)

        location_ok = self.__check_location(service_location)
        assert(location_ok)

        self.service_location = service_location

        # Create our socket type and connect, if necessary
        if self.socket_type == "UDP_BROADCAST":
            self.udp = udplib.UDP(self.service_port, self.service_address)
            self.f_send = self.udp.send
            self.f_recv = self.udp.recv_noblock
            return

        if self.socket_type == "ZMQ_PUB":
            self.zmq_ctx = zmq.Context.instance()
            assert(self.zmq_ctx is not None)
            self.socket = self.zmq_ctx.socket(zmq.PUB)
            assert(self.socket is not None)
        elif self.socket_type == "ZMQ_SUB":
            self.zmq_ctx = zmq.Context.instance()
            assert(self.zmq_ctx is not None)
            self.socket = self.zmq_ctx.socket(zmq.SUB)
            assert(self.socket is not None)
            self.socket.setsockopt(zmq.SUBSCRIBE, subscription)
        else:
            assert(False)

        self.f_send = self.socket.send
        self.f_send_multipart = self.socket.send_multipart
        self.f_recv = self.socket.recv
        self.f_recv_multipart = self.socket.recv_multipart

    def create_client(self, service_location, subscription=""):
        self.__create_socket(service_location, subscription)
        if self.socket is not None:
            self.socket.connect(service_location)

    def create_server(self, service_location, subscription="", discovery=True):
        self.__create_socket(service_location, subscription)
        assert(self.socket is not None)
        self.socket.bind(service_location)

        if discovery is True:
            service = {'name': self.service_name, 'location': service_location}
            self.discover_service = Discover()
            assert(self.discover_service is not None)
            self.discover_service.register_service(service)

    def recv(self):
        assert(self.f_recv is not None)
        msg = self.f_recv()
        if msg is None:
            return None

        msg = msg.lstrip()
        for header in self.protocol_headers:
            (msghdr, sep, msg) = msg.partition(" ")
            if msghdr != header:
                Llog.LogError("Invalid protocol header received!" +
                               "(" + msghdr + ")")
                return None
        return msg

    def recv_multipart(self):
        assert(self.f_recv_multipart is not None)
        msg = self.f_recv_multipart()
        if msg is None:
            return None

        if len(msg) < len(self.protocol_headers):
            Llog.LogError("Invalid/short message received!")
            return None

        for msghdr, protohdr in zip(msg, self.protocol_headers):
            if msghdr != protohdr:
                Llog.LogError("Invalid protocol header received!" +
                               "(" + msghdr + ")")
                return None
        return msg

    def send(self, msg):
        assert(self.f_recv is not None)
        sendmsg = " ".join(self.protocol_headers) + " " + msg
        Llog.LogDebug("Sending... " + sendmsg)
        self.f_send(sendmsg)

    def send_multipart(self, msg):
        assert(self.f_recv is not None)
        sndmsg = self.protocol_headers[:]
        sndmsg.append(msg)
        self.f_send_multipart(sndmsg)


def test1():

    # Simple test.  Create a single client and a single server.
    # Send messages between them.
    c = Protocol("SIMPLE_SERVICE", "ZMQ_SUB", ["SIMPLE!"])
    s = Protocol("SIMPLE_SERVICE", "ZMQ_PUB", ["SIMPLE!"])
    s.create_server("tcp://127.0.0.1:5678", discovery=False)
    c.create_client("tcp://127.0.0.1:5678")
    poller = zmq.Poller()
    poller.register(c.socket, zmq.POLLIN)

    s.send("hello world")
    msg = ""
    while True:
        try:
            items = dict(poller.poll())
        except:
            break

        if c.socket in items:
            msg = c.recv()
            break

    assert(msg == "hello world")
    print "PASSED"


def test2():

    # Test using PUB/SUB with a subscription
    c = Protocol("SIMPLE_SERVICE", "ZMQ_SUB", ["SIMPLE2"])
    s = Protocol("SIMPLE_SERVICE", "ZMQ_PUB", ["SIMPLE2"])
    s.create_server("tcp://127.0.0.1:5678", discovery=False)
    c.create_client("tcp://127.0.0.1:5678", "SIMPLE2")
    poller = zmq.Poller()
    poller.register(c.socket, zmq.POLLIN)

    s.send("hello world2")
    msg = ""
    while True:
        try:
            items = dict(poller.poll())
        except:
            break

        if c.socket in items:
            msg = c.recv()
            break

    assert(msg == "hello world2")
    print "PASSED"


if __name__ == '__main__':
    test1()
    test2()
