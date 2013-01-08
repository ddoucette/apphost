"""
    Protocol interface class.
    This class provides basic socket connectivity for all ZMQ queue types.
"""
import zmq
from local_log import *


class Protocol():

    """
        The protocol class is threadless, simply providing
        code and methods for the creation of sockets for communication.
    """
    # Supported socket types
    socket_types = [zmq.PUB,
                    zmq.SUB,
                    zmq.ROUTER,
                    zmq.DEALER,
                    zmq.PUSH,
                    zmq.PULL,
                    zmq.REQ,
                    zmq.REP]

    class Stats():
        def __init__(self):
            self.msgs_rx = 0
            self.msgs_tx = 0
            self.msgs_err_rx_short = 0
            self.msgs_err_rx_bad_header = 0

    """
        Constructor
        protocol_headers - Contains a list of strings
                           with which the protocol can validate
                           incoming messages.
                           The specified strings will be removed
                           from the message before delivery to the caller.
    """
    def __init__(self, socket_type, protocol_headers=[]):

        assert(socket_type in Protocol.socket_types)

        self.stats = Protocol.Stats()
        self.ctx = None
        self.socket_type = socket_type
        self.protocol_headers = protocol_headers

        self.socket = None
        self.zmq_ctx = None
        self.service_location = ""
        self.service_protocol = ""
        self.service_address = ""
        self.service_port = 0

    def __del__(self):
        pass
        #self.close()  XXX commented out until I figure out why the
        # close below does not work...

    def get_socket(self):
        return self.socket

    def close(self):
        # XXX The close fails below...???
        if self.socket is not None:
            self.socket.close()
        self.socket = None

        if self.zmq_ctx is not None:
            self.zmq_ctx.term()
        self.zmq_ctx = None

    def __create_socket(self):
        assert(self.zmq_ctx is None)
        assert(self.socket is None)

        self.zmq_ctx = zmq.Context(1)
        assert(self.zmq_ctx is not None)

        self.socket = self.zmq_ctx.socket(self.socket_type)
        assert(self.socket is not None)

    def create_client(self, protocol, address, port):
        assert(protocol in ["tcp"])
        assert(address != "")
        assert(port > 0 and port < 65536)

        self.__create_socket()

        self.service_location = protocol + "://" + address + ":" + str(port)
        self.service_protocol = protocol
        self.service_address = address
        self.service_port = port

        self.socket.connect(self.service_location)

        # If this is a SUB socket, we should subscribe automatically
        # to the protocol headers supplied, if supplied
        if self.socket_type == zmq.SUB:
            for header in self.protocol_headers:
                self.subscribe(header)

    def create_server(self, protocol, address, port_range):
        assert(protocol in ["tcp"])
        assert(address != "")
        assert(len(port_range) > 0 and len(port_range) <= 2)

        for port in port_range:
            assert(port > 0 and port < 65536)

        if len(port_range) == 2:
            assert(port_range[0] < port_range[1])

        self.__create_socket()

        self.service_protocol = protocol
        self.service_address = address

        if len(port_range) == 1:
            # No range specified, simply attempt to bind to the specified
            # port.  If it fails, the caller will get the exception
            self.service_port = port
            self.service_location = (protocol
                                    + "://" + address
                                    + ":" + str(self.service_port))
            self.socket.bind(self.service_location)
        else:
            # Cycle through the range of ports...
            port = port_range[0]
            port_stop = port_range[1]
            bound = False
            while port < port_stop:
                self.service_port = port
                self.service_location = (protocol
                                        + "://" + address
                                        + ":" + str(self.service_port))
                try:
                    self.socket.bind(self.service_location)
                    Llog.LogDebug("bound to port (" + str(port) + ")")
                    bound = True
                    break
                except:
                    # Failed to bind, try the next port
                    Llog.LogDebug("Failed to bind to port " + str(port))
                    port += 1

            assert(bound is True)

    def subscribe(self, subscription):
        assert(self.socket is not None)
        assert(self.socket_type is zmq.SUB)
        Llog.LogDebug("Subscribing to <" + subscription + ">")
        self.socket.setsockopt(zmq.SUBSCRIBE, subscription)

    def recv(self):
        assert(self.socket is not None)
        msg = self.socket.recv()
        if msg is None:
            return None

        self.stats.msgs_rx += 1

        msg = msg.lstrip()
        for header in self.protocol_headers:
            (msghdr, sep, msg) = msg.partition(" ")
            if msghdr != header:
                Llog.LogError("Invalid protocol header received!" +
                               "(" + msghdr + ")")
                self.stats.msgs_err_rx_bad_header += 1
                return None
        return msg

    def send(self, msg):
        assert(self.socket is not None)
        if len(self.protocol_headers) > 0:
            sendmsg = "".join(self.protocol_headers) + " " + msg
        else:
            sendmsg = msg
        Llog.LogDebug("Sending... <" + sendmsg + ">")
        self.socket.send(sendmsg)


def test1():

    # Simple test.  Create a single client and a single server.
    # Send messages between them.
    pub = Protocol(zmq.PUB)
    sub = Protocol(zmq.SUB)
    pub.create_server("tcp", "127.0.0.1", [5678, 5690])
    sub.create_client("tcp", "127.0.0.1", 5678)
    sub.subscribe("app.foo")
    poller = zmq.Poller()
    poller.register(sub.get_socket(), zmq.POLLIN)
    time.sleep(1)

    pub.send("app hello world")
    pub.send("app.foo hello world1")
    msg = sub.recv()
    assert(msg == "app.foo hello world1")

    sub.subscribe("app.bar")
    time.sleep(1)

    pub.send("app hello world")
    pub.send("app.boo.foo.bar hello world1")
    pub.send("app.bar hello world2")
    msg = sub.recv()
    assert(msg == "app.bar hello world2")

    pub.send("app.bar.foo hello world2")
    msg = sub.recv()
    assert(msg == "app.bar.foo hello world2")

    #sub.subscribe("")
    sub.subscribe("app")
    time.sleep(1)

    pub.send("app hello world")
    pub.send("app.foo hello world1")
    pub.send("app.bar hello world2")

    msg = sub.recv()
    assert(msg == "app hello world")
    msg = sub.recv()
    assert(msg == "app.foo hello world1")
    msg = sub.recv()
    assert(msg == "app.bar hello world2")

    print "PASSED"


def test2():

    # Test using PUB/SUB with a subscription
    c = Protocol("SIMPLE_SERVICE", "ZMQ_SUB", ["SIMPLE2"])
    s = Protocol("SIMPLE_SERVICE", "ZMQ_PUB", ["SIMPLE2"])
    s.create_server("tcp://127.0.0.1:5679", discovery=False)
    c.create_client("tcp://127.0.0.1:5679", "SIMPLE2")
    time.sleep(1)
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


def test3():
    # Simple test.  Create a single client and a single server.
    # Send messages between them.
    c = Protocol("SIMPLE_SERVICE", "ZMQ_SUB", ["SIMPLE!", "aabbcc"])
    s = Protocol("SIMPLE_SERVICE", "ZMQ_PUB", ["SIMPLE!", "aabbcc"])
    s.create_server("tcp://127.0.0.1:5678", discovery=False)
    c.create_client("tcp://127.0.0.1:5678", "SIMPLE!")

    # It is important to sleep for a period of time, otherwise the
    # server will attempt to send out the message below before
    # the subscription has been registered.
    time.sleep(1)
    s.send_multipart(["hello world", "bye"])
    msg = c.recv_multipart()
    assert(len(msg) == 2)
    assert(msg[0] == "hello world")
    assert(msg[1] == "bye")

    s.send_multipart([])
    msg = c.recv_multipart()
    assert(len(msg) == 0)

    print "PASSED"


def test4():
    # UDP client/server test
    a = Protocol("SIMPLE_SERVICE", "UDP_BROADCAST", ["SIMPLE!"])
    b = Protocol("SIMPLE_SERVICE", "UDP_BROADCAST", ["SIMPLE!"])
    a.create_client("udp://255.255.255.255:3342")
    b.create_client("udp://255.255.255.255:3342")

    # It is important to sleep for a period of time, otherwise the
    # server will attempt to send out the message below before
    # the subscription has been registered.
    time.sleep(1)

    a.send("hello world")
    time.sleep(1)
    msg = b.recv()
    assert(msg == "hello world")

    print "PASSED"


def test5():
    # UDP client/server test
    a = Protocol("SIMPLE_SERVICE", "UDP_BROADCAST", ["SIMPLE!"])
    b = Protocol("SIMPLE_SERVICE", "UDP_BROADCAST", ["SIMPLE"])
    a.create_client("udp://255.255.255.255:3342", "SIMPLE!")
    b.create_client("udp://255.255.255.255:3342", "SIMPLE!")

    # It is important to sleep for a period of time, otherwise the
    # server will attempt to send out the message below before
    # the subscription has been registered.
    time.sleep(1)

    a.send("hello world")
    time.sleep(1)
    msg = b.recv()
    assert(msg == "hello world")

    print "PASSED"


if __name__ == '__main__':
    test1()
    #test2()
    #test3()
    #test4()
