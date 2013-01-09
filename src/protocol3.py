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

    def recv_multipart(self):
        assert(self.socket is not None)
        assert(self.socket_type == zmq.ROUTER or
               self.socket_type == zmq.DEALER)

        msg = self.socket.recv_multipart()
        if msg is None:
            return None

        if len(msg) < 2:
            Llog.LogInfo("Invalid message received! " + str(msg))
            return None

        # Some multipart messages, such as those received from a DEALER
        # socket, have no empty frame delimiter, i.e. ""
        address = msg[0]
        if len(msg) == 2:
            msg = "".join(msg[1:])
        else:
            msg = "".join(msg[2:])

        self.stats.msgs_rx += 1

        msg = msg.lstrip()
        for header in self.protocol_headers:
            (msghdr, sep, msg) = msg.partition(" ")
            if msghdr != header:
                Llog.LogError("Invalid protocol header received!" +
                               "(" + msghdr + ")")
                self.stats.msgs_err_rx_bad_header += 1
                return None
        return [address, msg]

    def recv(self):
        assert(self.socket is not None)
        assert(self.socket_type != zmq.ROUTER)
        assert(self.socket_type != zmq.DEALER)

        # For non-ROUTER sockets, just receive and process the
        # message
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

    def send_multipart(self, msg):
        assert(self.socket is not None)
        assert(self.socket_type == zmq.ROUTER)

        # The multipart send contains the address for the destination
        # and the message in a 2-entry list
        assert(len(msg) == 2)

        if len(self.protocol_headers) > 0:
            sendmsg = "".join(self.protocol_headers) + " " + msg[1]
        else:
            sendmsg = msg

        Llog.LogDebug("Sending to " + msg[0] + " <" + sendmsg + ">")
        self.socket.send_multipart([msg[0], "", sendmsg])
        self.stats.msgs_tx += 1

    def send(self, msg):
        assert(self.socket is not None)
        assert(self.socket_type != zmq.ROUTER)

        if len(self.protocol_headers) > 0:
            sendmsg = "".join(self.protocol_headers) + " " + msg
        else:
            sendmsg = msg
        Llog.LogDebug("Sending... <" + sendmsg + ">")
        self.socket.send(sendmsg)
        self.stats.msgs_tx += 1


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

    # REQ/REP with bad headers
    req = Protocol(zmq.REQ, ["MYPROTO"])
    req.create_client("tcp", "127.0.0.1", 5678)
    rep = Protocol(zmq.REP, ["MYPROTO"])
    rep.create_server("tcp", "127.0.0.1", [5678, 5690])

    tx_msg = "help me!!!"
    req.send(tx_msg)
    msg = rep.recv()
    assert(msg == tx_msg)
    assert(req.stats.msgs_tx == 1)
    assert(rep.stats.msgs_rx == 1)

    # send the response...

    tx_msg = "ok.. help is on the way!!!"
    rep.send(tx_msg)

    msg = req.recv()
    assert(msg == tx_msg)
    assert(rep.stats.msgs_tx == 1)
    assert(req.stats.msgs_rx == 1)

    # Create a client with a different header.  Verify the requests are
    # dropped...
    bad_req = Protocol(zmq.REQ, ["MYBADPROTO"])
    bad_req.create_client("tcp", "127.0.0.1", 5678)

    tx_msg = "help me!!!"
    bad_req.send(tx_msg)
    msg = rep.recv()
    assert(msg is None)
    assert(rep.stats.msgs_err_rx_bad_header == 1)
    print "PASSED"


def test3():

    # ROUTER
    rtr = Protocol(zmq.ROUTER, ["MYPROTO"])
    rtr.create_server("tcp", "127.0.0.1", [5678, 5690])

    req = Protocol(zmq.REQ, ["MYPROTO"])
    req.create_client("tcp", "127.0.0.1", 5678)

    tx_msg = "help me!!!"
    req.send(tx_msg)
    msg = rtr.recv_multipart()

    req_address = msg[0]
    print "Address: " + req_address
    assert(msg[1] == tx_msg)
    assert(req.stats.msgs_tx == 1)
    assert(rtr.stats.msgs_rx == 1)

    # Send a response back to the requestor...
    tx_msg = "get lost!!!"
    rtr.send_multipart([req_address, tx_msg])
    msg = req.recv()
    assert(msg == tx_msg)
    assert(rtr.stats.msgs_tx == 1)
    assert(req.stats.msgs_rx == 1)

    # Now, many requests...
    req1 = Protocol(zmq.REQ, ["MYPROTO"])
    req1.create_client("tcp", "127.0.0.1", 5678)
    req2 = Protocol(zmq.REQ, ["MYPROTO"])
    req2.create_client("tcp", "127.0.0.1", 5678)
    req3 = Protocol(zmq.REQ, ["MYPROTO"])
    req3.create_client("tcp", "127.0.0.1", 5678)

    req1.send("req - 1")
    req2.send("req - 2")
    req3.send("req - 3")

    i = 0
    while i < 3:
        msg = rtr.recv_multipart()
        req_address = msg[0]
        print "Address: " + req_address
        rtr.send_multipart([req_address, msg[1]])
        i += 1

    msg = req1.recv()
    assert(msg == "req - 1")
    msg = req2.recv()
    assert(msg == "req - 2")
    msg = req3.recv()
    assert(msg == "req - 3")

    print "PASSED"


def test4():

    # ROUTER/DEALER
    rtr = Protocol(zmq.ROUTER, ["MYPROTO"])
    rtr.create_server("tcp", "127.0.0.1", [5678, 5690])

    dlr = Protocol(zmq.DEALER, ["MYPROTO"])
    dlr.create_client("tcp", "127.0.0.1", 5678)

    tx_msg = "help me!!!"
    dlr.send(tx_msg)
    msg = rtr.recv_multipart()
    assert(msg is not None)

    req_address = msg[0]
    print "Address: " + req_address
    assert(msg[1] == tx_msg)
    assert(dlr.stats.msgs_tx == 1)
    assert(rtr.stats.msgs_rx == 1)

    # Many simulaneous requests now...
    dlr1 = Protocol(zmq.DEALER, ["MYPROTO"])
    dlr1.create_client("tcp", "127.0.0.1", 5678)
    dlr2 = Protocol(zmq.DEALER, ["MYPROTO"])
    dlr2.create_client("tcp", "127.0.0.1", 5678)
    dlr3 = Protocol(zmq.DEALER, ["MYPROTO"])
    dlr3.create_client("tcp", "127.0.0.1", 5678)

    dlr1.send("dlr - 1")
    dlr2.send("dlr - 2")
    dlr3.send("dlr - 3")

    i = 0
    while i < 3:
        msg = rtr.recv_multipart()
        req_address = msg[0]
        print "Address: " + req_address
        rtr.send_multipart([req_address, msg[1]])
        i += 1

    msg = dlr2.recv_multipart()
    assert(msg[1] == "dlr - 2")
    msg = dlr3.recv_multipart()
    assert(msg[1] == "dlr - 3")
    msg = dlr1.recv_multipart()
    assert(msg[1] == "dlr - 1")
    print "PASSED"


if __name__ == '__main__':
    test1()
    test2()
    test3()
    test4()
