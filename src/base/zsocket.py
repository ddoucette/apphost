"""
    Socket wrapper classes:  ZSocketServer,ZSocketClient.
    This module wraps the standard ZMQ sockets with an API which
    abstracts out:
        - bind operations.  Provides functionality to bind a server
        within a range of ports.
        - send/recv_multipart.  Determines the correct format and
        ensures/enforces messages adhere to the correct format.
"""
import zmq
import types
from local_log import *


class ZSocket():

    """
        Base class for ZSocketServer/Client.
    """
    # Supported socket types
    socket_types = [zmq.PUB,
                    zmq.SUB,
                    zmq.ROUTER,
                    zmq.PUSH,
                    zmq.PULL,
                    zmq.REQ,
                    zmq.REP]

    class Stats():
        def __init__(self):
            self.rx_ok = 0
            self.tx_ok = 0
            self.rx_err_short = 0
            self.rx_err_bad_header = 0

    """
        Constructor
        protocol_headers - Contains a list of strings
                           with which the protocol can validate
                           incoming messages.
                           The specified strings will be removed
                           from the message before delivery to the caller.
    """
    def __init__(self, socket_type, protocol_headers=[]):

        assert(socket_type in self.socket_types)
        assert(isinstance(protocol_headers, types.ListType))

        self.stats = ZSocket.Stats()
        self.socket_type = socket_type
        self.protocol_headers = protocol_headers

        self.socket = None
        self.zmq_ctx = zmq.Context(1)
        self.location = ""
        self.port = 0

    def __del__(self):
        self.close()

    def close(self):
        if self.socket is not None:
            self.socket.close()
        self.socket = None

        if self.zmq_ctx is not None:
            self.zmq_ctx.term()
        self.zmq_ctx = None

    def create_socket(self):
        assert(self.socket is None)
        self.socket = self.zmq_ctx.socket(self.socket_type)
        assert(self.socket is not None)

    def subscribe(self, subscription):
        assert(self.socket is not None)
        assert(self.socket_type is zmq.SUB)
        Llog.LogDebug("Subscribing to <" + subscription + ">")
        self.socket.setsockopt(zmq.SUBSCRIBE, subscription)

    def __recv_multipart(self):
        assert(self.socket is not None)
        assert(self.socket_type == zmq.ROUTER)

        msg = self.socket.recv_multipart()
        if msg is None:
            return None

        # Router messages received are always the following
        # format:
        # ['address', '', 'contents']
        if len(msg) != 3:
            Llog.LogInfo("Invalid message received! " + str(msg))
            self.stats.rx_err_short += 1
            return None

        address = msg[0]
        msg_str = msg[2].lstrip()

        for header in self.protocol_headers:
            (msghdr, sep, msg_str) = msg_str.partition(" ")
            if msghdr != header:
                Llog.LogError("Invalid protocol header received!" +
                               "(" + msghdr + ")")
                self.stats.rx_err_bad_header += 1
                return None

        self.stats.rx_ok += 1
        return [address, msg_str]

    def __recv(self):
        assert(self.socket_type != zmq.ROUTER)

        # For non-ROUTER sockets, just receive and process the
        # message
        msg = self.socket.recv()
        if msg is None:
            return None

        msg = msg.lstrip()
        for header in self.protocol_headers:
            (msghdr, sep, msg) = msg.partition(" ")
            if msghdr != header:
                Llog.LogError("Invalid protocol header received!" +
                               "(" + msghdr + ")")
                self.stats.msgs_err_rx_bad_header += 1
                return None

        self.stats.rx_ok += 1
        return msg

    def recv(self):
        assert(self.socket is not None)

        if self.socket_type == zmq.ROUTER:
            msg = self.__recv_multipart()
        else:
            msg = self.__recv()
        Llog.LogDebug("Received: " + str(msg))
        return msg

    def __send_multipart(self, msg):
        assert(self.socket is not None)
        assert(self.socket_type == zmq.ROUTER)

        # The multipart send contains the address for the destination
        # and the message in a 2-entry list
        assert(len(msg) == 2)

        # Insert the protocol headers, if we have any
        if len(self.protocol_headers) > 0:
            sendmsg = "".join(self.protocol_headers) + " " + msg[1]
        else:
            sendmsg = msg[1]

        Llog.LogDebug("Sending to " + msg[0] + " <" + sendmsg + ">")
        self.socket.send_multipart([msg[0], '', sendmsg])
        self.stats.tx_ok += 1

    def __send(self, msg):
        assert(self.socket_type != zmq.ROUTER)
        assert(isinstance(msg, types.StringType))

        if len(self.protocol_headers) > 0:
            sendmsg = "".join(self.protocol_headers) + " " + msg
        else:
            sendmsg = msg

        Llog.LogDebug("Sending... <" + sendmsg + ">")
        self.socket.send(sendmsg)
        self.stats.tx_ok += 1

    def send(self, msg):
        assert(self.socket is not None)

        if self.socket_type == zmq.ROUTER:
            self.__send_multipart(msg)
        else:
            self.__send(msg)


class ZSocketServer(ZSocket):

    def __init__(self, socket_type,
                       protocol_name,
                       bind_address,
                       port_range,
                       protocol_headers=[]):
        assert(bind_address != "")
        assert(len(port_range) > 0 and len(port_range) <= 2)
        assert(protocol_name in ["tcp"])

        for port in port_range:
            assert(port > 0 and port < 65536)

        if len(port_range) == 2:
            assert(port_range[0] < port_range[1])

        ZSocket.__init__(self, socket_type, protocol_headers)

        self.bind_address = bind_address
        self.port_range = port_range
        self.protocol_name = protocol_name

    def bind(self):

        self.create_socket()

        # Loop through each port in our port range and attempt
        # to bind the socket.
        port = self.port_range[0]
        if len(self.port_range) == 2:
            port_stop = self.port_range[1]
        else:
            port_stop = port + 1

        bound = False
        while port < port_stop:
            self.port = port
            self.location = (self.protocol_name
                                    + "://" + self.bind_address
                                    + ":" + str(self.port))
            try:
                self.socket.bind(self.location)
                Llog.LogDebug("bound to port (" + str(port) + ")")
                bound = True
                break
            except:
                # Failed to bind, try the next port
                Llog.LogDebug("Failed to bind to port " + str(port))
                port += 1

        assert(bound is True)


class ZSocketClient(ZSocket):

    def __init__(self, socket_type,
                       protocol_name,
                       address,
                       port,
                       protocol_headers=[]):
        assert(address != "")
        assert(protocol_name in ["tcp"])
        assert(port > 0 and port < 65536)

        ZSocket.__init__(self, socket_type, protocol_headers)

        self.address = address
        self.protocol_name = protocol_name
        self.port = port

    def connect(self):
        self.create_socket()
        self.location = self.protocol_name + "://" \
                        + self.address + ":" \
                        + str(self.port)
        self.socket.connect(self.location)



def test1():

    c = ZSocketClient(zmq.REQ, "tcp", "127.0.0.1", 4321)
    s = ZSocketServer(zmq.REP, "tcp", "*", [4321, 4323])

    assert(c is not None)
    assert(s is not None)

    s.bind()
    c.connect()

    tx_msg = "hello there..."
    c.send(tx_msg)
    msg = s.recv()
    assert(msg == tx_msg)

    c.close()
    s.close()
    print "test1() - PASSED"


def test2():

    # Multiple requestors with a simple protocol header
    clist = []
    nr_clients = 100

    s = ZSocketServer(zmq.ROUTER, "tcp", "*", [4321, 4323], ["MYPROTO"])
    assert(s is not None)
    s.bind()

    i = 0
    while i < nr_clients:
        c = ZSocketClient(zmq.REQ, "tcp", "127.0.0.1", 4321, ["MYPROTO"])
        assert(c is not None)
        clist.append(c)
        c.connect()
        i += 1

    tx_msg = "hello there..."
    i = 0
    while i < nr_clients:
        msg = tx_msg + ":" + str(i)
        c = clist[i]
        c.send(msg)
        i += 1

    i = 0
    while i < nr_clients:
        msg = s.recv()
        address, msg_text = msg[0:2]
        msg_text += " - processed"
        s.send([address, msg_text])
        i += 1

    i = 0
    while i < nr_clients:
        rx_msg = tx_msg + ":" + str(i) + " - processed"
        c = clist[i]
        msg = c.recv()
        assert(msg == rx_msg)
        c.close()
        i += 1

    s.close()
    print "test2() - PASSED"


def test3():

    # Verify the server bind range mechanism works
    s1 = ZSocketServer(zmq.REP, "tcp", "*", [4321])
    s = ZSocketServer(zmq.REP, "tcp", "*", [4321, 4323])

    assert(s1 is not None)
    assert(s is not None)
    s1.bind()
    s.bind()

    # Because server s1 above already bound to port 4321, server 's'
    # should be bound to port 4322.

    c = ZSocketClient(zmq.REQ, "tcp", "127.0.0.1", 4322)
    assert(c is not None)

    c.connect()

    tx_msg = "hello there..."
    c.send(tx_msg)
    msg = s.recv()
    assert(msg == tx_msg)

    c.close()
    s.close()
    s1.close()
    print "test3() - PASSED"


if __name__ == '__main__':
    test1()
    test2()
    test3()
