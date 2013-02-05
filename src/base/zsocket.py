"""
    The ZSocket object is basically a simple wrapper on top of ZMQ.
    What it adds is:
        signature - unique signature per-protocol to weed out errantly
                    received packets, which happens often when we reuse
                    ports across different applications.
    server binding - Automatically attempt re-binding across a 
                    port range for server sockets.
    basic packet framing - All send/recv APIs provide a list of
                    strings for TX/RX.  The ZSocket provides framing,
                    which is not the same as the message framing
                    provided by ZMQ.

    A ZSocket protocol frame looks like this:

    signature:s1:s2:s3:s4:...:sXitem0item1 .. itemX

    Following the signature are the indices for the start of each message field.
    This allows multiple fields without in-band escaping.
    For debugging, optional white-space characters may be inserted to allow the
    messages to be more easily debugged.

    The send function accepts a dictionary as input, specifying the message
    and the optional address.
    The recv function returns a dictionary with the message and address.

"""
import zmq
import types
import zhelpers
import random
import string

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
        signature - An arbitrary string which will be inserted and
                    removed from each sent and received PDU in order
                    to weed out errantly received messages.
    """
    def __init__(self, socket_type, signature=None):

        assert(socket_type in self.socket_types)
        if signature is not None:
            assert(isinstance(signature, types.StringType))
            # Make sure the signature does not have our delimiting
            # character.


        self.stats = ZSocket.Stats()
        self.socket_type = socket_type
        self.signature = signature 

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

    def __parse_message(self, msg):
        # Message format:
        # signature:idx1:idx2:idxN+MSG
        # The + sign separates header from actual message.
        # The indices separate the parts of the message.
        header, sep, msg = msg.partition('+')
        if header == "":
            Llog.LogDebug("Invalid header received!")
            self.stats.rx_err_bad_header += 1
            return None

        Llog.LogDebug("Received header: " + header + " : msg " + msg)
        signature, sep, header = header.partition(':')
        if signature != self.signature:
            Llog.LogDebug("Invalid signature received! (" + signature + ")")
            self.stats.rx_err_bad_header += 1
            return None

        msg_list = []
        last_msg_end = 0
        i = 0
        # Parse through the start field indices and peel out the sub-strings.
        while True:
            length, sep, header = header.partition(':')
            if sep == "":
                break
            field_length = int(length)

            Llog.LogDebug("MSG-" + str(i) + " ==> " + str(length))

            msg_begin = last_msg_end
            msg_end = msg_begin + field_length
            if msg_end > len(msg):
                Llog.LogDebug("Invalid message index! (" + str(i) + ")")
                self.stats.rx_err_bad_header += 1
                return None
            msg_list.append(msg[msg_begin:msg_end])
            last_msg_end = msg_end
            i += 1

        self.stats.rx_ok += 1
        return msg_list

    def __construct_message(self, msg):
        assert(isinstance(msg, types.ListType))
        msg_lengths = []
        for msg_str in msg:
            msg_lengths.append(str(len(msg_str)))

        header = ":".join([self.signature] + msg_lengths)
        header += ":"
        msg_str = "".join(msg)
        return "+".join([header, msg_str])

    def __recv(self):
        assert(self.socket_type != zmq.ROUTER)

        # For non-ROUTER sockets, just receive and process the
        # message
        msg = self.socket.recv()
        if msg is None:
            return None

        msg_list = self.__parse_message(msg)
        if msg_list is None:
            return None
        return {'address': "", 'message':msg_list}

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
        msg_list = self.__parse_message(msg[2])
        return {'address': address, 'message':msg_list}

    def recv(self):
        assert(self.socket is not None)

        if self.socket_type == zmq.ROUTER:
            msg = self.__recv_multipart()
        else:
            msg = self.__recv()
        Llog.LogDebug("Received: " + str(msg))
        return msg

    def __send_multipart(self, address, msg):
        assert(self.socket is not None)
        assert(self.socket_type == zmq.ROUTER)
        Llog.LogDebug("Sending to " + address + " <" + msg + ">")
        self.socket.send_multipart([address, '', msg])
        self.stats.tx_ok += 1

    def __send(self, msg):
        assert(self.socket_type != zmq.ROUTER)
        assert(isinstance(msg, types.StringType))
        Llog.LogDebug("Sending... <" + msg + ">")
        self.socket.send(msg)
        self.stats.tx_ok += 1

    def send(self, msg):
        assert(self.socket is not None)
        assert(isinstance(msg, types.DictType))

        # Message format:
        # msg['address'] == address
        # msg['message'] = list of message pieces
        msg_str = self.__construct_message(msg['message'])

        if self.socket_type == zmq.ROUTER:
            self.__send_multipart(msg['address'], msg_str)
        else:
            self.__send(msg_str)


class ZSocketServer(ZSocket):

    def __init__(self, socket_type,
                       protocol_name,
                       bind_address,
                       port_range=[],
                       signature=""):
        assert(bind_address != "")
        assert(protocol_name in ["tcp", "ipc"])

        if protocol_name == "tcp":
            assert(len(port_range) > 0 and len(port_range) <= 2)

        for port in port_range:
            assert(port > 0 and port < 65536)

        if len(port_range) == 2:
            assert(port_range[0] < port_range[1])

        ZSocket.__init__(self, socket_type, signature)

        self.bind_address = bind_address
        self.port_range = port_range
        self.protocol_name = protocol_name

    def __bind_tcp(self):
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

    def __bind_ipc(self):
        self.location = self.protocol_name \
                            + "://" + self.bind_address
        self.socket.bind(self.location)
        Llog.LogDebug("bound to IPC channel (" + self.location + ")")

    def bind(self):
        self.create_socket()

        if self.protocol_name == "tcp":
            self.__bind_tcp()
        elif self.protocol_name == "ipc":
            self.__bind_ipc()
        else:
            assert(False)


class ZSocketClient(ZSocket):

    def __init__(self, socket_type,
                       protocol_name,
                       address,
                       port=0,
                       signature=""):
        assert(address != "")
        assert(protocol_name in ["tcp", "ipc"])

        if protocol_name == "tcp":
            assert(port > 0 and port < 65536)

        ZSocket.__init__(self, socket_type, signature)

        self.address = address
        self.protocol_name = protocol_name
        self.port = port

    def connect(self):
        self.create_socket()
        self.location = self.protocol_name + "://" + self.address

        if self.protocol_name == "tcp":
            self.location += ":" + str(self.port)
        self.socket.connect(self.location)

        # If this is a subscription socket, we need/should subscribe
        # to our protocol signature.
        if self.socket_type == zmq.SUB:
            self.subscribe(self.signature)


def test1():

    c = ZSocketClient(zmq.REQ, "tcp", "127.0.0.1", 4321, "mysig")
    s = ZSocketServer(zmq.REP, "tcp", "*", [4321, 4323], "mysig")

    assert(c is not None)
    assert(s is not None)

    s.bind()
    c.connect()

    msg_strings = ["hello there...", "this %%-   -2(  ~~  #$*&$#*&$", "!!~~~@@#(#($dkfkdsa"]
    c.send({'message': msg_strings})
    msg = s.recv()['message']
    for i in range(len(msg)):
        assert(msg_strings[i] == msg[i])

    s.send({'message': ["OK"]})
    msg = c.recv()['message']
    assert(msg[0] == "OK")

    msg_strings = []
    for i in range(25):
        msg_strings.append(''.join(random.choice(string.ascii_uppercase
                                                    + string.digits)
                                                    for x in range(12)))
    c.send({'message': msg_strings})
    msg = s.recv()['message']
    for i in range(len(msg)):
        assert(msg_strings[i] == msg[i])

    s.send({'message': ["OK"]})
    msg = c.recv()['message']
    assert(msg[0] == "OK")

    c.close()
    s.close()
    print "test1() - PASSED"


def test2():

    # Multiple requestors with a simple protocol header
    clist = []
    nr_clients = 100

    s = ZSocketServer(zmq.ROUTER, "tcp", "*", [4321, 4323], "MYPROTO")
    assert(s is not None)
    s.bind()

    i = 0
    while i < nr_clients:
        c = ZSocketClient(zmq.REQ, "tcp", "127.0.0.1", 4321, "MYPROTO")
        assert(c is not None)
        clist.append(c)
        c.connect()
        i += 1

    # Have the clients send a message to the server/router
    tx_msg = "hello there..."
    for i in range(len(clist)):
        c = clist[i]
        c.send({'message':[tx_msg, str(i)]})

    # Server/router appends a simple OK-done string, as another message string
    for i in range(len(clist)):
        msg = s.recv()
        msg['message'].append("OK-done")
        s.send(msg)

    # Clients receive the augmented message and verify 
    for i in range(len(clist)):
        c = clist[i]
        msg = c.recv()['message']
        assert(msg[0] == tx_msg)
        assert(msg[1] == str(i))
        assert(msg[2] == "OK-done")
        c.close()

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
    c.send({'message':[tx_msg]})
    msg = s.recv()['message'][0]
    assert(msg == tx_msg)

    c.close()
    s.close()
    s1.close()
    print "test3() - PASSED"


def test4():

    # Verify push/pull sockets work
    spush = ZSocketServer(zmq.PUSH, "tcp", "*", [4567,4569])

    assert(spush is not None)
    spush.bind()

    cpull = ZSocketClient(zmq.PULL, "tcp", "127.0.0.1", 4567)
    assert(cpull is not None)

    cpull.connect()

    tx_msg = "hello there..."
    spush.send({'message':[tx_msg]})

    msg = cpull.recv()['message'][0]
    assert(msg == tx_msg)

    cpull.close()
    spush.close()

    # And now verify the PULL side can be the server...
    spull = ZSocketServer(zmq.PULL, "tcp", "*", [4567,4569])

    assert(spull is not None)
    spull.bind()

    cpush = ZSocketClient(zmq.PUSH, "tcp", "127.0.0.1", 4567)
    assert(cpush is not None)

    cpush.connect()

    tx_msg = "hello there..."
    cpush.send({'message':[tx_msg]})

    msg = spull.recv()['message'][0]
    assert(msg == tx_msg)

    cpush.close()
    spull.close()
    print "test4() - PASSED"


def test5():

    # Verify IPC communications
    spush = ZSocketServer(zmq.PUSH, "ipc", "test5-push.ipc")

    assert(spush is not None)
    spush.bind()

    cpull = ZSocketClient(zmq.PULL, "ipc", "test5-push.ipc")
    assert(cpull is not None)

    cpull.connect()

    tx_msg = "hello there..."
    spush.send({'message':[tx_msg]})

    msg = cpull.recv()['message'][0]
    assert(msg == tx_msg)

    cpull.close()
    spush.close()

    # Now verify the PULL socket can act like a server... 
    spull = ZSocketServer(zmq.PULL, "ipc", "test5-pull.ipc")

    assert(spull is not None)
    spull.bind()

    cpush = ZSocketClient(zmq.PUSH, "ipc", "test5-pull.ipc")
    assert(cpush is not None)

    cpush.connect()

    tx_msg = "hello there..."
    cpush.send({'message':[tx_msg]})

    msg = spull.recv()['message'][0]
    assert(msg == tx_msg)

    cpush.close()
    spull.close()
    print "test5() - PASSED"


def test6():

    # Binary file chunking...
    spush = ZSocketServer(zmq.PUSH, "ipc", "test5-push.ipc")

    assert(spush is not None)
    spush.bind()

    cpull = ZSocketClient(zmq.PULL, "ipc", "test5-push.ipc")
    assert(cpull is not None)
    cpull.connect()

    nr_chunks = 0
    chunksize = 1000
    with open("testfile.bin", "rb") as f:
        while True:
            chunk = f.read(chunksize)
            if chunk:
                spush.send({'message':["CHUNK", str(nr_chunks), chunk]})
                nr_chunks += 1
            else:
                break
        f.close()

    i = 0
    with open("testfile-recv.bin", "w+") as f:
        while True:
            msg = cpull.recv()['message']
            assert(msg[0] == "CHUNK")
            f.write(msg[2])
            i += 1
            if i == nr_chunks:
                break
        f.close()

    cpull.close()
    spush.close()

    # Run md5sum on both files to ensure they are identical
    tx_md5 = zhelpers.md5sum("testfile.bin")
    rx_md5 = zhelpers.md5sum("testfile-recv.bin")
    assert(tx_md5 == rx_md5 and rx_md5 is not None)

    print "test6() - PASSED"


if __name__ == '__main__':
    test1()
    test2()
    test3()
    test4()
    test5()
    test6()
