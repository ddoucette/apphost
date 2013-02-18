"""
    Protocol class.
    Protocol class provides some simple threading encapsulation
    and protocol message verification to the standard interface class.
"""
import interface
import time
import zmq
import zhelpers
import types
import zsocket
from local_log import *
from override import *


class Protocol(object):

    """
        The format of the protocol_description is as follows:

        {'FOO':<min>,<max><func>,'BAR':<min>,<max>,<func>,...}
        where <min>/<max> are integers stating the minimum and maximum
        number of protocol fields for the given protocol message.
        <func> is a pointer to the function to call on receipt of
        the message.
    """
    class Stats():
        def __init__(self):
            self.rx_err_bad_header = 0
            self.rx_err_short = 0
            self.rx_err_long = 0

    def __init__(self, proto_desc):
        assert(isinstance(proto_desc, types.DictType))

        self.stats = Protocol.Stats()
        self.pdesc = proto_desc
        self.interface = interface.Interface(self.__rx_msg, None)

    def __rx_filter(self, msg):
        # We do some basic protocol checking, then forward the
        # message to our interface pipe so the application
        # can process this message in it's own context.
        msg_list = msg['message']
        msg_hdr = msg_list[0]
        if msg_hdr not in self.pdesc:
            Llog.LogError("Invalid message header: " + msg_hdr)
            self.stats.rx_err_bad_header += 1
            return None

        m_min, m_max, cback = self.pdesc[msg_hdr]

        if len(msg_list) < m_min:
            Llog.LogError("Message too short!")
            self.stats.rx_err_short += 1
            return None
        if len(msg_list) > m_max:
            Llog.LogError("Message too long!")
            self.stats.rx_err_long += 1
            return None
        return msg

    def __rx_msg(self, msg):
        # Check the message against our protocol descriptor to ensure
        # it meets the message guidelines set out there.
        msg = self.__rx_filter(msg)
        if msg is None:
            return

        # The message has already been filtered/vetted using the
        # rx_filter function above.  Receive the message and forward
        # it to the appropriate callback.
        msg_hdr = msg['message'][0]
        m_min, m_max, cback = self.pdesc[msg_hdr]
        cback(msg)

    def send(self, msg):
        msg_hdr = msg['message'][0]
        self.interface.push_in_msg(msg)

    def close(self):
        if self.interface is not None:
            self.interface.close()


class ProtocolServer(Protocol):

    def __init__(self, location_desc, proto_desc):
        Protocol.__init__(self, proto_desc)
        self.zsocket = zsocket.ZSocketServer(location_desc['type'],
                                             location_desc['protocol'],
                                             location_desc['bind_address'],
                                             location_desc['name'],
                                             location_desc['port_range'])
        self.zsocket.bind()
        self.interface.add_socket(self.zsocket)


class ProtocolClient(Protocol):

    def __init__(self, location_desc, proto_desc):
        Protocol.__init__(self, proto_desc)
        self.zsocket = zsocket.ZSocketClient(location_desc['type'],
                                             location_desc['protocol'],
                                             location_desc['address'],
                                             location_desc['name'],
                                             location_desc['port'])
        self.zsocket.connect()
        self.interface.add_socket(self.zsocket)
        self.address_override = ""
        if location_desc['type'] == zmq.ROUTER:
            # The client is using a ROUTER socket.  We need to provide
            # an override address, so each message sent will have
            # an address attached.
            self.zsocket.set_identity(location_desc['name'])
            self.address_override = location_desc['name']

    @overrides(Protocol)
    def send(self, msg):
        if self.address_override != "":
            msg['address'] = self.address_override
        Protocol.send(self, msg)


def test1():

    class MyProtoServer(object):
        def __init__(self):
            location_descriptor = {'name':"myproto",
                                   'type':zmq.ROUTER,
                                   'protocol':"tcp",
                                   'bind_address':"*",
                                   'port_range':[4122,4132]}
            protocol_descriptor = {'HELLO':(1,1,self.do_hello),
                                   'BYE':(1,2,self.do_bye),
                                   'THAT':(2,2,self.do_that)}
            self.proto = ProtocolServer(location_descriptor,
                                        protocol_descriptor)
            self.got_hello = False
            self.got_bye = False
            self.got_that = False

        def do_hello(self, msg):
            msg['message'][0] = "OK"
            self.got_hello = True
            self.proto.send(msg)

        def do_bye(self, msg):
            msg['message'][0] = "OK"
            self.got_bye = True
            self.proto.send(msg)

        def do_that(self, msg):
            value = int(msg['message'][1])
            Llog.LogInfo("do_that: " + str(value))
            value += 1
            msg['message'][0] = "OK"
            msg['message'][1] = str(value)
            self.got_that = True
            self.proto.send(msg)

        def close(self):
            self.proto.close()

    class MyProtoClient(object):
        def __init__(self):
            location_descriptor = {'name':"myproto",
                                   'type':zmq.REQ,
                                   'protocol':"tcp",
                                   'address':"127.0.0.1",
                                   'port':4122}
            protocol_descriptor = {'OK':(1,2,self.do_ok)}
            self.proto = ProtocolClient(location_descriptor,
                                        protocol_descriptor)
            self.wait_for_that_ok = False
            self.value = 0

        def hello(self):
            msg = {'message':["HELLO"]}
            self.proto.send(msg)

        def bye(self):
            msg = {'message':["BYE"]}
            self.proto.send(msg)

        def that(self, value):
            msg = {'message':["THAT", str(value)]}
            self.proto.send(msg)
            self.wait_for_that_ok = True

        def do_ok(self, msg):
            msg_list = msg['message']
            Llog.LogDebug("RX: " + msg_list[0] + " len:" + str(len(msg_list)))
            if self.wait_for_that_ok is True:
                value = int(msg_list[1])
                Llog.LogInfo("That value: " + str(value))
                self.value = value
                self.wait_for_that_ok = False

        def close(self):
            self.proto.close()

    Llog.SetLevel("I")

    s = MyProtoServer()
    c = MyProtoClient()

    c.hello()
    time.sleep(1)
    assert(s.got_hello is True)

    value = 1234
    c.that(value)
    time.sleep(1)
    assert(s.got_that is True)
    assert(c.value == value + 1)

    c.bye()
    time.sleep(1)
    assert(s.got_bye is True)

    c.close()
    s.close()
    print "test1() PASSED"

def test2():

    # ROUTER-ROUTER test
    class MyProtoServer(object):
        def __init__(self):
            location_descriptor = {'name':"myproto",
                                   'type':zmq.ROUTER,
                                   'protocol':"tcp",
                                   'bind_address':"*",
                                   'port_range':[4122,4132]}
            protocol_descriptor = {'HELLO':(1,1,self.do_hello),
                                   'BYE':(1,2,self.do_bye),
                                   'THAT':(2,2,self.do_that)}
            self.proto = ProtocolServer(location_descriptor,
                                        protocol_descriptor)
            self.got_hello = False
            self.got_bye = False
            self.got_that = False

        def do_hello(self, msg):
            msg['message'][0] = "OK"
            self.got_hello = True
            self.proto.send(msg)

        def do_bye(self, msg):
            msg['message'][0] = "OK"
            self.got_bye = True
            self.proto.send(msg)

        def do_that(self, msg):
            value = int(msg['message'][1])
            Llog.LogInfo("do_that: " + str(value))
            value += 1
            msg['message'][0] = "OK"
            msg['message'][1] = str(value)
            self.got_that = True
            self.proto.send(msg)

        def close(self):
            self.proto.close()

    class MyProtoClient(object):
        def __init__(self):
            location_descriptor = {'name':"myproto",
                                   'type':zmq.ROUTER,
                                   'protocol':"tcp",
                                   'address':"127.0.0.1",
                                   'port':4122}
            protocol_descriptor = {'OK':(1,2,self.do_ok)}
            self.proto = ProtocolClient(location_descriptor,
                                        protocol_descriptor)
            self.wait_for_that_ok = False
            self.value = 0

        def close(self):
            self.proto.close()

        def hello(self):
            msg = {'message':["HELLO"]}
            self.proto.send(msg)

        def bye(self):
            msg = {'message':["BYE"]}
            self.proto.send(msg)

        def that(self, value):
            msg = {'message':["THAT", str(value)]}
            self.proto.send(msg)
            self.wait_for_that_ok = True

        def do_ok(self, msg):
            msg_list = msg['message']
            Llog.LogDebug("RX: " + msg_list[0] + " len:" + str(len(msg_list)))
            if self.wait_for_that_ok is True:
                value = int(msg_list[1])
                Llog.LogInfo("That value: " + str(value))
                self.value = value
                self.wait_for_that_ok = False

    Llog.SetLevel("D")

    s = MyProtoServer()
    c = MyProtoClient()

    c.hello()
    time.sleep(1)
    assert(s.got_hello is True)

    value = 1234
    c.that(value)
    time.sleep(1)
    assert(s.got_that is True)
    assert(c.value == value + 1)

    c.bye()
    time.sleep(1)
    assert(s.got_bye is True)

    c.close()
    s.close()
    print "test2() PASSED"


if __name__ == '__main__':
    test1()
    test2()
