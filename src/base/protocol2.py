"""
    Protocol class.
    Protocol class provides some simple threading encapsulation
    and protocol message verification to the standard interface class.
"""
import interface
import time
import zmq
import types
import zsocket
from local_log import *
from override import *


class Protocol(object):

    """
    """
    class Stats():
        def __init__(self):
            self.rx_err_bad_header = 0
            self.rx_err_short = 0
            self.rx_err_long = 0

    def __init__(self, name, location, messages, states):
        assert(isinstance(location, types.DictType))
        assert(isinstance(messages, types.DictType))
        assert(isinstance(states, types.DictType))

        self.stats = Protocol.Stats()
        self.location = location
        self.messages = messages
        self.states = states
        self.name = name
        self.__set_state(states[0]['name'])
        self.interface = interface.Interface(self.__rx_msg, None)

        Llog.LogInfo("Created protocol (" + name + ") "
                     + len(messages) + " messages, "
                     + len(states) + " states.")

    def __set_state(self, state_name):
        # Find the state name within our list of valid states
        for state in self.states:
            if state['name'] == state_name:
                self.current_state = state
                return
        Llog.Bug("Cannot find state (" + state_name + ")")

    def __rx_filter(self, msg):
        # We do some basic protocol checking here.
        msg_list = msg['message']
        msg_hdr = msg_list[0]
        if msg_hdr not in self.messages:
            Llog.LogError("Invalid message header: " + msg_hdr)
            self.stats.rx_err_bad_header += 1
            return None

        fields = self.messages[msg_hdr]

        if len(msg_list) - 1 != len(fields):
            Llog.LogError("Invalid number of message fields"
                          + " received for message '"+ msg_hdr + "'"
                          + " Expecting " + str(len(fields)) + " but got "
                          + str(len(msg_list) - 1))
            self.stats.rx_err_invalid += 1
            return None

        # Loop through each field and cast it into its type.
        field_list = [msg_hdr]
        for i, field in enumerate(fields):
            rcv_field = msg_list[i + 1]
            try:
                field_list.append(field['type'](rcv_field))
            except:
                Llog.LogError("Invalid field type received for field ("
                              + field['name'] + ") ("
                              + rcv_field + ")")
                return None

        # We substitute the message list with our formally cast
        # and typed fields
        msg['message'] = field_list
        return msg

    def __rx_msg(self, msg):
        # Check the message against our message list to ensure
        # it meets the message guidelines set out there.
        msg = self.__rx_filter(msg)
        if msg is None:
            return

        # The message has already been filtered/vetted using the
        # rx_filter function above.  Process this message against
        # our current state.
        msg_hdr = msg['message'][0]
        state_messages = self.current_state['messages']
        # state_messages is a list of all messages we process in this
        # state.  Find the received message in this list and perform
        # the action.
        for msg_def in state_messages:
            if msg_def['name'] == msg_hdr:
                next_state = msg_def['next_state']
                action = msg_def['action']
                if action is not None:
                    if action(msg) == False:
                        Llog.LogError("Action failed for message ("
                                      + msg_hdr + ")")
                        return
                self.__set_state(next_state)
                return

        # If we get here, we could not find this message header
        # in our list of messages.
        Llog.LogError("Invalid message (" + msg_hdr + ") received!")

    def action(self, action_name):
        state_actions = self.current_state['actions']

        # state_actions is a list of all actions we process in this
        # state.  Find the specified action in this list.
        for action_def in state_actions:
            if action_def['name'] == action_name:
                action = action_def['action']
                next_state = action_def['next_state']
                if action is not None:
                    if action() == False:
                        Llog.LogError("Action failed for action ("
                                      + action_name + ")")
                        return
                self.__set_state(next_state)
                return

        # If we get here, we could not find this action in our list
        # of actions for this state.  This is a SW error!
        Llog.Bug("Invalid action (" + action_name + ") received!")

    def send(self, msg):
        msg_hdr = msg['message'][0]
        self.interface.push_in_msg(msg)

    def close(self):
        if self.interface is not None:
            self.interface.close()


class ProtocolServer(Protocol):

    def __init__(self, name, location, messages, states):
        Protocol.__init__(self, name, location, messages, states)
        self.zsocket = zsocket.ZSocketServer(location['type'],
                                             location['protocol'],
                                             location['bind_address'],
                                             name,
                                             location['port_range'])
        self.zsocket.bind()
        self.interface.add_socket(self.zsocket)


class ProtocolClient(Protocol):

    def __init__(self, name, location, messages, states):
        Protocol.__init__(self, name, location, messages, states)
        self.zsocket = zsocket.ZSocketClient(location['type'],
                                             location['protocol'],
                                             location['address'],
                                             name,
                                             location['port'])
        self.zsocket.connect()
        self.interface.add_socket(self.zsocket)

    @overrides(Protocol)
    def send(self, msg):
        if self.zsocket.socket_type == zmq.ROUTER:
            # The client is using a ROUTER socket.  The client
            # ROUTER socket must provide an address when sending
            # all messages.  By default, the protocol server address
            # will be set to the protocol name.
            msg['address'] = self.location_descriptor['name']
        Protocol.send(self, msg)


def test1():

    messages = [{'HOWDY', []}, \
                {'HI', [{'name':'file name', \
                         'type':types.StringType}, \
                        {'name':'md5sum', \
                         'type':types.StringType}]}, \
                {'RUN', []}, \
                {'RUN_OK', []}, \
                {'STOP', []}, \
                {'STOP_OK', []}, \
                {'FINISHED', [{'name':'error code',
                               'type':types.IntType}]}, \
                {'QUIT', []}]

    class MyProtoServer(object):

        def __init__(self):

            states = [{'name':"START",
                       'actions':[],
                       'messages':[{'name':"HOWDY",
                                    'action':self.do_howdy,
                                    'next_state':"READY"}]},
                      {'name':"READY",
                       'actions':[],
                       'messages':[{'name':"RUN",
                                    'action':self.do_run,
                                    'next_state':"RUNNING"},
                                   {'name':"QUIT",
                                    'action':self.do_quit,
                                    'next_state':"START"}]},
                      {'name':"RUNNING",
                       'actions':[],
                       'messages':[{'name':"STOP",
                                    'action':self.do_stop,
                                    'next_state':"READY"}]}]
            location = {'type':zmq.ROUTER,
                        'protocol':"tcp",
                        'bind_address':"*",
                        'port_range':[4122,4132]}

            self.proto = ProtocolServer("myproto",
                                        location,
                                        messages,
                                        states)

        def do_howdy(self, msg):
            msg_list = ["HI", "myfile.name", "0x1234abcd"]
            msg['message'] = msg_list
            self.proto.send(msg)

        def do_run(self, msg):
            msg_list = ["RUN_OK"]
            msg['message'] = msg_list
            self.proto.send(msg)

        def do_quit(self, msg):
            self.close()

        def do_stop(self, msg):
            msg_list = ["STOP_OK"]
            msg['message'] = msg_list
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
