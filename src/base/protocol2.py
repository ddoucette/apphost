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
import log 
from override import *


class Protocol(log.Logger):

    """
    """
    class Stats():
        def __init__(self):
            self.rx_err_bad_header = 0
            self.rx_err_short = 0
            self.rx_err_long = 0

    def __init__(self, name, location, messages, states):
        assert(isinstance(location, types.DictType))
        assert(isinstance(messages, types.ListType))
        assert(isinstance(states, types.ListType))

        log.Logger.__init__(self)
        self.log_level = "D"

        self.stats = Protocol.Stats()
        self.location = location
        self.messages = messages
        self.states = states
        self.name = name
        self.current_state = states[0]

        # Within the 'states' array, there may be a description
        # which applies to 'all states'.  I.e. the state name is '*'.
        # For convenience, we reference this array entry directly.
        self.all_states = None
        for state in states:
            if state['name'] == "*":
                self.all_states = state
                break

        self.interface = interface.Interface(self.__rx_msg, self.do_intf_action)

        self.log_info("Created protocol (" + name + ") "
                     + str(len(messages)) + " messages, "
                     + str(len(states)) + " states.")

    def get_state(self):
        return self.current_state['name']

    def __set_state(self, state_name):

        # if the state_name == '-', this is a signal to just
        # stay in our current state.
        if state_name == "-":
            return

        # Find the state name within our list of valid states
        for state in self.states:
            if state['name'] == state_name:
                self.log_debug("state: " + self.current_state['name']
                              + " ==> " + state['name'])
                self.current_state = state
                return
        self.bug("Cannot find state (" + state_name + ")")

    def __find_msg(self, msg_hdr):
        for msg in self.messages:
            if msg_hdr in msg:
                return msg
        return None

    def __rx_filter(self, msg):
        # We do some basic protocol checking here.
        # First, we make sure the message just received is actually
        # a message we understand.  (I.e. this message is in our
        # list of valid messages)

        msg_list = msg['message']
        msg_hdr = msg_list[0]
        msg_def = self.__find_msg(msg_hdr)
        if msg_def is None:
            self.log_error("Invalid message header: " + msg_hdr)
            self.stats.rx_err_bad_header += 1
            return None

        fields = msg_def[msg_hdr]

        if len(msg_list) - 1 != len(fields):
            self.log_error("Invalid number of message fields"
                          + " received for message '"+ msg_hdr + "'"
                          + " Expecting " + str(len(fields)) + " but got "
                          + str(len(msg_list) - 1))
            self.stats.rx_err_invalid += 1
            return None

        # Loop through each field and cast it into its type.
        # In the 'messages' array, we describe each field of
        # a received message.  Use these descriptions and types
        # to create a message list which contains the values cast
        # into their correct type.
        field_list = [msg_hdr]
        for i, field in enumerate(fields):
            rcv_field = msg_list[i + 1]
            try:
                field_list.append(field['type'](rcv_field))
            except:
                self.log_error("Invalid field type received for field ("
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
                        self.log_error("Action failed for message ("
                                      + msg_hdr + ")")
                        return
                self.__set_state(next_state)
                return

        # If we get here, our current state description does not
        # include the message received.  However, this message may
        # be accepted in our 'all_states' list of messages.
        if self.all_states is not None:
            state_messages = self.all_states['messages']
            for msg_def in state_messages:
                if msg_def['name'] == msg_hdr:
                    next_state = msg_def['next_state']
                    action = msg_def['action']
                    if action is not None:
                        if action(msg) == False:
                            self.log_error("Action failed for message ("
                                          + msg_hdr + ")")
                            return
                    self.__set_state(next_state)
                    return

        # If we get here, we could not find this message header
        # in any of our lists of messages.
        self.log_error("Invalid message ("
                        + msg_hdr + ") received in state ("
                        + self.get_state() +")")

    def action(self, action_name, arg_list=[]):
        self.interface.do_action(action_name, arg_list)

    def do_intf_action(self, action_name, arg_list=[]):
        state_actions = self.current_state['actions']

        # state_actions is a list of all actions we process in this
        # state.  Find the specified action in this list.
        for action_def in state_actions:
            if action_def['name'] == action_name:
                action = action_def['action']
                next_state = action_def['next_state']
                if action is not None:
                    self.log_debug("action: " + action_name)
                    if action() == False:
                        self.log_error("Action failed for action ("
                                      + action_name + ")")
                        return
                self.__set_state(next_state)
                return

        # If we get here, we could not find the specified action in the
        # list of actions for our current state.
        # However, the action may be described in our 'all_states' entry.
        if self.all_states is not None:
            state_actions = self.all_states['actions']
            for action_def in state_actions:
                if action_def['name'] == action_name:
                    action = action_def['action']
                    next_state = action_def['next_state']
                    if action is not None:
                        self.log_debug("action: " + action_name)
                        if action() == False:
                            self.log_error("Action failed for action ("
                                          + action_name + ")")
                            return
                    self.__set_state(next_state)
                    return

        # If we get here, we could not find this action in our list
        # of actions for this state.  This is not necessarily a SW
        # error, as the state may have changed asynchronously.
        self.log_info("Invalid action (" + action_name
                      + ") received in state (" + self.get_state() + ")")

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
            msg['address'] = self.name
        Protocol.send(self, msg)


def test1():

    messages = [{'HOWDY': []}, \
                {'HI': [{'name':'file name', \
                         'type':types.StringType}, \
                        {'name':'md5sum', \
                         'type':types.StringType}]}, \
                {'RUN': []}, \
                {'RUN_OK': []}, \
                {'STOP': []}, \
                {'STOP_OK': []}, \
                {'FINISHED': [{'name':'error code',
                               'type':types.IntType}]}, \
                {'QUIT': []}]

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
            pass

        def do_stop(self, msg):
            msg_list = ["STOP_OK"]
            msg['message'] = msg_list
            self.proto.send(msg)

        def close(self):
            self.proto.close()

    class MyProtoClient(object):
        def __init__(self, address, port):
            states = [{'name':"START",
                       'actions':[{'name':"begin",
                                   'action':self.do_begin,
                                   'next_state':"WAIT_FOR_HI"}],
                       'messages':[]},
                      {'name':"WAIT_FOR_HI",
                       'actions':[],
                       'messages':[{'name':"HI",
                                    'action':None,
                                    'next_state':"READY"}]},
                      {'name':"READY",
                       'actions':[{'name':"run",
                                   'action':self.do_run,
                                   'next_state':"WAIT_FOR_RUN_OK"},
                                  {'name':"quit",
                                   'action':self.do_quit,
                                   'next_state':"START"}],
                       'messages':[]},
                      {'name':"WAIT_FOR_RUN_OK",
                       'actions':[],
                       'messages':[{'name':"RUN_OK",
                                    'action':None,
                                    'next_state':"RUNNING"}]},
                      {'name':"RUNNING",
                       'actions':[{'name':"stop",
                                   'action':self.do_stop,
                                   'next_state':"WAIT_FOR_STOP_OK"}],
                       'messages':[]},
                      {'name':"*",
                       'actions':[{'name':"all_test",
                                   'action':self.do_all_test,
                                   'next_state':"-"}],
                       'messages':[]},
                      {'name':"WAIT_FOR_STOP_OK",
                       'actions':[],
                       'messages':[{'name':"STOP_OK",
                                    'action':None,
                                    'next_state':"READY"}]}]
            location = {'type':zmq.ROUTER,
                        'protocol':"tcp",
                        'address':address,
                        'port':port}

            self.proto = ProtocolClient("myproto",
                                        location,
                                        messages,
                                        states)
            self.wait_for_that_ok = False
            self.value = 0
            self.all_test_count = 0

        def start(self):
            self.proto.action("begin")

        def run(self):
            self.proto.action("run")

        def stop(self):
            self.proto.action("stop")

        def quit(self):
            self.proto.action("quit")

        def do_begin(self):
            msg = {'message':["HOWDY"]}
            self.proto.send(msg)

        def do_run(self):
            msg = {'message':["RUN"]}
            self.proto.send(msg)

        def do_stop(self):
            msg = {'message':["STOP"]}
            self.proto.send(msg)

        def do_quit(self):
            msg = {'message':["QUIT"]}
            self.proto.send(msg)

        def all_test(self):
            self.proto.action("all_test")

        def do_all_test(self):
            self.all_test_count += 1

        def close(self):
            self.proto.close()

    s = MyProtoServer()
    c = MyProtoClient("127.0.0.1", s.proto.zsocket.port)

    c.start()
    time.sleep(3)
    assert(c.proto.get_state() == "READY")
    assert(s.proto.get_state() == "READY")

    # Verify we can trigger our 'all_test' action in all states,
    # as described in our state array.
    c.all_test()
    time.sleep(1)
    assert(c.all_test_count == 1)

    c.run()
    time.sleep(1)
    assert(c.proto.get_state() == "RUNNING")
    assert(s.proto.get_state() == "RUNNING")

    c.all_test()
    time.sleep(1)
    assert(c.all_test_count == 2)

    # Make sure we cant quit in the running state
    c.quit()
    time.sleep(1)
    assert(c.proto.get_state() == "RUNNING")
    assert(s.proto.get_state() == "RUNNING")

    c.stop()
    time.sleep(1)
    assert(c.proto.get_state() == "READY")
    assert(s.proto.get_state() == "READY")

    c.quit()
    time.sleep(1)
    assert(c.proto.get_state() == "START")
    assert(s.proto.get_state() == "START")

    c.close()
    s.close()
    print "test1() PASSED"


if __name__ == '__main__':
    test1()
