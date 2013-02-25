"""
    Interface class.
    Provides basic threading functionality to enable processing
    of received messages from 1 or more sockets, and processing
    of commands from the protocol API layer.
"""
import threading
import time
import zmq
import zhelpers
import types
import zsocket
from local_log import *


class Interface(object):

    """
    """
    def __init__(self, rx_cback=None, action_cback=None, timer_cback=None):

        self.ctx = zmq.Context.instance()
        self.in_pipe = zsocket.zpipe()
        self.poller = zmq.Poller()
        self.poller.register(self.in_pipe[1].socket, zmq.POLLIN)
        self.alive = True
        self.closed = False
        self.sockets = []
        self.rx_cback = rx_cback
        self.action_cback = action_cback
        self.timer_cback = timer_cback
        self.sys_timer = None
        self.timers = []

        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

    def __del__(self):
        # Ensure the caller closed this interface
        if self.closed is not True:
            Llog.LogError("Interface not closed properly!")
            assert(False)

    def add_timer(self, name, duration):
        # Ensure this timer name is unique in our list, then
        # add it.
        assert(self.timer_cback is not None)
        for timer in self.timers:
            if timer['name'] == name:
                Llog.Bug("Timer: " + name + " already exists!")
        t = threading.Timer(duration, self.do_sys_timeout, {name})
        self.timers.append({'name':name, 'duration':duration, 'timer':t})
        t.start()

    def do_sys_timeout(self, timer_args):
        # We need to process the timer in our interface thread.
        # Post a 'TIMER' message to our queue to move processing
        # to the interface thread.
        self.__push_in_msg_raw({'message':["INTF_TIMER", timer_args[0]]})
 
    def __process_timers(self, timer_name):
        for i, timer in enumerate(self.timers):
            if timer['name'] == timer_name:
                timer['timer'] = None
                self.timers.remove(i)
                self.timer_cback(timer_name)
                return
        Llog.LogInfo("Unknown timer (" + timer_name + ") received!")
                
    def add_socket(self, zskt):
        assert(zskt is not None)
        assert(zskt not in self.sockets)
        assert(zskt.socket is not None)

        self.sockets.append(zskt)
        self.poller.register(zskt.socket, zmq.POLLIN)
        # Unblock the interface thread with a PASS message.
        # Our processing thread is most likely blocked on
        # the old list of sockets.  Now that we have a new one,
        # unblock the thread so it will begin processing this one.
        self.__push_in_msg_raw({'message':["INTF_PASS"]})

    def find_socket_by_location(self, location):
        for socket in self.sockets:
            if socket.location == location:
                return socket
        return None

    def remove_socket(self, zskt):
        assert(zskt in self.sockets)

        for index, socket in enumerate(self.sockets):
            if socket == zskt:
                self.poller.unregister(zskt.socket)
                self.sockets.pop(index)
                # Unblock the interface thread with a PASS message.
                self.__push_in_msg_raw({'message':["INTF_PASS"]})
                socket.close()
                return

        Llog.LogError("Cannot find socket: <"
                      + str(zskt) + "> in registered socket list!")
        assert(False)

    # Send a message to the interface thread from the API layer
    def push_in_msg(self, msg):
        assert(isinstance(msg, types.DictType) is True)
        assert('message' in msg)
        self.__push_in_msg_raw(msg)

    # Send a message to the API layer from the interface
    def push_out_msg(self, msg):
        assert(isinstance(msg, types.DictType) is True)
        assert('message' in msg)
        self.in_pipe[1].send(msg)

    def close(self):
        self.closed = True
        for zsocket in self.sockets[:]:
            self.remove_socket(zsocket)

        # Send the KILL command to the interface thread.
        self.__push_in_msg_raw({'message':["INTF_KILL"]})

        # We have sent the KILL message, now wait for the thread
        # to complete
        iterations = 10
        while self.alive is True and iterations > 0:
            try:
                time.sleep(1)
            except:
                print "ERROR: Interface has not cleaned up!  Exiting anyway!"
                break
            iterations -= 1

        if self.in_pipe is not None:
            self.in_pipe[0].close()
            self.in_pipe[1].close()
        self.ctx.destroy()

    def do_action(self, action_name, action_args=[]):
        msg_list = ["INTF_ACTION", action_name]
        if len(action_args) > 0:
            msg_list += action_args
        self.__push_in_msg_raw({'message':msg_list})

    def __thread_entry(self):
        # This is the one and only interface processing thread.
        # This thread pulls all messages from the interface command
        # queue and processes them.  It also pulls messages from
        # the protocol message queue and processes them.
        while self.alive is True:
            try:
                items = dict(self.poller.poll())
            except zmq.ZMQError:
                # We will see ZMQErrors from time to time.
                # These are generally the result of removing
                # sockets from the poll list (unregistering)
                # from different thread contexts.
                # The poll implementation will wait for the timeout
                # period, then attempt to operate on all registered
                # sockets.  If we remove one during the timeout
                # sleep, it will cause an error.
                # We just ignore the error here.
                items = {}

            if self.in_pipe[1].socket in items:
                self.__process_inbound_msgs()
            else:
                # Check to see if any of our sockets are now readable
                for zskt in self.sockets:
                    if zskt.socket in items:
                        self.__process_socket(zskt)

    def __process_socket(self, socket):
        msg = socket.recv()
        if msg is None:
            return
        assert('message' in msg)

        if self.rx_cback is not None:
            msg = self.rx_cback(msg)

        if msg is not None:
            # The message has not been filtered, push it up
            # to the protocol
            self.in_pipe[1].send(msg)

    def __process_inbound_msgs(self):
        msg = self.in_pipe[1].recv()
        if msg is None:
            return

        # In the interface layer, there are only a few valid
        # message types:
        #  MSG - protocol message
        #  KILL - kill message
        #  PASS - do nothing.  Simply unblocks the processing thread.
        msg_list = msg['message']
        if msg_list[0] == "INTF_PASS":
            # Null message meant to unblock the thread.
            return
        elif msg_list[0] == "INTF_KILL":
            # We are finished.  Just get out of the thread.
            self.alive = False
            return
        elif msg_list[0] == "INTF_ACTION":
            # Message to execute our action callback.  Basically a
            # mechanism to push processing into the interface thread
            # for safe concurrent processing.
            if self.action_cback is not None:
                action_name = msg_list[1]
                if len(msg_list) > 2:
                    action_args = msg_list[2:]
                else:
                    action_args = []
                self.action_cback(action_name, action_args)
            return
        elif msg_list[0] == "INTF_TIMER":
            # A timer has fired.  Process it...
            self.__process_timers(msg_list[1])
            return
        else:
            # Check to verify we have 1, and only 1 socket.
            # We support multiple sockets, so it gets a bit
            # difficult to decide which socket to send the message
            # to.
            assert(len(self.sockets) == 1)
            self.sockets[0].send(msg)

    def __push_in_msg_raw(self, msg):
        assert(isinstance(msg, types.DictType) is True)
        assert('message' in msg)
        self.in_pipe[0].send(msg)


def test1():

    class MyServer():
        def __init__(self, name, port):
            self.name = name
            server = zsocket.ZSocketServer(zmq.ROUTER,
                                           "tcp",
                                           "*",
                                           "MYPROTO",
                                           [port])
            server.bind()
            self.interface = Interface(rx_cback=self.handle_proto_req)
            self.interface.add_socket(server)

        def handle_proto_req(self, msg):
            address = msg['address']
            msglist = msg['message']
            print "Received from " + str(address) + " : " + str(msglist)
            msglist.append("ok")
            self.interface.push_in_msg(msg)

    class MyClient():
        def __init__(self, name, port):
            self.name = name
            client = zsocket.ZSocketClient(zmq.REQ,
                                           "tcp",
                                           "127.0.0.1",
                                           "MYPROTO",
                                           port)
            client.connect()
            self.interface = Interface(rx_cback=self.handle_proto_msg)
            self.interface.add_socket(client)
            self.done = False

        def handle_proto_msg(self, msg):
            assert(msg['message'][0] == self.name)
            assert(msg['message'][-1] == "ok")
            self.done = True

        def do_something(self):
            self.interface.push_in_msg(
                            {'message':[self.name, " - do - something"]})

    s = MyServer("srv", 5000)
    c = MyClient("cli", 5000)

    c.do_something()
    time.sleep(2)
    assert(c.done is True)

    s.interface.close()
    c.interface.close()
    print "PASSED"


def test2():

    class MyServer():
        def __init__(self, name):
            self.name = name
            self.interface = Interface(action_cback=self.process_action,
                                       timer_cback=self.process_timer)
            self.action_args = None
            self.action_name = ""
            self.timer_name = ""

        def process_action(self, action_name, action_args):
            Llog.LogInfo("Got my action!")
            self.got_action = True
            self.action_name = action_name
            self.action_args = action_args

        def process_timer(self, timer_name):
            Llog.LogInfo("Got my timer! " + timer_name)
            self.got_timer = True
            self.timer_name = timer_name

    s = MyServer("test2-server")
    s.interface.do_action("myaction", ["myarg1"])
    time.sleep(1)
    assert(s.got_action is True)
    assert(s.action_name == "myaction")
    assert(s.action_args[0] == "myarg1")

    s.interface.add_timer("mytimer1", 1)
    time.sleep(3)
    assert(s.got_timer is True)
    assert(s.timer_name == "mytimer1")

    s.interface.close()
    print "PASSED"


if __name__ == '__main__':
    #test1()
    test2()
