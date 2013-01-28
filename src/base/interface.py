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


class Interface(object):

    """
    """
    def __init__(self, protocol_rx_cback=None):

        self.ctx = zmq.Context.instance()
        self.in_pipe = zhelpers.zpipe(self.ctx)
        self.poller = zmq.Poller()
        self.poller.register(self.in_pipe[1], zmq.POLLIN)
        self.alive = True
        self.sockets = []
        self.protocol_rx_cback = protocol_rx_cback

        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

    def add_socket(self, zsocket):
        assert(zsocket is not None)
        assert(zsocket not in self.sockets)
        assert(zsocket.socket is not None)

        self.sockets.append(zsocket)
        self.poller.register(zsocket.socket, zmq.POLLIN)

    def find_socket_by_location(self, location):
        for socket in self.sockets:
            if socket.location == location:
                return socket
        return None

    def remove_socket(self, zsocket):
        assert(zsocket in self.sockets)

        for index, socket in enumerate(self.sockets):
            if socket == zsocket:
                self.poller.unregister(zsocket.socket)
                self.sockets.pop(index)
                return

        Llog.LogError("Cannot find socket: <"
                      + str(zsocket) + "> in registered socket list!")
        assert(False)

    # Send a message to the interface thread from the API layer
    def push_in_msg(self, msg):
        if isinstance(msg, types.ListType) is True:
            cmd = ["MSG", ''] + msg
        else:
            cmd = ["MSG", msg]

        self.__push_in_msg_raw(cmd)

    def close(self):

        if self.in_pipe is None:
            return

        # Send the KILL command to the interface thread.
        msg = ["KILL"]
        self.__push_in_msg_raw(msg)

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

    def __thread_entry(self):
        # This is the one and only interface processing thread.
        # This thread pulls all messages from the interface command
        # queue and processes them.  It also pulls messages from
        # the protocol message queue and processes them.
        while self.alive is True:
            try:
                items = dict(self.poller.poll(1000))
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

            if self.in_pipe[1] in items:
                self.__process_command_pipe()
            else:
                # Check to see if any of our sockets are now readable
                for zsocket in self.sockets:
                    if zsocket.socket in items:
                        self.__process_socket(zsocket)

    def __process_socket(self, socket):
        msg = socket.recv()
        if msg is None:
            return
        assert(self.protocol_rx_cback)
        self.protocol_rx_cback(msg)

    def __process_command_pipe(self):

        msg = self.in_pipe[1].recv_multipart()
        assert(msg is not None)
        assert(len(msg) >= 2)

        # In the interface layer, there are only a few valid
        # message types:
        #  MSG - protocol message
        #  KILL - kill message
        #
        #  The format of all multi-part commands are:
        #   ['CMDSTR', '', 'msgpart', 'msgpart', ...]
        #  The format of all string commands are:
        #   ['CMDSTR', 'msg-string']

        if msg[0] == "KILL":
            # We are finished.  Just get out of the thread.
            self.alive = False
            return
        elif msg[0] == "MSG":

            # Check to verify we have 1, and only 1 socket.
            # We support multiple sockets, so it gets a bit
            # difficult to decide which socket to send the message
            # to.
            assert(len(self.sockets) == 1)

            if len(msg) == 2:
                # String-based message
                msgtxt = str(msg[1])
                print "Sending: " + msgtxt
                msg = msgtxt
            else:
                msg = msg[2:]
                print "Sending: " + str(msg)

            self.sockets[0].send(msg)

        else:
            Llog.LogError("Invalid message header! (" + msg[0] + ")")
            assert(False)

    def __push_in_msg_raw(self, msg):
        assert(isinstance(msg, types.ListType))
        self.in_pipe[0].send_multipart(msg)


def test1():

    class MyServer():
        def __init__(self, name, port):
            self.name = name
            server = zsocket.ZSocketServer(zmq.ROUTER,
                                           "tcp",
                                           "*",
                                           [port],
                                           ["MYPROTO"])
            server.bind()
            self.interface = Interface(self.handle_proto_req)
            self.interface.add_socket(server)

        def handle_proto_req(self, msg):
            address, msgtxt = msg[0:2]
            print "Received from " + str(address) + " : " + msgtxt
            msgtxt += " ok"
            self.interface.push_in_msg([address, msgtxt])

    class MyClient():
        def __init__(self, name, port):
            self.name = name
            client = zsocket.ZSocketClient(zmq.REQ,
                                           "tcp",
                                           "127.0.0.1",
                                           port,
                                           ["MYPROTO"])
            client.connect()
            self.interface = Interface(self.handle_proto_msg)
            self.interface.add_socket(client)
            self.done = False

        def handle_proto_msg(self, msg):
            print "Response: " + msg

            (name, sep, msg) = msg.partition(" ")
            assert(name == self.name)
            (msg, sep, ok) = msg.rpartition(" ")
            assert(ok == "ok")
            self.done = True

        def do_something(self):
            self.interface.push_in_msg(self.name + " - do - something")

    s = MyServer("srv", 5000)
    c = MyClient("cli", 5000)

    c.do_something()
    time.sleep(5)
    assert(c.done is True)
    print "PASSED"


if __name__ == '__main__':
    test1()
