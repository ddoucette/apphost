"""
    Interface class.
    This class provides basic messaging and threading functionality for
    a protocol block.  This class provides a message layer to isolate
    API calls from the core thread processing.
    This class also provides functionality to ensure no messsages are lost
    during cleanup of the owning object.
    This class is designed to be inherited as an abstract class by the
    final protocol classes.
"""
import threading
import time
import zmq
import zhelpers
import types
import protocol


class Interface():

    """
    """
    def __init__(self):
        self.ctx = zmq.Context.instance()
        self.in_pipe = zhelpers.zpipe(self.ctx)
        self.poller = zmq.Poller()
        self.poller.register(self.in_pipe[1], zmq.POLLIN)
        self.alive = True
        self.protocol = None
        self.protocol_cback = None

        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True

    def __del__(self):
        self.close()

    def set_protocol(self, protocol, protocol_cback):
        assert(self.protocol is None)
        assert(protocol.get_socket() is not None)
        self.protocol = protocol
        self.protocol_cback = protocol_cback
        self.poller.register(protocol.get_socket(), zmq.POLLIN)
        self.thread.start()

    def push_in_msg(self, msg):
        if isinstance(msg, types.ListType):
            intf_msg = ["MESSAGE"] + msg
        else:
            intf_msg = ["MESSAGE", msg]
        self.__push_in_msg_raw(intf_msg)

    def close(self):

        # Send the KILL command to the interface thread.
        msg = ["KILL"]
        self.__push_in_msg_raw(msg)

        # We have sent the KILL message, now wait for the thread
        # to complete
        iterations = 10
        while self.alive is True and iterations > 0:
            try:
                time.sleep(0.5)
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
            items = dict(self.poller.poll())
            if self.in_pipe[1] in items:
                self.__process_pipe()
            elif self.protocol.get_socket() in items:
                self.__process_protocol()
            else:
                assert(False)

    def __process_protocol(self):
        assert(self.protocol is not None)
        msg = self.protocol.recv()
        if msg is None:
            return

        assert(self.protocol_cback is not None)
        self.protocol_cback(msg)

    def __process_pipe(self):
        msg = self.in_pipe[1].recv_multipart()
        assert(msg is not None)
        assert(len(msg) >= 1)

        # In the interface layer, there are only a few valid
        # message types:
        #  MESSAGE - protocol message
        #  KILL - kill message
        if msg[0] == "KILL":
            # We are finished.  Just get out of the thread.
            self.alive = False
            return
        elif msg[0] == "MESSAGE":
            assert(len(msg) >= 2)
            # Strip off our 'MESSAGE' header and forward the message
            # to the interface implementation
            # The message can be either a list of strings forming a
            # multipart message, or it can be just a string to send.
            # We need to inspect the message list further to see.
            if len(msg) > 2:
                # This is a multipart message.  Be sure to keep the list
                # format and forward the message after removing the
                # 'MESSAGE' header...
                msg.pop(0)
            else:
                msg = str(msg[1])

            if self.protocol is not None:
                self.protocol.send(msg)
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
            self.proto = protocol.Protocol(zmq.ROUTER, ["MYPROTO"])
            self.proto.create_server("tcp", "127.0.0.1", [port])
            self.interface = Interface()
            self.interface.set_protocol(self.proto, self.handle_proto_req)

        def handle_proto_req(self, msg):
            print "Received from " + str(msg[0]) + " : " + msg[1]
            self.interface.push_in_msg([msg[0], msg[1] + " ok"])

    class MyClient():
        def __init__(self, name, port):
            self.name = name
            self.proto = protocol.Protocol(zmq.REQ, ["MYPROTO"])
            self.proto.create_client("tcp", "127.0.0.1", port)
            self.interface = Interface()
            self.interface.set_protocol(self.proto, self.handle_proto_msg)
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
    time.sleep(1)
    assert(c.done is True)
    print "PASSED"


if __name__ == '__main__':
    test1()
