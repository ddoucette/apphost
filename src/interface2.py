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


class Interface():

    """
    """
    def __init__(self):
        self.ctx = zmq.Context.instance()
        self.in_pipe = zhelpers.zpipe(self.ctx)
        self.out_pipe = zhelpers.zpipe(self.ctx)
        self.poller = zmq.Poller()
        self.poller.register(self.in_pipe[1], zmq.POLLIN)
        self.alive = True
        self.timers = []
        self.protocol = None

        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

    def __del__(self):
        self.close()

    def set_protocol(self, protocol):
        assert(self.protocol is None)
        assert(protocol.socket is not None)
        self.protocol = protocol
        self.poller.register(protocol.socket, zmq.POLLIN)

    def close(self):

        # Close all the timers
        for timer in self.timers:
            timer.close()

        # Send the KILL command to the interface thread.
        msg = ["KILL"]
        self.push_in_msg_raw(msg)

        # We have sent the KILL message, now wait for the thread
        # to complete
        iterations = 20
        while self.alive is True and iterations > 0:
            try:
                time.sleep(0.5)
            except:
                print "ERROR: Interface has not cleaned up!  Exiting anyway!"
                break
            iterations -= 1

    def create_timer(self, timer_name, period):
        assert(period > 0)
        assert(timer_name != "")
        timer = InterfaceTimer(self, timer_name, period)
        self.timers.append(timer)
        return timer

    def __thread_entry(self):
        # This is the one and only interface processing thread.
        # This thread pulls all messages from the interface command
        # queue and processes them.  It also pulls messages from
        # the protocol message queue and processes them.
        while self.alive is True:
            items = dict(self.poller.poll())
            if self.in_pipe[1] in items:
                self.__process_pipes()
            elif self.protocol.socket.fileno() in items:
                self.__process_protocol()
            else:
                assert(False)

    def __process_protocol(self):
        assert(self.protocol is not None)
        msg = self.protocol.recv()
        assert(msg is not None)
        self.process_protocol(msg)

    def __process_pipes(self):
        msg = self.in_pipe[1].recv_multipart()
        assert(msg is not None)
        assert(len(msg) >= 1)

        # In the interface layer, there are only a few valid
        # message types:
        #  MESSAGE - protocol message
        #  KILL - kill message
        #  TIMER - timer message
        if msg[0] == "KILL":
            self.process_kill()

            # We are finished.  Just get out of the thread.
            self.alive = False
            return
        elif msg[0] == "MESSAGE":
            assert(len(msg) >= 2)
            # Strip off our 'MESSAGE' header and forward the message
            # to the interface implementation
            msg.pop(0)
            self.process_msg(msg)
        elif msg[0] == "TIMER":
            assert(len(msg) == 2)
            self.process_timer(msg[1])
        else:
            assert(False)

    """
        process_protocol.
        Method is meant to be overridden by the inheriting class to provide
        protocol processing capabilities.
    """
    def process_protocol(self, msg):
        assert(False)

    """
        process_msg interface method.  This is where the inheriting class
        will do its message processing work inside the thread.
    """
    def process_msg(self, msg):
        assert(False)

    """
        process_timer interface method.  This method must be overwritten
        by the inheriting class, if timers are used.
    """
    def process_timer(self, timer):
        assert(False)

    """
        process_kill interface method.  This method allows the inheriting class
        to process the object's kill event.  Any cleanup which
        must be done should be done here.
    """
    def process_kill(self):
        pass

    def push_in_msg_raw(self, msg):
        assert(isinstance(msg, types.ListType))
        self.in_pipe[0].send_multipart(msg)

    def push_in_msg(self, msg):
        assert(isinstance(msg, types.ListType))
        assert(len(msg) > 0)
        msg.insert(0, "MESSAGE")
        self.push_in_msg_raw(msg)

    def pull_out_msg(self):
        msg = self.out_pipe[1].recv_multipart()
        assert(len(msg) >= 1)
        return msg

    def __push_out_msg(self, msg):
        self.out_pipe[0].send_multipart(msg)


class InterfaceTimer():

    """
        InterfaceTimer object.  Contains the wrapper implementation
        of an interface timer.
        When the timer goes off, it simply sends a timer message to
        the interface.
    """
    def __init__(self, interface, name, period):
        self.name = name
        self.period = period
        self.interface = interface
        self.alive = True
        self.timer = threading.Timer(period, self.__timer_handler)
        self.timer.start()

    def __timer_handler(self):

        # Called by the timer.  Push in the 'TIMER' message
        # into the main interface thread.  Let the main interface
        # thread call the user's timer callback function.
        msg = ["TIMER", self.name]
        self.interface.push_in_msg_raw(msg)
        # Re-schedule the timer...
        if self.alive is True:
            self.timer = threading.Timer(self.period, self.__timer_handler)
            self.timer.start()

    def close(self):
        self.alive = False
        self.timer.cancel()


def test1():
    class MyClass(Interface):
        def __init__(self):
            Interface.__init__(self)
            self.timers_fired = 0
            self.msgs_rx = 0

        def process_msg(self, msg):
            self.msgs_rx += 1

        def process_timer(self, timer_name):
            assert(timer_name == "mytimer")
            self.timers_fired += 1

    mc = MyClass()
    timer = mc.create_timer("mytimer", 5)
    time.sleep(6)
    assert(mc.timers_fired == 1)

    mc.push_in_msg(["hello there"])
    time.sleep(1)
    assert(mc.msgs_rx == 1)
    mc.close()
    print "PASSED"


def test2():
    import protocol

    class MyClass(Interface):
        def __init__(self):
            Interface.__init__(self)
            self.timers_fired = 0
            self.msgs_rx = 0
            self.proto_msgs_rx = 0
            p = protocol.Protocol(
                                "MYSERVICE",
                                "UDP_BROADCAST",
                                ["MYPROTO"])
            p.create_client("udp://255.255.255.255:3342")
            self.set_protocol(p)

        def process_msg(self, msg):
            self.msgs_rx += 1
            self.rx_msg = msg

        def process_protocol(self, msg):
            self.proto_msgs_rx += 1
            self.proto_rx_msg = msg

        def process_timer(self, timer_name):
            assert(timer_name == "mytimer")
            self.timers_fired += 1

    mc = MyClass()
    timer = mc.create_timer("mytimer", 5)

    p = protocol.Protocol(
                        "TESTSERVICE",
                        "UDP_BROADCAST",
                        ["MYPROTO"])
    p.create_client("udp://255.255.255.255:3342")

    time.sleep(6)
    assert(mc.timers_fired == 1)

    mc.push_in_msg(["testing 1 2 3 4"])
    time.sleep(1)
    assert(mc.msgs_rx == 1)
    assert(mc.rx_msg == ["testing 1 2 3 4"])

    p.send("hello there")

    time.sleep(1)
    assert(mc.proto_msgs_rx == 1)
    assert(mc.proto_rx_msg == "hello there")
    p.close()
    p = None

    p = protocol.Protocol(
                        "TESTSERVICE",
                        "UDP_BROADCAST",
                        ["MYPROTO"])
    p.create_client("udp://255.255.255.255:3342")
    p.send("hello there")

    time.sleep(1)
    assert(mc.proto_msgs_rx == 2)
    assert(mc.proto_rx_msg == "hello there")
    p.close()
    p = None

    mc.close()
    print "PASSED"


if __name__ == '__main__':
    #test1()
    test2()
