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
from override import *


class Interface():

    """
    """
    def __init__(self,
                    protocol_type=None,
                    protocol_location="",
                    protocol_bind=False):

        self.ctx = zmq.Context.instance()
        self.in_pipe = zhelpers.zpipe(self.ctx)
        self.out_pipe = zhelpers.zpipe(self.ctx)
        self.poller = zmq.Poller()
        self.poller.register(self.in_pipe[1], zmq.POLLIN)
        self.alive = True
        self.timers = []

        # Check to see if the caller has specified parameters
        # for the protocol socket.  If so, create and bind/connect
        # the socket and add it to the pollers list
        if protocol_type is not None and protocol_location != "":
            self.protocol_socket = self.ctx.socket(protocol_type)
            if protocol_bind is True:
                self.protocol_socket.bind(protocol_location)
            else:
                self.protocol_socket.connect(protocol_location)
            self.poller.register(self.protocol_socket, zmq.POLLIN)
        else:
            self.protocol_socket = None

        self.thread = threading.Thread(target=self.__intf_thread_entry)
        self.thread.daemon = True

    def __del__(self):
        self.close()

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
            iterations = iterations - 1

    def start(self):
        self.thread.start()

    def create_timer(self, timer_name, period):
        timer = InterfaceTimer(self, timer_name, period)
        self.timers.append(timer)

    def __intf_thread_entry(self):
        while self.alive:
            items = dict(self.poller.poll())

            if self.in_pipe[1] in items:
                self.__process_pipes()

            if (self.protocol_socket is not None and
                     self.protocol_socket in items):
                self.__process_socket()

    def __process_socket(self):
        msg = self.protocol_socket.recv_multipart()
        assert(len(msg) >= 1)
        self.process_protocol(msg)

    def __process_pipes(self):
        msg = self.in_pipe[1].recv_multipart()
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
    def process_protocol(self):
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
        self.in_pipe[0].send_multipart(msg)

    def push_in_msg(self, msg):
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
        self.timer = None
        msg = ["TIMER", self.name]
        self.interface.push_in_msg_raw(msg)
        # Re-schedule the timer...
        if self.alive is True:
            self.timer = threading.Timer(self.period, self.__timer_handler)
            self.timer.start()

    def close(self):
        self.alive = False
        if self.timer is not None:
            self.timer.cancel()
