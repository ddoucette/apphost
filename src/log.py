"""
"""
import threading
import time
import zmq
import zhelpers
import inspect
from discovery import Discover, DiscoverNotifier
from interface import *


class Log(DiscoverNotifier, Interface):

    """
        Log class.  A single-instance class for handling all log messages.
    """
    ERROR = "E"
    INFO = "I"
    DEBUG = "D"
    log_inst = None

    def __init__(self, throttle=True, throttle_period=3):
        assert(Log.log_inst is None)
        Interface.__init__(self)
        self.publisher = self.ctx.socket(zmq.PUB)
        self.connected = False
        self.log_entries = []
        self.throttle = throttle
        self.throttle_period = throttle_period

        # Create a timer to process our throttle log entries
        if throttle is True:
            self.create_timer("ThrottleTimer", throttle_period)

        # Configure our discovery notifier object to notify us
        # when a log collector is discovered.  We will connect
        # up to all log collectors and publish messages to
        # them.
        self.discovery = Discover()
        self.discovery.register_notifier("LOG_COLLECTOR", self)

        Log.log_inst = self

        # Start up the interface
        self.start()

    def __find_log_entry(self, filename, line):
        for log_entry in self.log_entries:
            if (log_entry.filename == filename and
                log_entry.line == line):
                return log_entry
        return None

    """
        __process_msg()
        Override the Interface process_msg method.  This is where all
        the in-thread work is done.
    """
    @overrides(Interface)
    def process_msg(self, msg):
        assert(len(msg) >= 2)
        if msg[0] == "LOG":
            # This is a log message.  Push it out our subscriber
            # channel.
            assert(len(msg) == 6)

            level = msg[1]
            msgtype = msg[2]
            filename = msg[3]
            line = msg[4]
            throttle_message = False

            if self.throttle == True:
                # Message throttling.
                # If message throttling is enabled, we must check to see
                # if this log message has been issued within a threshold
                # number of times.  If it has, we simply increment the
                # log entry without forwarding the message.
                log_entry = self.__find_log_entry(filename, line)
                if log_entry is not None:
                    log_entry.nr_messages = log_entry.nr_messages + 1
                    # Update the entry with the most recent log message contents
                    log_entry.msg = msg[5]
                    throttle_message = True
                else:
                    log_entry = LogEntry(level, msgtype, filename, line, msg[5])
                    self.log_entries.append(log_entry)

            if throttle_message is False:
                self.__print_msg(level, filename, line, msg[5])

        elif msg[0] == "CONNECT":
            assert(len(msg) == 3)
            assert(msg[1] == "LOG_COLLECTOR")
            print "Connecting to " + msg[1] + " at " + msg[2]
        elif msg[0] == "DISCONNECT":
            assert(len(msg) == 3)
            assert(msg[1] == "LOG_COLLECTOR")
            print "Disconnecting from " + msg[1] + " at " + msg[2]
        else:
            assert(False)

    def __print_msg(self, level, filename, line, msg):
        if self.connected is True:
            msg.pop(0)
            self.publisher.send_multipart(msg)
        else:
            print level + ": " + filename + ":" + line + " " + msg

    @overrides(Interface)
    def process_timer(self, timer):
        assert(timer == "ThrottleTimer")
        now = time.time()

        # Go through each log entry and remove all the old ones.
        for log_entry in self.log_entries[:]:
            if log_entry.timestamp < now - self.throttle_period:
                # The entry is older than our throttle period.
                # Output the log message along with a message
                # indicating how many messages were throttled
                # Log entries are created for every log message.
                # We only need to output a message when there
                # are more than 1 entry present.
                if log_entry.nr_messages > 0:
                    self.__print_msg(
                                    log_entry.level,
                                    log_entry.filename,
                                    log_entry.line,
                                    log_entry.msg)
                    throttle_msg = " *** Last message repeated " \
                                    + str(log_entry.nr_messages) + " times ***"

                    self.__print_msg(
                                    log_entry.level,
                                    log_entry.filename,
                                    log_entry.line,
                                    throttle_msg)

                self.log_entries.remove(log_entry)
            else:
                # Log messages are added in chronological order.
                # If this entry is still not old enough, we can
                # just break out here
                break

    def notify_add(self, service):
        # Connect up to the log collector service
        # Send a message to the log instance thread to do the
        # connecting for us.
        print "NOTIFY ADD!"
        msg = ["CONNECT", service['name'], service['location']]
        self.push_in_msg(msg)

    def notify_remove(self, service):
        msg = ["DISCONNECT", service['name'], service['location']]
        self.push_in_msg(msg)

    def log_msg(self, level, msgtype, string):
        filename = inspect.stack()[2][1]
        line = str(inspect.stack()[2][2])
        logmsg = ["LOG", level, msgtype, filename, line, string]
        Log.log_inst.push_in_msg(logmsg)

    @staticmethod
    def Close():
        Log.log_inst.close()
        del Log.log_inst
        Log.log_inst = None

    @staticmethod
    def LogError(msg=""):
        assert(Log.log_inst is not None)
        Log.log_inst.log_msg("E", "A", msg)

    @staticmethod
    def LogInfo(msg=""):
        assert(Log.log_inst is not None)
        Log.log_inst.log_msg("I", "A", msg)

    @staticmethod
    def LogDebug(msg=""):
        assert(Log.log_inst is not None)
        Log.log_inst.log_msg("D", "A", msg)

    @staticmethod
    def LogDebugOnce(msg=""):
        assert(Log.log_inst is not None)
        Log.log_inst.log_msg("D", "O", msg)


class LogEntry():
    def __init__(self, level, msgtype, filename, line, msg):
        self.level = level
        self.msgtype = msgtype
        self.filename = filename
        self.line = line
        self.msg = msg
        self.nr_messages = 0
        self.timestamp = time.time()


def test1():
    log = Log()
    Log.LogInfo("test1()")
    Log.LogDebug("test1()")
    Log.LogError("test1()")

    # Make sure we handle empty log messages...
    Log.LogError("")

    Log.Close()
    log = None
    print "PASSED"


def test2():
    log = Log()
    num_messages = 1000
    while num_messages > 0:
        message = "message:" + str(num_messages)
        Log.LogInfo(message)
        num_messages = num_messages - 1

    time.sleep(10)

    Log.Close()
    log = None
    print "PASSED"


def test3():

    # No log throttling...
    log = Log(throttle=False)
    num_messages = 1000
    while num_messages > 0:
        message = "message:" + str(num_messages)
        Log.LogInfo(message)
        num_messages = num_messages - 1

    time.sleep(5)

    Log.Close()
    log = None
    print "PASSED"


def test4():

    # Output alot of log messages, interspersed with sleeps.
    log = Log(throttle_period=1)
    num_messages = 1000
    sleep_every_n_messages = 50

    while num_messages > 0:
        message = "message:" + str(num_messages)
        Log.LogInfo(message)
        num_messages = num_messages - 1
        sleep_every_n_messages = sleep_every_n_messages - 1
        if sleep_every_n_messages == 0:
            time.sleep(1)
            sleep_every_n_messages = 50

    time.sleep(5)

    Log.Close()
    log = None
    print "PASSED"


if __name__ == '__main__':
    #test1()
    #test2()
    #test3()
    test4()
else:
    log = Log()
    log = None
