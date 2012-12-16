"""
"""
import threading
import time
import zmq
import zhelpers
import discovery


class Log(DiscoverNotifier):

    """
        Log class.  A single-instance class for handling all log messages.
    """
    ERROR = "E"
    INFO = "I"
    DEBUG = "D"
    log_inst = None

    def __init__(self):
        assert(Log.log_inst is None)
        self.ctx = zmq.Context.instance()
        self.pipes = zhelpers.zpipe(self.ctx)
        self.publisher = self.ctx.socket(zmq.PUB)
        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

        # Configure our discovery notifier object to notify us
        # when a log collector is discovered.  We will connect
        # up to all log collectors and publish messages to
        # them.
        self.subscriber_list.append("LOG_COLLECTOR")
        Log.log_inst = self

    def __thread_entry(self):
        while True:
            msg = self.pipes[1].recv_multipart(1)
            if msg is None:
                print "Timeout..."
            else:
                print "LogMsg: level: " + msg[0] + " " + msg[1]

    def push_msg(self, msg):
        self.pipes[0].send_multipart(msg)

    def notify_add(self, service):
        # Connect up to the log collector service
        pass

    def notify_remove(self, service):
        # Disconnect from the log collector
        pass

    @staticmethod
    def log_error(msg=""):
        assert(Log.log_inst is not None)
        logmsg = [Log.ERROR, msg]
        Log.log_inst.push_msg(logmsg)

    @staticmethod
    def log_info(msg=""):
        assert(Log.log_inst is not None)
        logmsg = [Log.INFO, msg]
        Log.log_inst.push_msg(logmsg)

    @staticmethod
    def log_debug(msg=""):
        assert(Log.log_inst is not None)
        logmsg = [Log.DEBUG, msg]
        Log.log_inst.push_msg(logmsg)


log = Log()
