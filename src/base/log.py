"""
    Log class.
    Adds a bit of functionality on top of a standard log event.
    The format of all log messages is:

    <event> [contents]
    where [contents] is:
        <level> <file> <line> [repeat count] <message>

    so the whole thing would be:

    LOG <name> <timestamp> <username> <appname> <level> <file> 
                        <line> [repeat count] <message>
"""
import system
import event
import inspect
import time


class Log():

    """
    """
    ERROR = "ERROR"
    INFO = "INFO"
    DEBUG = "DEBUG"
    log_levels = [ERROR, INFO, DEBUG]
    default_level = INFO
    
    def __init__(self, name):
        # A log instance is basically just an event instance
        # with the addition of log levels and file info pushed
        # into the event context.

        self.level = Log.default_level
        self.name = name
        self.event = event.EventLog(name)
        assert(self.event is not None)

    def __log_msg(self, level, logmsg):
        assert(level in Log.log_levels)

        # Check to see if the log level of this log message is high
        # enough to proceed
        if Log.log_levels.index(level) > Log.log_levels.index(self.level):
            return

        stack = inspect.stack()
        filename = stack[2][1]
        # filename contains the entire path to the file.
        # trim the path off.
        before, sep, filename = filename.rpartition('/')

        # and the line number...
        line = str(stack[2][2])

        logmsg = " ".join([level, filename, line, logmsg])
        self.event.send(logmsg)

    def log_error(self, logmsg):
        self.__log_msg(Log.ERROR, logmsg)

    def log_info(self, logmsg):
        self.__log_msg(Log.INFO, logmsg)

    def log_debug(self, logmsg):
        self.__log_msg(Log.DEBUG, logmsg)

    def set_level(self, level):
        assert(level in Log.log_levels)

        self.level = level
        

class LogCollector():

    def __init__(self, user_name="", application_name="", message_cback=None):
        self.message_cback = message_cback
        self.collector = event.EventCollector([event.EventSource.LOG],
                                        self.log_event,
                                        user_name,
                                        application_name)
        assert(self.collector is not None)

    def log_event(self, log_event):
        if log_event['type'] != event.EventSource.LOG:
            print "ERROR: Invalid event type received!: " + log_event['type']
            return

        log_msg = log_event['contents']

        level, sep, log_msg = log_msg.partition(" ")
        if level not in Log.log_levels:
            level = Log.ERROR

        filename, sep, log_msg = log_msg.partition(" ")
        if filename == "":
            filename = "unknown"

        line_number, sep, log_msg = log_msg.partition(" ")
        if line_number == "":
            line_number = 0

        if self.message_cback is not None:
            self.message_cback(log_event['timestamp'],
                               log_event['user_name'],
                               log_event['application_name'],
                               level,
                               filename,
                               line_number,
                               log_msg)
        else:
            print " ".join([log_event['timestamp'],
                            log_event['user_name'],
                            log_event['application_name'],
                            level,
                            ":".join([filename, str(line_number)]),
                            log_msg])


def test1():
    # Just log a bunch of messages, then exit...

    log_source = Log("test1")
    time.sleep(1)

    i = 0
    nr_msgs = 200
    while i < nr_msgs:
        log_source.log_error("something bad happened!")
        log_source.log_info("something interesting happened!")
        log_source.log_debug("something stupid happened!")
        i += 1
        time.sleep(5)


if __name__ == '__main__':

    user_name = "bob"
    app_name = "mytestapp2"
    module_name = "log"

    system.System.Init(user_name, app_name, module_name)
    event.EventSource.Init()
    test1()
