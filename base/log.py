"""
"""
import time
import inspect


class Logger(object):

    log_levels = {'E':0, 'I':1, 'D':2}

    def __init__(self):
        self.log_level = "I"
        self.verbose = True

    def log_debug(self, msg):
        self.__log_msg("D", msg)

    def log_info(self, msg):
        self.__log_msg("I", msg)

    def log_error(self, msg):
        self.__log_msg("I", msg)

    def bug(self, msg):
        self.__log_msg("E", msg)
        assert(False)

    def __print_msg(self, level, msg, filename, line):
        logmsg = time.strftime("%x-%X") + " <" + level + ">"
        logmsg += " " + self.__class__.__name__
        if self.verbose is True:
            logmsg = logmsg + "(" + filename + ":" + line + ")"
        logmsg = logmsg + " " + msg
        print logmsg

    def __log_msg(self, level, msg):
        assert(level in self.log_levels)
        if self.log_levels[level] > self.log_levels[self.log_level]:
            return

        filename = inspect.stack()[2][1]
        before, sep, filename = filename.rpartition('/')
        line = str(inspect.stack()[2][2])
        self.__print_msg(level, msg, filename, line)
