"""
"""
import time
import inspect


class Llog():

    """
        Local log class.  This is a simple log API designed to be
        used when proper logging cannot be used, such as in the actual
        implementations of the modules which make up the proper log
        infrastructure.
    """
    ERROR = "E"
    INFO = "I"
    DEBUG = "D"
    DEFAULT = "D"
    log_inst = None

    def __init__(self, verbose=True):
        assert(Llog.log_inst is None)
        self.verbose = verbose
        self.level = Llog.DEFAULT
        Llog.log_inst = self

    def __print_msg(self, level, filename, line, msg):
        assert(level in ["E", "I", "D"])
        levels = {'E':1, 'I':2, 'D':3}

        if levels[level] > levels[self.level]:
            return

        logmsg = time.strftime("%x-%X") + " <" + level + ">"
        if self.verbose is True:
            logmsg = logmsg + "(" + filename + ":" + line + ")"
        logmsg = logmsg + " " + msg
        print logmsg

    def log_msg(self, level, msgtype, string):
        filename = inspect.stack()[2][1]
        before, sep, filename = filename.rpartition('/')
        line = str(inspect.stack()[2][2])
        logmsg = ["LOG", level, msgtype, filename, line, string]
        self.__print_msg(level, filename, line, string)

    @staticmethod
    def LogError(msg=""):
        if Llog.log_inst is None:
            Llog.log_inst = Llog()
        Llog.log_inst.log_msg("E", "A", msg)

    @staticmethod
    def LogInfo(msg=""):
        if Llog.log_inst is None:
            Llog.log_inst = Llog()
        Llog.log_inst.log_msg("I", "A", msg)

    @staticmethod
    def LogDebug(msg=""):
        if Llog.log_inst is None:
            Llog.log_inst = Llog()
        Llog.log_inst.log_msg("D", "A", msg)

    @staticmethod
    def LogDebugOnce(msg=""):
        if Llog.log_inst is None:
            Llog.log_inst = Llog()
        Llog.log_inst.log_msg("D", "O", msg)

    @staticmethod
    def SetLevel(level):
        assert(level in ["E", "I", "D"])
        if Llog.log_inst is None:
            Llog.log_inst = Llog()
        Llog.log_inst.level = level


def test1():
    Llog.LogError("My error!")
    Llog.LogDebug("My error!")
    Llog.LogInfo("My error!")
    print "PASSED"

if __name__ == '__main__':
    test1()
