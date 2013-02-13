"""
    Protocol class.
    Protocol class provides some simple threading encapsulation
    and protocol message verification to the standard interface class.
"""
import interface
import time
import zmq
import zhelpers
import types
import zsocket
from local_log import *


class Protocol(object):

    """
        The format of the protocol_description is as follows:

        {'FOO':<min>,<max>),('BAR':<min>,<max>),...}
        where <min>/<max> are integers stating the minimum and maximum
        number of protocol fields for the given protocol message.
    """
    class Stats(object):
        def __init__(self):
            self.rx_err_bad_header = 0
            self.rx_err_short = 0
            self.rx_err_long = 0

    def __init__(self, proto_desc):
        assert(isinstance(proto_desc, types.DictType))

        self.stats = Stats()
        self.pdesc = proto_desc
        self.interface = interface.Interface(self.__msg_rx_process)

    def __msg_rx_process(self, msg):
        # We do some basic protocol checking, then forward the
        # message to our interface pipe so the application
        # can process this message in it's own context.
        msg_list = msg['message']
        msg_hdr = msg_list[0]
        if msg_hdr not in self.pdesc:
            Llog.LogError("Invalid message header: " + msg_hdr)
            self.stats.rx_err_bad_header += 1

        m_min, m_max = self.pdesc[msg_hdr]

        if len(msg_list) < m_min:
            Llog.LogError("Message too short!")
            self.stats.rx_err_short += 1

        if len(msg_list) > m_max:
            Llog.LogError("Message too long!")
            self.stats.rx_err_long += 1

        self.interface.push_out_msg(msg)


def test1():
    pass

if __name__ == '__main__':
    test1()
