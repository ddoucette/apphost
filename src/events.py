
"""
    Event classes.
    Implementation of the Event source (server) and sink (client).

    Events are structured as follows:

    "<time> <namespace> <type> [contents which depend on type]"

    where:
      <namespace> is a dot-separated name which is ultimately unique
            to the event.
            The namespace is much (well exactly) like a reverse URL
            (think java).  I.e. pmc.cmd-dispatch.utilization, where 'pmc'
            is the name of the module, cmd-dispatch is the name of the
            sub-function and 'utilization' is the particular
            event within cmd-dispatch.

      <type> Events can come in a variety of types.
         0 - log message.  The [contents] following will be a log level
            and string message.
         1 - value.  The contents following will have a numerical value.
         2 - boolean.  Sub-type of above.  Will contain True/False stringified.

"""
import interface3
import protocol3
import time


class EventSource():

    """
    """
    LOG = 0
    VALUE = 1
    BOOLEAN = 2

    def __init__(self):
        self.protocol = protocol3.Protocol(zmq.PUB, ["EVT"])
        assert(self.protocol is not None)
        # XXX Discovery...

    def send(self, namespace, event_type, contents="", timestamp=""):
        assert(event_type >= LOG and event_type <= BOOLEAN)
        assert(contents != "")

        if timestamp == "":
            timestamp = time.strftime("%x-%X")

        msg = "".join(namespace, str(event_type), contents, " ")
        self.protocol.send(msg)


def test1():

    s = MyServer("srv", 5000)
    c = MyClient("cli", 5000)

    c.do_something()
    time.sleep(1)
    assert(c.done is True)
    print "PASSED"


if __name__ == '__main__':
    test1()
