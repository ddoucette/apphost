"""
    Vital statistics.
"""

import system
import event_source
import event_collector
import time
import types
from local_log import *


##
# Vital statistics are basically python descriptor variables which,
# when incremented, result in the sending of a "VITAL" event to the
# listeners.  This is a syntactic way of binding error and other
# statistics to event reporting and system health monitoring.
# Each vital statistic type is broken into 2 parts.  Part 1 is the
# actual variable which is implemented as a python descriptor.  The
# 2nd part is the object which sends the event.
# Each event is based on a standard 'EventSource' event, with the
# event type set to "VITAL".  Each of the various types of vital
# statistics, (ie. ERROR, THRESHOLD, ...), have their own objects
# below (VStatErrorEvent, VStatThresholdEvent, ...) and use the first
# field of the event contents to indicate the vital statistic type.

class VStatEvent(object):

    """
        Base class for all vital statistic events.
        All vital statistic events take the form:

        <event msg> <vital stat type> <vital stat description>
                        <value1> ... <valueN>

        where:
            <vital stat type> == CRITICAL, ERROR, THRESHOLD, ...

        So the entire event message is:

        'VITAL myvitalstatname TIMESTAMP username appname vstattype values'
    """

    def __init__(self, name, vstat_type, description):

        # We use ':' to separate vstat members, so we must
        # ensure the colon is not used in the name or description
        assert(vstat_type.count(":") == 0)
        assert(description.count(":") == 0)

        self.user_name = system.System.GetUserName()
        self.application_name = system.System.GetApplicationName()

        self.event = event_source.EventSource(name,
                                              "VITAL",
                                              self.user_name,
                                              self.application_name)
        self.description = description
        self.name = name
        self.vstat_type = vstat_type

    def send(self, values):
        values_str = ":".join(values)
        msg = ":".join([self.vstat_type,
                        self.description,
                        values_str])
        self.event.send(msg)

    @staticmethod
    def decode(event):
        # Take in the event dictionary and augment the members
        # with the data parsed out of the 'contents' of the event
        # message.
        assert(event['contents'] != "")

        msg = event['contents']
        vital_type, sep, msg = msg.partition(":")
        description, sep, msg = msg.partition(":")

        # The remainder of the msg is the values.  The parsing
        # of the values is specific to the sub-class.
        event['vital_type'] = vital_type
        event['description'] = description
        event['values'] = msg


class VStatErrorEvent(VStatEvent):

    def __init__(self, name, description):
        VStatEvent.__init__(self, name, "ERROR", description)

    def send(self, value, delta):
        values = [str(value), str(delta)]
        VStatEvent.send(self, values)

    @staticmethod
    def decode(event):
        VStatEvent.decode(event)
        try:
            value, delta = event['values'].split(":")
        except:
            Llog.LogError("Could not parse ERROR VStat: values=("
                          + event['values'] + ")")
            return
        event['value'] = int(value)
        event['delta'] = int(delta)


class VStatError(object):

    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.event = VStatErrorEvent(name, description)
        self.value = 0

    def __set__(self, instance, value):
        delta = value - self.value

        if delta == 0:
            # Nothing has changed.  Nothing else to do...
            return

        self.value = value
        # The value has been updated.  Issue the event.
        self.event.send(self.value, delta)

    def __get__(self, instance, owner):
        return self.value


class VStatEventDecoder():

    @staticmethod
    def decode(vital_type, event):
        if vital_type == "THRESHOLD":
            VStatThresholdEvent.decode(event)
            return

        if vital_type == "ERROR":
            VStatErrorEvent.decode(event)
            return

        Llog.LogError("Invalid vital statistic type: " + str(vital_type))
        assert(False)


class VitalEventCollector():

    def __init__(self, vital_types, rx_cback, username="", appname=""):
        assert(isinstance(vital_types, types.ListType))
        assert(isinstance(rx_cback, types.MethodType))

        self.vital_rx_cback = rx_cback
        self.vital_types = vital_types[:]
        self.collector = event_collector.EventCollector(["VITAL"],
                                              self.event_rx_cback,
                                              username,
                                              appname)

    def event_rx_cback(self, event):
        assert(event['type'] == "VITAL")
        vital_type, sep, msg = event['contents'].partition(":")
        if vital_type in self.vital_types:
            VStatEventDecoder.decode(vital_type, event)
            self.vital_rx_cback(event)


def test1():

    class EventWatcher():
        def __init__(self):
            self.events = 0
            self.value = 0
            self.delta = 0
            self.collector = VitalEventCollector(
                                    ["ERROR"],
                                    self.event_cback)

        def event_cback(self, event):
            print "Event: " + str(event)
            assert(event['vital_type'] == "ERROR")
            self.events += 1
            self.value = event['value']
            self.delta = event['delta']

    evtwatch = EventWatcher()
    # We need to wait for a while here.  The service below will not
    # output a discovery frame right away, causing our event watcher
    # to miss the broadcast.  We wait for approximately 1 beacon frame
    # period, then proceed, allowing time to discover the service.
    time.sleep(10)

    class Myclass(object):
        mystat = VStatError("mystat", "Some junk statistic")

    mc = Myclass()
    mc.mystat += 1
    time.sleep(1)
    assert(evtwatch.value == 1)
    assert(evtwatch.delta == 1)

    mc.mystat += 1
    time.sleep(1)
    assert(evtwatch.value == 2)
    assert(evtwatch.delta == 1)
    mc.mystat += 12345
    time.sleep(1)
    assert(evtwatch.value == 12347)
    assert(evtwatch.delta == 12345)
    time.sleep(1)

    assert(evtwatch.events == 3)
    print "PASSED"


if __name__ == '__main__':

    username = "sysuser"
    appname = "test1"
    modulename = "vitals"

    print "initializing sys..."
    system.System.Init(username, appname, modulename)

    test1()
