"""
    Vital statistics.
"""

import system
import event
import time
import types
from local_log import *


class VitalStatisticErrorEvent(event.EventSource):

    """
        Vital statistic error event.  Event is fired when an error
        statistic is incremented.

        Format is:
        <event msg> <vital stat type> <vital stat description>
                        <value> <delta>

        where:
            <vital stat type> == CRITICAL, ERROR, THRESHOLD, ...
            <value> == Current value of the statistic.
            <delta> == Change in value since last event was issued.
    """

    def __init__(self, name, description):
        event.EventSource.__init__(self, name, "VITAL")
        self.description = description
        self.name = name
        
    def send(self, value, delta):
        msg = self.create_event_msg()
        msg = " ".join([msg,
                        "ERROR",
                        "".join(["'", self.description, "'"]),
                        str(value),
                        str(delta)])
        self.interface.push_in_msg(msg)

    @staticmethod
    def decode(event):
        # Take in the event dictionary and augment the members
        # with the data parsed out of the 'contents' of the event
        # message.

        event['vital_type'] = "ERROR"
        event['description'] = "Invalid event"
        event['value'] = 0
        event['delta'] = 0

        try:
            vital_type, description, values = event['contents'].split("'")
        except:
            Llog.LogError("Cannot parse ERROR statistic contents!")
            return

        vital_type = vital_type.strip()
        description = description.strip()
        values = values.strip()

        try:
            value_str, delta_str = values.split()
        except:
            Llog.LogError("Cannot parse ERROR statistic values!")
            return

        event['vital_type'] = vital_type
        event['description'] = description
        event['value'] = int(value_str)
        event['delta'] = int(delta_str)


class VitalStatisticError(object):

    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.event = VitalStatisticErrorEvent(name, description)
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


class VitalStatisticThresholdEvent(event.EventSource):

    """
        Vital statistic threshold event.  If a statistic crosses a threshold,
        this event will fire.

        Format is:
        <event msg> <vital stat type> <vital stat description>
                        <value> <threshold>

        where:
            <vital stat type> == CRITICAL, ERROR, THRESHOLD, ...
            <value> == Current value of the statistic
            <threshold> == Threshold crossed resulting in this event.
    """
    decode_errors = VitalStatisticError(
                            "decode_errors",
                            "Error decoding a threshold event")

    def __init__(self, name, description):

        event.EventSource.__init__(self, name, "VITAL")

        self.description = description
        self.name = name
        
    def send(self, value, threshold):
        msg = self.create_event_msg()
        msg = " ".join([msg,
                        "THRESHOLD",
                        "".join(["'", self.description, "'"]),
                        str(value),
                        str(threshold)])
        self.interface.push_in_msg(msg)

    @staticmethod
    def decode(event):
        # Take in the event dictionary and augment the members
        # with the data parsed out of the 'contents' of the event
        # message.

        event['vital_type'] = "THRESHOLD"
        event['description'] = "Invalid event"
        event['value'] = 0
        event['threshold'] = 0

        try:
            vital_type, description, values = event['contents'].split()
        except:
            self.decode_errors += 1
            return

        vital_type = vital_type.strip()
        description = description.strip()
        values = values.strip()

        try:
            value_str, threshold_str = values.split()
        except:
            self.decode_errors += 1
            return

        event['vital_type'] = "THRESHOLD"
        event['description'] = description
        event['value'] = int(value_str)
        event['threshold'] = int(threshold_str)


class VitalStatisticEventDecoder():

    @staticmethod
    def decode(vital_type, event):
        if vital_type == "THRESHOLD":
            VitalStatisticThresholdEvent.decode(event)
            return

        if vital_type == "ERROR":
            VitalStatisticErrorEvent.decode(event)
            return

        Llog.LogError("Invalid vital statistic type: " + vital_type)
        assert(False)


class VitalEventCollector():

    def __init__(self, vital_types, rx_cback, username="", appname=""):
        assert(isinstance(vital_types, types.ListType))
        assert(isinstance(rx_cback, types.MethodType))

        self.vital_rx_cback = rx_cback
        self.vital_types = vital_types[:]
        self.collector = event.EventCollector(["VITAL"],
                                              self.event_rx_cback,
                                              username,
                                              appname)

    def event_rx_cback(self, event):
        assert(event['type'] == "VITAL")
        vital_type, description, values \
                                = event['contents'].split("'")
        vital_type = vital_type.strip()
        VitalStatisticEventDecoder.decode(vital_type, event)
        self.vital_rx_cback(event)


class VitalStatisticThreshold(object):

    ABOVE = "ABOVE"
    BELOW = "BELOW"
    threshold_types = [ABOVE, BELOW]
    invalid_input = VitalStatisticError(
                        "invalid_input",
                        "Input value received outside the range specified.")

    def __init__(self, name,
                       description,
                       input_range,
                       threshold_type,
                       threshold):
        assert(isinstance(input_range, types.ListType))
        assert(input_range[0] < input_range[1])
        assert(threshold > input_range[0] and threshold < input_range[1])
        assert(threshold_type in VitalThreshold.threshold_types)

        self.threshold = threshold
        self.threshold_type = threshold_type
        self.input_range = input_range
        self.event = VitalStatisticThresholdEvent(name, description)
        self.value = 0

    def __set__(self, instance, value):

        if value < self.input_range[0] or value > self.input_range[1]:
            # Value is outside the range specified.
            self.invalid_input += 1
            return

        self.value = value

        if value > self.threshold and \
           self.threshold_type == VitalThreshold.ABOVE:
            # The value of the input is above the set threshold,
            # and this is an ABOVE type...
            self.event.send(self.value, self.threshold)
            return

        if value < self.threshold and \
           self.threshold_type == VitalThreshold.BELOW:
            # The value of the input is below the set threshold,
            # and this is a BELOW type...
            self.event.send(self.value, self.threshold)
            return

    def __get__(self, instance, owner):
        return self.value


def test1():

    username = "sysuser"
    appname = "test1"
    modulename = "vitals"

    print "initializing sys..."
    system.System.Init(username, appname, modulename)

    class EventWatcher():
        def __init__(self):
            self.events = 0
            self.value = 0
            self.delta = 0
            self.collector = VitalEventCollector(
                                    ["VITAL"],
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
        mystat = VitalStatisticError("mystat", "Some junk statistic")

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
    test1()
