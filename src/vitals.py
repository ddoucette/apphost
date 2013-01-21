"""
    Vital statistics.
"""

import system
import event
import time
import types


class VitalBase(object):

    CRITICAL = "CRITICAL"
    THRESHOLD = "THRESHOLD"
    ERROR = "ERROR"
    vstat_types = [CRITICAL, THRESHOLD, ERROR]

    def __init__(self, vstat_type, name, description, initial_value=0):
        assert(vstat_type in VitalBase.vstat_types)

        self.value = initial_value
        self.name = name
        self.vstat_type = vstat_type
        self.description = description
        self.event = event.EventVitalStatistic(name, description, vstat_type)

    def __get__(self, instance, owner):
        return self.value

    def __set__(self, instance, value):
        self.value = value


class VitalCritical(VitalBase):

    def __init__(self, name, description):
        VitalBase.__init__(self, VitalBase.CRITICAL, name, description)

    def __set__(self, instance, value):
        delta = value - self.value

        if delta == 0:
            # Nothing has changed.  Nothing else to do...
            return

        self.value = value

        # The value has been updated.  This is a critical vital
        # statistic, so an event needs to be issued immediately
        self.event.send(self.value, delta)


class VitalError(VitalBase):

    def __init__(self, name, description):
        VitalBase.__init__(self, "ERROR", name, description)

    def __set__(self, instance, value):
        delta = value - self.value

        if delta == 0:
            # Nothing has changed.  Nothing else to do...
            return

        self.value = value

        # The value has been updated.  This is a critical vital
        # statistic, so an event needs to be issued immediately
        self.event.send(self.value, delta)


class VitalThreshold(VitalBase):

    ABOVE = "ABOVE"
    BELOW = "BELOW"
    threshold_types = [ABOVE, BELOW]
    description = "Input value received outside the range specified."
    invalid_input = VitalError("invalid_input", description)

    def __init__(self, name,
                       description,
                       input_range,
                       threshold_type,
                       threshold):
        assert(isinstance(input_range, types.ListType))
        assert(input_range[0] < input_range[1])
        assert(threshold > input_range[0] and threshold < input_range[1])
        assert(threshold_type in VitalThreshold.threshold_types)

        VitalBase.__init__(self, VitalBase.THRESHOLD, name, description)

        self.threshold = threshold
        self.threshold_type = threshold_type
        self.input_range = input_range

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


def test1():

    username = "sysuser"
    appname = "test1"
    modulename = "vitals"

    system.System.Init(username, appname, modulename)
    event.EventSource.Init()

    class EventWatcher():
        def __init__(self):
            self.events = 0
            self.collector = event.EventCollector(
                                    [event.EventSource.VITAL],
                                    self.event_cback)

        def event_cback(self, event):
            print "Event: " + str(event)
            self.events += 1

    evtwatch = EventWatcher()
    # We need to wait for a while here.  The service below will not
    # output a discovery frame right away, causing our event watcher
    # to miss the broadcast.  We wait for approximately 1 beacon frame
    # period, then proceed, allowing time to discover the service.
    time.sleep(10)

    class Myclass(object):
        mystat = VitalCritical("mystat", "Some junk statistic")

    mc = Myclass()
    mc.mystat += 1
    mc.mystat += 1
    mc.mystat += 1
    time.sleep(1)

    assert(evtwatch.events == 3)
    print "PASSED"

def test2():

    # Threshold vital statistics.
    # Verify the range operates correctly and the above/below
    # thresholds

    username = "sysuser"
    appname = "test2"
    modulename = "vitals"

    system.System.Init(username, appname, modulename)
    event.EventSource.Init()

    class EventWatcher():
        def __init__(self):
            self.events = 0
            self.collector = event.EventCollector(
                                    [event.EventSource.VITAL],
                                    self.event_cback)
            self.value = 0
            self.threshold = 0

        def event_cback(self, evt):
            print "Event: " + str(evt)
            if evt['type'] == event.EventSource.VITAL:
                self.events += 1
                vstat_type, description, values \
                                = evt['contents'].split("'")
                vstat_type = vstat_type.strip()
                value, threshold = values.strip().split()
                self.value = int(value)
                self.threshold = int(threshold)
                self.vstat_type = vstat_type

    evtwatch = EventWatcher()
    # We need to wait for a while here.  The service below will not
    # output a discovery frame right away, causing our event watcher
    # to miss the broadcast.  We wait for approximately 1 beacon frame
    # period, then proceed, allowing time to discover the service.
    time.sleep(10)

    input_range = [44, 100]
    threshold = 77

    class Myclass(object):
        mystat = VitalThreshold("mystat",
                                "Some junk statistic",
                                input_range,
                                VitalThreshold.ABOVE,
                                threshold)

    mc = Myclass()
    mc.mystat = 48
    mc.mystat = 66
    mc.mystat = 77 # On the edge, but this should not have triggered anything

    time.sleep(1)

    # Should be no events to this point.
    assert(evtwatch.events == 0)

    mc.mystat = 78
    time.sleep(1)
    assert(evtwatch.events == 1)
    assert(evtwatch.threshold == 77)
    assert(evtwatch.value == 78)
    assert(evtwatch.vstat_type == VitalBase.THRESHOLD)

    # Now go beyond our limits
    mc.mystat = 101
    time.sleep(1)
    assert(evtwatch.events == 2)
    assert(evtwatch.threshold == 1)
    assert(evtwatch.value == 1)
    assert(evtwatch.vstat_type == VitalBase.ERROR)

    mc.mystat = -4456788
    time.sleep(1)
    assert(evtwatch.events == 3)
    assert(evtwatch.threshold == 1)
    assert(evtwatch.value == 2)
    assert(evtwatch.vstat_type == VitalBase.ERROR)

    print "PASSED"


if __name__ == '__main__':
    #test1()
    test2()
