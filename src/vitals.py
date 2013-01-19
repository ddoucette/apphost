"""
    Vital statistics.
"""

import event
import time


class VitalBase(object):

    def __init__(self, vstat_type, name, description, initial_value=0):

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
        VitalBase.__init__(self, "CRITICAL", name, description)

    def __set__(self, instance, value):
        delta = value - self.value

        if delta == 0:
            # Nothing has changed.  Nothing else to do...
            return

        self.value = value

        # The value has been updated.  This is a critical vital
        # statistic, so an event needs to be issued immediately
        self.event.send(self.value, delta)


def test1():
   
    event.EventSource.Init("sysuser", "test1()")

    class Myclass(object):
        mystat = VitalCritical("mystat", "Some junk statistic")

    mc = Myclass()
    mc.mystat += 1
    mc.mystat += 1
    mc.mystat += 1
    time.sleep(1)


if __name__ == '__main__':
    test1()
