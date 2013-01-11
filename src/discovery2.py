"""
    Implementation of the DiscoveryServer and DiscoveryClient
    classes.
"""
import udplib
import threading
import time
import uuid
import location
from local_log import *


class DiscoveryServer:

    """
    """
    # Optional argument to specify how often this module will
    # output a discovery beacon packet, in units of seconds.
    def __init__(self, service_name, service_location, period=10):
        assert(service_name != "")
        assert(service_location != "")

        #
        # Each instance of this object creates it's own identity.
        # This means we generate an UUID here and it is used
        # to identify this object (and it's caller) uniquely.
        self.service_name = service_name
        self.service_location = service_location
        self.l_uuid = uuid.uuid4()
        self.udp = udplib.UDP()
        self.period = period

        self.alive = True
        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

    def __thread_entry(self):
        while self.alive:
            self.__send_beacon()
            time.sleep(self.period)

    def __send_beacon(self):
        #
        # The beacon frame looks like this:
        #
        # [ BEACON UUID service1-name service1-location ]
        #
        beacon_list = ["BEACON",
                       str(self.l_uuid),
                       self.service_name,
                       self.service_location]
        beacon = " ".join(beacon_list)
        self.udp.send(beacon)

    def __str__(self):
        return " ".join([str(self.l_uuid),
                         self.service_name,
                         self.service_location])


class DiscoveryClient:

    """
    """
    # Optional argument to specify how long before discovered services
    # are purged from our list of available services.
    def __init__(self, cback=None, ageout=40):
        self.udp = udplib.UDP()
        self.ageout = ageout
        self.alive = True
        self.discovery_cback = cback
        self.discovered_service_list = []
        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

    def __thread_entry(self):
        while self.alive:
            self.__recv_beacons()
            self.__process_discovery_list()

    def __process_discovery_list(self):
        #
        # check each entry in our discovered services list to
        # see if any entries are too old to keep around
        #
        now = time.time()
        for discovered_service in self.discovered_service_list[:]:
            if discovered_service['time'] < now - self.ageout:
                # The entry is too old.  Dump it
                # print "Ageout: " + str(discovered_service)
                self.discovered_service_list.remove(discovered_service)

                # Notify those who are registered
                for name in self.notifiers:
                    if name == discovered_service['name']:
                        notifier_list = self.notifiers[name]
                        for notifier_obj in notifier_list:
                            notifier_obj.notify_remove(discovered_service)

    def __recv_beacons(self):
        while True:
            beacon = self.udp.recv()
            if beacon is None:
                break
            self.__process_beacon(beacon)

    def __process_beacon(self, beacon):
        pieces = beacon.split()

        if len(pieces) != 4:
            Llog.LogError("Invalid beacon received: " + beacon)
            return

        if pieces[0] != "BEACON":
            Llog.LogError("Invalid beacon header received: " + beacon)
            return

        Llog.LogDebug("Beacon: <" + beacon + ">")

        #
        # We have a valid beacon frame.
        # Process the services to see if there are any
        # new services contained that we do not have in
        # our discovered_services_list.
        #
        service_uuid, service_name, service_location = pieces[1:4]

        # Attempt to parse the UUID string to ensure it is well-formed
        try:
            r_uuid = uuid.UUID(service_uuid)
        except ValueError:
            # The UUID was most likely malformed in the beacon.
            # Just discard this beacon.
            Llog.LogError("Malformed UUID string: " + service_uuid)
            return

        # Lets make sure the location meets our expectations
        # for format
        location_check = location.check_location(service_location)
        if location_check is False:
            Llog.LogError("Malformed/invalid location!: " + service_location)
            return

        # Check to see if we already have this entry in our internal
        # list of services
        service = self.find_discovered_service(
                                    service_name,
                                    service_location,
                                    service_uuid)
        now = time.time()
        if service is None:
            # Service does not already exist in our list, add it.
            # Timestamp the discovered service so we can
            # remove them if they expire
            new_service = {'name': service_name,
                           'location': service_location,
                           'uuid': service_uuid,
                           'time': now}
            self.discovered_service_list.append(new_service)

            # Callout to notify the user of this new service
            if self.discovery_cback is not None:
                self.discovery_cback(service_uuid,
                                     service_name,
                                     service_location)
        else:
            # The service exists.  Update the timestamp
            # in the service entry to keep it from
            # expiring
            service['time'] = now

    def does_service_exist(self,
                           service_name,
                           service_location,
                           service_uuid):
        discovered_service = self.find_discovered_service(
                                    service_name,
                                    service_location,
                                    service_uuid)
        if discovered_service is None:
            return False
        return True

    def find_discovered_service(self, name, location, service_uuid):
        for service in self.discovered_service_list:
            if ((name == service['name']) and
                (location == service['location'])):
                if service_uuid != service['uuid']:
                    # We have just found a service which has the same
                    # name and location, but different UUID.  This is
                    # probably because of a restarted service daemon.  The
                    # newly restarted service maintained its name/address,
                    # but regenerated a new, random UUID.
                    # Just replace the service UUID with the new one.
                    Llog.LogInfo("UUID mismatch for service <"
                                 + service['name']
                                 + ":"
                                 + service['location']
                                 + ">.  Updating service entry with new UUID.")
                    service['uuid'] = service_uuid
                return service
        return None


def test1():

    service_name = "myds.myblock"
    service_location = "tcp://127.0.0.1:4321"

    class TestClass():
        def __init__(self):
            self.service_found = False

        def myservicecback(self, service_uuid, name, location):
            Llog.LogInfo("Discovered service ("
                         + name
                         + ") at ("
                         + location + ")")
            if name == service_name and location == service_location:
                self.service_found = True

    t = TestClass()

    dc = DiscoveryClient(t.myservicecback)

    # Discovery server with a beacon output period of 5 seconds
    ds = DiscoveryServer(service_name, service_location, 5)

    time.sleep(6)

    assert(t.service_found is True)
    print "PASSED"


def test2():

    service_name = "myds.myblock"
    # Try a bad location
    service_location = "tcpl://127.0.0.1:4321"

    class TestClass():
        def __init__(self):
            self.service_found = False

        def myservicecback(self, service_uuid, name, location):
            Llog.LogInfo("Discovered service ("
                         + name
                         + ") at ("
                         + location + ")")
            if name == service_name and location == service_location:
                self.service_found = True

    t = TestClass()

    dc = DiscoveryClient(t.myservicecback)

    try:
        # Discovery server with a beacon output period of 5 seconds
        ds = DiscoveryServer(service_name, service_location, 5)
        assert(False)
    except:
        # This is correct, we should have asserted on the bad
        # location
        pass

    print "PASSED"


def test3():

    # Kill debug messages, there is just too many in this test.
    Llog.SetLevel("I")

    class TestClass():
        def __init__(self, service_name, service_location):
            self.service_found = False
            self.service_name = service_name
            self.service_location = service_location

        def myservicecback(self, service_uuid, name, location):
            Llog.LogInfo("Discovered service ("
                         + name
                         + ") at ("
                         + location + ")")
            if name == self.service_name and location == self.service_location:
                self.service_found = True

    t = TestClass("myds.myblock44", "tcp://127.0.0.1:4044")

    dc = DiscoveryClient(t.myservicecback)

    # Start up a few hundred services, but just look for one...

    i = 0
    while i < 100:
        service_name = "myds.myblock" + str(i)
        port = 4000 + i
        service_location = "tcp://127.0.0.1:" + str(port)

        # Discovery server with a beacon output period of 5 seconds
        ds = DiscoveryServer(service_name, service_location, 5)
        i += 1

    time.sleep(10)
    assert(t.service_found is True)
    print "PASSED"


if __name__ == '__main__':
    #test1()
    #test2()
    test3()
