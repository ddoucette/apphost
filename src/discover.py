"""
    Discovery is a simple object to register services and receive
    lists of services available in the discoverable broadcast
    network.
   """
import threading
import time
import uuid
import re
import protocol
import override
import location 
from interface2 import *

MAX_BEACON_SIZE = 500


class Discover(Interface):

    """
    """
    # Optional argument to specify how often this module will
    # output a discovery beacon packet, in units of seconds.
    def __init__(self, service_name, service_location, period=10, ageout=40):
    
        Interface.__init__(self)

        self.service_name = service_name
        self.service_location = service_location
        self.period = period
        self.ageout = ageout

        p = protocol.Protocol(
                        "DISCOVERY",
                        "UDP_BROADCAST",
                        ["DISCOVER_BEACON"])

        # Each instance of this object creates it's own identity.
        # This means we generate an UUID here and it is used
        # to identify this object (and it's caller) uniquely.
        self.l_uuid = uuid.uuid4()
        self.discovered_service_list = []
        self.notifiers = {}

        # Add the protocol to our interface.  This allows the protocol's
        # socket to be added to the interface's poller.
        self.set_protocol(p)

        self.timer = self.create_timer("DISCOVER_TIMER", period)
        assert(self.timer is not None)

    @overrides(Interface)
    def process_timer(self, timer_name):
        assert(timer_name == "DISCOVER_TIMER")
        self.__send_beacon()
        self.__recv_beacons()
        self.__process_discovery_list()

    def __send_beacon(self):
        #
        # The beacon frame looks like this:
        #
        # [ BEACON UUID service1-name service1-location
        #       service2-name service2-location ... ]
        #
        #if len(self.registered_service_list) == 0:
        #    return

        beacon = str(self.l_uuid)
        for service in self.registered_service_list:
            beacon += " " + service['name']
            beacon += " " + service['location']
        self.protocol.send(beacon)

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

                # Notify those who are registered for this
                # service name
                for name in self.notifiers:
                    if name == discovered_service['name']:
                        notifier_list = self.notifiers[name]
                        for notifier_obj in notifier_list:
                            notifier_obj.notify_remove(discovered_service)
                        self.notifiers[name] = None
                        break

    @overrides(Interface)
    def process_protocol(self, beacon):

        # Receive and process the beacon frame.
        # The format is:
        # BEACON UUID [service_name service_location]...
        # The Protocol object will strip off the 'BEACON'
        # portion of the frame
        pieces = beacon.split()

        if len(pieces) < 1:
            print "Invalid/short beacon received: " + beacon
            return

        try:
            r_uuid = uuid.UUID(pieces[0])
        except ValueError:
            # The UUID was most likely malformed in the beacon.
            # Just discard this beacon.
            print "Malformed UUID string: " + pieces[0]
            return

        # If we are using broadcast, we may receive
        # our own beacons...
        if r_uuid == self.l_uuid:
            return

        # print "Beacon: <" + str(self.l_uuid) + "> ==> [" + str(r_uuid) + "]"

        #
        # We have a valid beacon frame from another source.
        # Process the services to see if there are any
        # new services contained that we do not have in
        # our discovered_services_list.
        #
        i = 1
        while i + 1 < len(pieces):
            service_name = pieces[i]
            service_location = pieces[i + 1]

            # Lets make sure the location meets our expectations
            # for format
            location_check = check_location(service_location)
            if location_check is False:
                print "Malformed/invalid location!: " + service_location
                i = i + 2
                continue

            service = self.find_discovered_service(
                                    service_name,
                                    service_location)
            if service is None:
                #
                # Service does not already exist in our list, add it.
                # Timestamp the discovered service so we can
                # remove them if they expire
                #
                now = time.time()
                new_service = {'name': service_name,
                               'location': service_location,
                               'time': now}
                self.discovered_service_list.append(new_service)

                # Notify all those who have registered for this
                # service
                for name in self.notifiers:
                    if name == service_name:
                        notifier_list = self.notifiers[name]
                        for notifier_obj in notifier_list:
                            notifier_obj.notify_add(new_service)

                #print ("New: " + str(self) + " " + str(r_uuid)
                #                + " " + str(new_service))
            else:
                #
                # The service exists.  Update the timestamp
                # in the service entry to keep it from
                # expiring
                #
                service['time'] = time.time()
            i = i + 2

    def get_services(self):
        return self.discovered_service_list[:]

    def discovered_service_exists(self, service):
        discovered_service = self.find_discovered_service(
                                    service['name'],
                                    service['location'])
        if discovered_service is None:
            return False
        return True

    def find_discovered_service(self, name, location):
        for discovered_service in self.discovered_service_list:
            if ((name == discovered_service['name']) and
                (location == discovered_service['location'])):
                return discovered_service
        return None

    def __str__(self):
        return str(self.l_uuid)

    @overrides(Interface)
    def close(self):
        if self.timer is not None:
            self.timer.close()
            self.timer = None

        Interface.close()
        self.registered_service_list = []
        self.discovered_service_list = []


"""
    DiscoverNotifier is an interface class for notifications when
    a service is discovered
"""


class DiscoverNotifier:

    def __init__(self):
        assert(False)

    """
        The notify_add method is called when a service in the
        subscriber list is discovered.
    """
    def notify_add(self, service):
        pass

    """
        The notify_add method is called when a service in the
        subscriber list is removed.
    """

    def notify_remove(self, service):
        pass


def test1():
    # Start up 2 discover objects.  They should discover
    # eachother.

    d1_service = {'name': 'HOOZA', 'location': 'tcp://localhost:4321'}
    d2_service = {'name': 'WHATZA', 'location': 'tcp://localhost:2234'}

    d1 = Discover(d1_service['name'], d1_service['location'], 1, 4)
    d2 = Discover(d2_service['name'], d2_service['location'], 1, 4)
            
    print "Added both services!"
    time.sleep(3)

    #
    # With a ping time of 1 second, we should now expect
    # to find that both d1 and d2 have eachother's services
    # in their discovered service list
    d1_has_d2 = d1.discovered_service_exists(d2_service)
    d2_has_d1 = d2.discovered_service_exists(d1_service)
    assert(d1_has_d2 is True)
    assert(d2_has_d1 is True)

    # Now close d1 and wait 6 seconds.  The service should be
    # removed from d2.
    d1.close()
    print "Closed service d1!"
    time.sleep(6)

    d2_has_d1 = d2.discovered_service_exists(d1_service)
    assert(d2_has_d1 is False)
    d2.close()
    print "PASSED"
    return True


def test2():
    #
    # Now lets throw some junk at the UDP port which receives
    # the beacon frames...
    #

    # Start up 2 discover objects.  They should discover
    # eachother.
    d1 = Discover(1, 4)
    d1_service = {'name': 'HOOZA', 'location': 'tcp://localhost:4321'}
    d1.register_service(d1_service)

    print "Added services!"
    time.sleep(3)

    import udpspam
    udp_spam = udpspam.UDPSpam(period=1)

    # Add the spam output to our spammer
    # Start with a mal-formed broadcast beacon
    udp_spam.add_line("BEACON junk-323323-")

    # And a line with lots of extra white-space and an invalid UUID
    # The UUID below is missing 1 character
    udp_spam.add_line(
            "BEACON   161f0e5b-7cbe-4f9b-bd29-9650833ce95   blah   xxxx    ")

    # And a line with an odd number of name-service pairs
    # No service should be added to the registered service list because
    # the location strings are bad.
    line = "BEACON   aaaa0e5b-7cbe-4f9b-bd29-9650833ce958    "
    line = line + "myservice xxx  myotherservice  "
    udp_spam.add_line(line)

    # And add the same service as above, with the name/location replicated
    # This should not trigger any assertions or errors
    line = "BEACON   aaaa0e5b-7cbe-4f9b-bd29-9650833ce958    "
    line = line + "myservice xxx    myservice xxx "
    line = line + "myservice tcp://##$$:1234 yyy"
    udp_spam.add_line(line)

    # And add some strange characters, just to make sure...
    line = "BEACON   aaaa0e5b-7cbe-4f9b-bd29-9650833ce958    "
    line = line + "???%%^&$ &&##@@$%^++   **&!\'\'\'~~~~((##(#)T% "
    line = line + "><>?D:P#)$($&#(@))"
    udp_spam.add_line(line)

    # And some whitespace, all by itself
    line = " "
    udp_spam.add_line(line)

    time.sleep(5)

    # Verify d1 does not have any services
    blah_service = {'name': 'blah', 'location': 'xxxx'}
    d1_has_blah = d1.discovered_service_exists(blah_service)
    assert(d1_has_blah is False)

    my_service = {'name': 'myservice', 'location': 'xxx'}
    other_service = {'name': 'myotherservice', 'location': ''}
    d1_has_my_service = d1.discovered_service_exists(my_service)
    d1_has_other_service = d1.discovered_service_exists(other_service)
    assert(d1_has_my_service is False)
    assert(d1_has_other_service is False)

    my_service = {'name': 'myservice', 'location': 'tcp://##$$:1234'}
    d1_has_my_service = d1.discovered_service_exists(my_service)
    assert(d1_has_my_service is False)

    d1.close()
    udp_spam.close()
    print "PASSED"
    return True


def test3():
    #
    # Open up 100s of discovery nodes.
    #

    period = 10
    ageout = 40
    dlist = []
    service_list = []
    nr_discovery_objs = 100

    i = 0
    while i < nr_discovery_objs:
        service_name = "service" + str(i)
        service_location = "tcp://localhost:" + str(4000 + i)
        service = {'name': service_name, 'location': service_location}
        service_list.append(service)

        d = Discover(period, ageout)
        d.register_service(service)
        dlist.append(d)
        i = i + 1

    print "Added " + str(i) + " services!"
    time.sleep(2 * period)

    #
    # Everyone should know eachother's name by now...
    #
    print "Checking all service discoveries..."

    i = 0
    while i < nr_discovery_objs:
        d = dlist[i]
        j = 0
        while j < nr_discovery_objs:
            if j != i:
                service = service_list[j]
                has_service = d.discovered_service_exists(service)
                assert(has_service is True)
            j = j + 1
        i = i + 1

    print "Closing discovery objects ..."
    i = 0
    while i < nr_discovery_objs:
        d = dlist[i]
        d.close()
        i = i + 1

    print "PASSED"


def test4():
    #
    # Verify the DiscoverNotifier interface works.
    #
    class Mytestobj(DiscoverNotifier):
        def __init__(self):
            self.isAdded = False

        def notify_add(self, service):
            assert(service['name'] == "SOMESRV")
            self.isAdded = True

        def notify_remove(self, service):
            assert(service['name'] == "SOMESRV")
            self.isAdded = False

    mto = Mytestobj()
    d1 = Discover(1, 4)
    d1.register_notifier("SOMESRV", mto)

    d2 = Discover(1, 4)
    d2_service = {'name': 'SOMESRV', 'location': 'tcp://localhost:3345'}
    d2.register_service(d2_service)

    time.sleep(5)

    assert(mto.isAdded is True)

    # Now remove the service and verify we are notified again
    d2.close()
    del d2

    time.sleep(6)
    assert(mto.isAdded is False)

    # Attempt to re-register for the notification
    got_assertion = False
    try:
        d1.register_notifier("SOMESRV", mto)
    except AssertionError:
        got_assertion = True

    assert(got_assertion)

    # Recreate the service, but with a minor name change
    d2 = Discover(1, 4)
    d2_service = {'name': 'SOMESRV2', 'location': 'tcp://localhost:3345'}
    d2.register_service(d2_service)

    time.sleep(5)
    assert(mto.isAdded is False)

    print "PASSED"


if __name__ == '__main__':
    test1()
