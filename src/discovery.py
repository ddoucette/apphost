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

    period = 10
    ageout = 40

    # Optional argument to specify how often this module will
    # output a discovery beacon packet, in units of seconds.
    def __init__(self, user_name,
                       application_name,
                       service_name,
                       service_location):
        assert(user_name != "")
        assert(application_name != "")
        assert(service_name != "")
        assert(service_location != "")

        # Each instance of this object creates it's own identity.
        # This means we generate an UUID here and it is used
        # to identify this object (and it's caller) uniquely.
        self.user_name = user_name
        self.application_name = application_name
        self.service_name = service_name
        self.service_location = service_location
        self.uuid = uuid.uuid4()
        self.udp = udplib.UDP()
        self.period = DiscoveryServer.period

        # Verify the location makes sense, syntactically
        location_check = location.check_location(service_location)
        assert(location_check is True)

        self.alive = True
        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

    def __del__(self):
        self.close()

    def close(self):
        if self.alive is True:
            Llog.LogDebug(str(self) + " closing...")
            self.alive = False
            # Give it some time to die.  The thread must have a chance
            # to process the 'alive' value and exit
            time.sleep(self.period)

    def __thread_entry(self):
        while self.alive:
            self.__send_beacon()
            time.sleep(self.period)

    def __send_beacon(self):
        beacon = " ".join(["BEACON",
                           str(self.uuid),
                           self.user_name,
                           self.application_name,
                           self.service_name,
                           self.service_location])
        self.udp.send(beacon)

    def __str__(self):
        return "-".join([str(self.uuid),
                         self.user_name,
                         self.application_name,
                         self.service_name,
                         self.service_location])


class DiscoveryService:
    def __init__(self, user_name, application_name, service_name, location,
                 uuid="00000000-0000-0000-0000-000000000000"):
        self.user_name = user_name
        self.application_name = application_name
        self.service_name = service_name
        self.location = location
        self.uuid = str(uuid)
        self.time = time.time()

    def __str__(self):
        return "-".join([self.user_name,
                         self.application_name,
                         self.service_name,
                         self.location,
                         self.uuid])

    def __eq__(self, service):
        if self.user_name == service.user_name and \
           self.application_name == service.application_name and \
           self.service_name == service.service_name and \
           self.location == service.location:
            return True
        return False

class DiscoveryClient:

    """
    """
    ageout = 40

    # ageout - optional argument to specify how long before an entry
    #          discovered will be removed from our list if another beacon
    #          for it is not received.
    def __init__(self, service_add_cback, service_remove_cback):
        assert(service_add_cback is not None)
        assert(service_remove_cback is not None)

        self.udp = udplib.UDP()
        self.ageout = DiscoveryClient.ageout
        self.alive = True
        self.service_add_cback = service_add_cback
        self.service_remove_cback = service_remove_cback
        self.discovered_service_list = []
        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

    def __thread_entry(self):
        while self.alive:
            self.__recv_beacons()
            self.__process_discovery_list()

    def __del__(self):
        self.close()

    def close(self):
        self.alive = False
        # We need to wait for a few seconds to allow the main thread
        # enough time to re-read the alive flag and exit
        time.sleep(5)

    def __process_discovery_list(self):
        # check each entry in our discovered services list to
        # see if any entries are too old to keep around
        now = time.time()
        service_list = self.discovered_service_list[:]
        for service in service_list:
            if service.time < now - self.ageout:
                # The entry is too old.  Dump it
                self.discovered_service_list.remove(service)
                self.service_remove_cback(service)

    def __recv_beacons(self):
        while True:
            beacon = self.udp.recv_timeout(1)
            if beacon is None:
                break
            self.__process_beacon(beacon)

    def __process_beacon(self, beacon):
        pieces = beacon.split()

        if len(pieces) != 6:
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
        service_uuid, \
        user_name, \
        application_name, \
        service_name, \
        service_location = pieces[1:6]

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

        new_service = DiscoveryService(user_name,
                                       application_name,
                                       service_name,
                                       service_location,
                                       service_uuid)

        # Check to see if we already have this entry in our internal
        # list of services
        service = self.find_discovered_service(new_service)

        if service is None:
            # Service does not already exist in our list, add it.
            # Timestamp the discovered service so we can
            # remove them if they expire.
            self.discovered_service_list.append(new_service)

            # Callout to notify the user of this new service
            if self.service_add_cback is not None:
                self.service_add_cback(new_service)
        else:
            # The service exists.  Update the timestamp
            # in the service entry to keep it from
            # expiring
            service.time = time.time()

    def find_discovered_service(self, service):
        for discovered_service in self.discovered_service_list:
            if discovered_service == service:
                if discovered_service.uuid != service.uuid:
                    # We have just found a service which has the same
                    # name and location, but different UUID.  This is
                    # probably because of a restarted service daemon.
                    # Because it is a restarted service, we need to
                    # re-create the socket connection.
                    # We need to declare the current service as
                    # 'removed' and add in this new service.
                    Llog.LogInfo("UUID mismatch for service <" + str(service)
                                 + ">.  Announcing service removal.")
                    self.discovered_service_list.remove(discovered_service)
                    self.service_remove_cback(discovered_service)
                    return None
                else:
                    return discovered_service
        return None

def test1():

    class TestClass():
        def __init__(self, discovery_service):
            self.service = discovery_service 
            self.service_found = False

        def service_add(self, service):
            Llog.LogInfo("Discovered service (" + str(service) + ")")
            if self.service == service:
                Llog.LogInfo("Found our service!")
                self.service_found = True

        def service_remove(self, service):
            match = self.service == service
            if match is True:
                self.service_found = False

    user_name = "sysadm"
    application_name = "myapp123"
    service_name = "EVENT"
    service_location = "tcp://127.0.0.1:4321"
    service = DiscoveryService(user_name,
                               application_name,
                               service_name,
                               service_location)

    t = TestClass(service)

    dc = DiscoveryClient(t.service_add, t.service_remove)

    # Discovery server with a beacon output period of 5 seconds
    DiscoveryServer.period = 5
    ds = DiscoveryServer(user_name,
                         application_name,
                         service_name,
                         service_location)
    time.sleep(6)

    assert(t.service_found is True)
    print "PASSED"

if __name__ == '__main__':
    test1()
