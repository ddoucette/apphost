"""
"""
import protocol
import interface
import system


class EventSource():

    """
    """
    LOG = 1
    BOOLEAN = 2
    VALUE = 3
    event_types = [LOG, BOOLEAN, VALUE]
    port_range = [7000, 8000]
    ip_addr = None
    proto = None
    interface = None
    discovery = None

    def __init__(self, event_name, event_type):
        assert(event_type in EventSource.event_types)
        assert(event_name is not None)
        self.event_type = event_type
        self.event_name = event_name

    def send(self, contents):
        msg = " ".join([System.get_user_name(),
                        System.get_application_name(),
                        self.event_type,
                        self.event_name,
                        contents])
        self.interface.push_in_msg(msg)

    @staticmethod
    def Init(bind_addr="*"):
        assert(EventSource.proto is None)
        assert(EventSource.interface is None)
        assert(EventSource.ip_addr is None)

        EventSource.proto = protocol.Protocol(zmq.PUB)
        EventSource.proto.create_server("tcp",
                                        bind_addr,
                                        EventSource.port_range)

        EventSource.interface = Interface()
        EventSource.interface.set_protocol(EventSource.proto)

        if EventSource.ip_addr is None:
            EventSource.ip_addr = get_local_ipaddr()

        # Create a discovery object to broadcast the existence
        # of this event
        service_location = "tcp://" \
                         + EventSource.ip_addr \
                         + ":" + EventSource.proto.port
        EventSource.discovery = DiscoveryServer(System.get_user_name(),
                                                System.get_application_name(),
                                                "EVENT",
                                                service_location)
        assert(discovery is not None)

    @staticmethod
    def Create(event_name, event_type):
        return EventSource(event_name, event_type)

class EventCollector():

    def __init__(self, event_type, event_cback,
                 user_name="", application_name=""):
        self.event_type = event_type
        if user_name == "":
            user_name = System.get_user_name()

        if application_name == "":
            application_name = System.get_application_name()

        self.user_name = user_name
        self.application_name = application_name
        self.protocol = None
        self.interface = Interface()
        self.dc = DiscoveryClient(self.service_add, self.service_remove)

    def service_add(self, service):
        if service.user_name == self.user_name and \
           service.application_name == self.application_name and \
           service.service_name == "EVENT":
            # We have found a matching service.  We must now get
            # the location of this service and open up a SUB socket
            # to it.  Once open, we must subscribe to the events
            # of interest.
            Llog.LogInfo("Found service: " + str(service))

            addr_info = location.parse_location(service.location)
            if addr_info == None:
                Llog.LogError("Invalid location: " + service.location)
                return

            self.protocol = protocol.Protocol(zmq.SUB)
            self.protocol.create_client(addr_info[0],
                                        addr_info[1],
                                        addr_info[2])
            self.protocol.subscribe(" ".join([self.user_name,
                                              self.application_name,
                                              self.event_type]))

    sub.subscribe("app.foo")
                                        bind_addr,
                                        EventSource.port_range)


