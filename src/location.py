"""
"""
import re


def parse_location(service_location):

    # Check to see if the location matches tcp|udp://X.X.X.X:port
    m = re.match(
                r'(tcp|udp)://([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)',
                service_location)
    if m is None:
        # Check to see if the location matches tcp:domainname:port
        m = re.match(
                    r'(tcp)://([0-9a-zA-Z\-]+):([0-9]+)',
                    service_location)
        if m is None:
            # Check to see if the location matches tcp:*:port
            m = re.match(
                        r'(tcp)://(\*+):([0-9]+)',
                        service_location)
            if m is None:
                return None

    service_protocol = m.group(1)
    service_address = m.group(2)
    service_port = int(m.group(3))
    return [service_protocol, service_address, service_port]


def check_location(service_location):
    if parse_location(service_location) is not None:
        return True
    return False


def test1():
    r = check_location("tcp://localhost:4321")
    assert(r is True)
    r = check_location("tcpp://localhost:4321")
    assert(r is False)
    r = check_location("tcp://192.168.1.:4321")
    assert(r is False)
    r = check_location("tcp://192.168.1.3.4:4321")
    assert(r is False)
    r = check_location("tcp://*:4321")
    assert(r is True)
    r = check_location("udp://192.168.3.2:4321")
    assert(r is True)
    info = parse_location("udp://255.255.255.255:4321")
    assert(info[0] == "udp")
    assert(info[1] == "255.255.255.255")
    assert(info[2] == 4321)

    print "PASSED"


if __name__ == '__main__':
    test1()
