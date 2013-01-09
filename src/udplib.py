"""
    Simple UDP broadcast/multicast send/recv library
   """
import socket
import select

MAX_RECV = 1024

class UDP:

    """simple UDP send/recv class"""

    def __init__(self, port=9411, broadcast='255.255.255.255'):

        self.broadcast = broadcast
        self.port = port
        # Create UDP socket
        self.socket = socket.socket(
                                socket.AF_INET,
                                socket.SOCK_DGRAM,
                                socket.IPPROTO_UDP)
        if not self.socket:
            raise SystemError("Cannot create UDP socket!")

        try:
            # Ask operating system to let us do broadcasts from socket
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Bind UDP socket to local port so we can receive pings
            self.socket.bind(('', port))
        except socket.error:
            print ("Cannot configure UDP broadcast socket options: "
                        + str(socket.error))
            raise

    def send(self, buf):
        assert(self.socket)
        self.socket.sendto(buf, 0, (self.broadcast, self.port))

    def recv(self):
        assert(self.socket)
        return self.socket.recv(MAX_RECV)

    def recv_noblock(self):
        assert(self.socket)
        (ready_read, ready_write, in_error) = select.select([self.socket], [], [], 0)

        for socket in ready_read:
            if socket == self.socket:
                # Our socket is readable
                return self.socket.recv(MAX_RECV)
        return None
