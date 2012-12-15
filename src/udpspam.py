"""
    UDPSpam is a simple class to periodically output UDP
    broadcast packets.  It is used here primarily to output
    junk UDP packets for testing
   """
import udplib
import threading
import time
import uuid

MAX_PDU_SIZE = 500


class UDPSpam:

    """
    """
    # Optional argument to specify how often this module will
    # output a discovery beacon packet, in units of seconds.
    def __init__(self, port=9411, period=10):
        self.udp = udplib.UDP()
        self.update_period = period
        self.alive = True
        self.output_lines = []
        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True
        self.thread.start()

    def __thread_entry(self):
        while self.alive:
            time.sleep(self.update_period)
            self.__send_output()
            self.__recv_input()

    def __send_output(self):
        for line in self.output_lines:
            self.udp.send(line)

    def __recv_input(self):
        while True:
            line = self.udp.recv_noblock(MAX_PDU_SIZE)
            if line is None:
                break

    def add_line(self, line):
        self.output_lines.append(line)

    def close(self):
        self.alive = False
        self.thread.join(1.0)
        if self.thread.isAlive() is True:
            print "Discovery thread did not die!"
        self.output_lines = []
