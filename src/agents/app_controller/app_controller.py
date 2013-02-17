
import projpath
import system
import vitals
import protocol
import zhelpers
from local_log import *


class AppControlProtocolServer(object):

    """
        AppControlProtocol:

         client --->  HELLO                   ---> server
             The server can be either empty, as in just started up,
             or loaded, with a valid application file or
             running, with the specified application file and md5.
         client <---  HELLO <ready>           <--- server
         client <---  HELLO <loaded> <filename,md5>    <--- server
         client <---  HELLO <running> <filename,md5>   <--- server

         client --->  LOAD <filename,md5> ---> server
         client <---    LOAD unknown      <--- server
             The server does not have this file, or the md5 does
             not match.  The client must chunk the file to the
             server.
         client --->  CHUNK <size,is_last>  ---> server
         client --->  CHUNK <size,is_last>  ---> server
         client --->  CHUNK <size,is_last>  ---> server
         client <---  CHUNK <ok>            <--- server
         client --->  CHUNK <size,is_last>  ---> server
         client --->  CHUNK <size,is_last>  ---> server
         client --->  CHUNK <size,is_last=true>  ---> server
         client <---  CHUNK <done,md5> <--- server
             All bytes have been received.  The server now 
             acknowledges the presense of the original file.
         client --->  LOAD <filename,md5> ---> server
         client <---  LOAD <filename,md5> <--- server
         client --->  RUN <command>       ---> server
         client <---  RUNNING             <--- server
         client --->  STOP                ---> server
         client <---  STOPPED             <--- server
         client --->  RUN <command>       ---> server
         client <---  RUNNING             <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
             If the application stops on its own, the client receives
             the FINISHED message.
         client <---  FINISHED <return code>  <--- server
         ...
             If the client sends a bad message...
         client --->  LAOD                ---> server
         client <---  ERROR <reason/description> <--- server
    """
    states = ["INIT", "LOADING", "LOADED", "RUNNING"]

    def __init__(self):
        location_descriptor = {'name':"appctl",
                               'type':zmq.ROUTER,
                               'protocol':"tcp",
                               'bind_address':"*",
                               'port_range':[8100,8500]}
        protocol_descriptor = {'HELLO':(1,1,self.do_hello),
                               'LOAD':(3,3,self.do_load),
                               'CHUNK':(3,3,self.do_chunk),
                               'RUN':(2,2,self.do_run),
                               'STOP':(1,1,self.do_stop),
                               'DONE':(1,1,self.do_done)}
        self.proto = ProtocolServer(location_descriptor,
                                    protocol_descriptor)
        self.state = "INIT"
        self.filename = ""
        self.md5sum = ""
        self.f = None
        self.alive = True

    def do_hello(self, msg):
        msg_list = msg['message']
        if self.state == "INIT":
            msg_list = ["HELLO", "ready"]
        elif self.state == "LOADED":
            msg_list = ["HELLO", "loaded", self.filename, self.md5sum]
        elif self.state == "RUNNING":
            msg_list = ["HELLO", "running", self.filename, self.md5sum]
        elif self.state == "LOADING":
            # This is strange.  We get a HELLO in the middle of loading
            # an application file.  Oh well, just close and delete
            # our in-progress file and return 'ready'.
            self.__reset()
            msg_list = ["HELLO", "ready"]
        else:
            assert(False)

        msg['message'] = msg_list
        self.proto.send(msg)

    def do_load(self, msg):
        filename = msg['message'][1]
        md5sum = msg['message'][2]

        if self.state == "LOADED" and \
           filename == self.filename and md5sum == self.md5sum:
            msg_list = ["LOAD", filename, md5sum]
        else:
            self.filename = filename
            self.md5sum = md5sum
            self.state = "INIT"
            msg_list = ["LOAD", "unknown"]

        msg['message'] = msg_list
        self.proto.send(msg)

    def do_chunk(self, msg):
        is_last = msg['message'][1]
        data_block = msg['message'][2]

        if self.state == "INIT":
            self.__reset()
            self.__create_file()
            if self.f is not None:
                self.state = "LOADING"
            else:
                # Could not create/open the file
                msg_list = ["ERROR", "Could not open file: " + self.filename]
                msg['message'] = msg_list
                self.proto.send(msg)
                return
        elif self.state == "LOADING":
            pass
        else:
            # Problem!  We should only receive chunks in INIT or
            # LOADING.  Reset and return to INIT.
            self.__reset()
            self.state = "INIT"
            msg_list = ["ERROR", "Invalid state: " + self.state]
            msg['message'] = msg_list
            self.proto.send(msg)
            return

        self.__write_file(data_block)
        msg_list = ["CHUNK", "ok"]
        if is_last == "true":
            self.__close_file(data_block)
            md5sum = self.__calc_md5sum()
            msg_list.append(md5sum)
            self.state = "LOADED"

        msg['message'] = msg_list
        self.proto.send(msg)

    def do_run(self, msg):
        command = msg['message'][1]

        if self.state != "LOADED":
            msg_list = ["ERROR", "Invalid state: " + self.state]
        else:
            Llog.LogInfo("Executing: " + self.filename + " " + command)
            self.state = "RUNNING"
            msg_list = ["RUNNING"]
            # XXX do JavaAppExec

        msg['message'] = msg_list
        self.proto.send(msg)

    def do_stop(self, msg):
        if self.state != "RUNNING":
            msg_list = ["ERROR", "Invalid state: " + self.state]
        else:
            Llog.LogInfo("Stopping: " + self.filename)
            self.state = "LOADED"
            msg_list = ["STOPPED"]
            # XXX do JavaAppExec
        msg['message'] = msg_list
        self.proto.send(msg)

    def do_done(self, msg):
        msg_list = ["DONE"]
        msg['message'] = msg_list
        self.proto.send(msg)
        # The top-level application thread will/should be monitoring
        # our alive state.  Seeing 'False' should cause this
        # server application to close.
        self.alive = False

    def __create_file(self):
        try:
            self.f = open(self.filename, "w+")
        except:
            Llog.LogError("Cannot open " + self.filename + " for writting!")

    def __write_file(self, data_block):
        assert(self.f is not None)
        f.write(data_block)

    def __close_file(self):
        assert(self.f is not None)
        f.close()
        self.f = None

    def __calc_md5sum(self):
        assert(self.f != "")
        return zhelpers.md5sum(self.filename)


class AppControlProtocolClient(object):
    """
        For a description of the protocol, see AppControlProtocolServer
        above.
    """
    states = ["INIT", "LOADING", "CHUNKING", "LOADED", "RUNNING"]
    def __init__(self, address, port):
        location_descriptor = {'name':"appctl",
                               'type':zmq.REQ,
                               'protocol':"tcp",
                               'address':address,
                               'port':port}
        protocol_descriptor = {'HELLO':(2,4,self.do_hello),
                               'LOAD':(2,3,self.do_load),
                               'CHUNK':(2,3,self.do_chunk),
                               'RUNNING':(1,1,self.do_running),
                               'STOPPED':(1,1,self.do_stopped),
                               'FINISHED':(1,1,self.do_finished)}
        self.proto = ProtocolClient(location_descriptor,
                                    protocol_descriptor)
        self.state = "INIT"
        self.filename = ""
        self.md5sum = ""
        self.f = None
        self.alive = True

    def hello(self):
        # Send hello message to the server
        self.proto.send({'message':["HELLO"]})

    def load(self, filename):
        if self.state != "INIT":
            Llog.LogError("Cannot load file: "
                           + filename + " in state " + self.state)
            return

        md5sum = zhelpers.md5sum(filename)
        if md5sum is None:
            Llog.LogError("Cannot find file: " + filename)
            return

        # Send a LOAD message to the server for this file.  The
        # server may already have a copy.
        self.filename = filename
        self.md5sum = md5sum
        self.state = "LOADING"
        self.proto.send({'message':["LOAD", filename, md5sum]})

    def run(self, command):
        if self.state != "LOADED":
            Llog.LogError("Cannot run application: "
                           + self.filename + " in state " + self.state)
            return
        self.proto.send({'message':["RUN", command]})

    def stop(self):
        if self.state != "LOADED" and self.state != "RUNNING":
            Llog.LogError("Cannot stop application: "
                           + self.filename + " in state " + self.state)
            return
        self.proto.send({'message':["STOP"]})

    def quit(self):
        self.proto.send({'message':["QUIT"]})

    def do_hello(self, msg):
        # Callback for a hello message
        msg_list = msg['message']
        if msg_list[1] == "ready":
            # The server is ready to receive our application file
            pass
        elif msg_list[1] == "loaded":
            self.filename = msg_list[2]
            self.md5sum = msg_list[3]
            self.state = "LOADED"
        elif msg_list[1] == "running":
            self.filename = msg_list[2]
            self.md5sum = msg_list[3]
            self.state = "RUNNING"
        else:
            Llog.LogError("Invalid server state: "
                          + msg_list[1] + " received in HELLO message!")
            self.state = "ERROR"

    def do_load(self, msg):
        if self.state != "LOADING":
            Llog.LogError("Received LOAD message in state: " + self.state)
            return

        msg_list = msg['message']
        if msg_list[1] == self.filename:
            # The file is present on the server.
            if msg_list[2] != self.md5sum:
                Llog.LogError("Server file ("
                              + self.filename
                              + ") has an invalid md5sum: "
                              + msg_list[2]
                              + " The md5sum should be: "
                              + self.md5sum)
                self.state = "ERROR"
                return
            self.state = "LOADED"
            return

        # The server does not have our file present.  Lets begin to
        # chunk-copy the file over
        assert(self.f is None)
        try:
            self.f = open(filename, "rb")
        except:
            Llog.LogError("Cannot open " + filename + " for reading!")
        self.chunks_outstanding = 0
        self.state = "CHUNKING"
        self.__send_file_chunks()

    def do_chunk(self, msg):
        if self.state != "CHUNKING":
            Llog.LogError("Received CHUNK message in state: " + self.state)
            return

        msg_list = msg['message']
        if msg_list[1] == "ok":
            # Received ACK for a chunk.  Send more chunks...
            self.chunks_outstanding -= 1
            self.__send_file_chunks()
        elif msg_list[1] == "done":
            if len(msg_list)
            md5sum = msg_list[2]
            if md5sum != self.md5sum:
                Llog.LogError("Received bad MD5 from completed file copy!")
                self.state = "INIT"
                return
            self.state = "LOADED"
        else:
            Llog.LogError("Received invalid CHUNK message: " + msg_list[1])
            self.state = "ERROR"

    def do_hello(self, msg):
        pass

    def __send_file_chunks(self):
        while self.chunks_outstanding < self.max_chunks_outstanding:
            chunk = self.f.read(self.chunksize)
            if chunk != "":
                # We have a chunk of data.  Check to see if it is
                # the last chunk of data.  We do this by reading the next
                # byte in the file.  If it is empty, we know we are at
                # the end of the file.
                last_chunk = "false"
                byte = self.f.read(1)
                if byte == "":
                    # Done.  Flag the last chunk
                    last_chunk = "true"
                else:
                    # There is still more data to read.  Put the
                    # file back to where it was.
                    self.f.seek(-1,1)
                self.proto.send({'message':["CHUNK", last_chunk, chunk]})
                self.chunks_outstanding += 1
            else:
                # Strange, we have run out of data to read.  This should
                # have been detected above.
                Llog.Bug("Empty chunk read!")

