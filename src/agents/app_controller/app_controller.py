
import projpath
import system
import vitals
import protocol
import zmq
import zhelpers
from local_log import *
from override import *


class AppControlProtocol(object):
    """
        AppControlProtocol:

         client --->  HOWDY <username><major><minor>   ---> server
             The server can be either empty, as in just started up,
             or loaded, with a valid application file or
             running, with the specified application file and md5.
         client <---  HI <major><minor><state><file_name><md5> <--- server
                         or
         client <---  ERROR <reason>          <--- server

         client --->  LOAD <file_name,md5> ---> server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client <---  CHUNK_OK         <--- server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last=true>  ---> server
         client <---  LOAD_OK <file_name,md5>    <--- server
             All bytes have been received.  The server now 
             acknowledges the presense of the original file.

         client --->  RUN <command>       ---> server
         client <---  RUN_OK              <--- server
         client --->  STOP                ---> server
         client <---  STOP_OK             <--- server
         client --->  RUN <command>       ---> server
         client <---  RUN_OK              <--- server
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
    messages = [{'HOWDY': [{'name':'user name', \
                            'type':types.StringType}, \
                           {'name':'major version', \
                            'type':types.IntType}, \
                           {'name':'minor version', \
                            'type':types.IntType}]}, \
                  {'HI': [{'name':'major version', \
                           'type':types.IntType}, \
                          {'name':'minor version', \
                           'type':types.IntType}, \
                          {'name':'state', \
                           'type':types.StringType}, \
                          {'name':'file_name', \
                           'type':types.StringType}, \
                          {'name':'md5sum', \
                           'type':types.StringType}]}, \
                  {'LOAD': [{'name':'file_name', \
                             'type':types.StringType}, \
                            {'name':'md5sum', \
                             'type':types.StringType}]}, \
                  {'LOAD_OK': [{'name':'file_name', \
                                'type':types.StringType}, \
                               {'name':'md5sum', \
                                'type':types.StringType}]}, \
                  {'CHUNK': [{'name':'is last', \
                              'type':types.BooleanType}, \
                             {'name':'data block', \
                              'type':types.StringType}]}, \
                  {'CHUNK_OK': []}, \
                  {'RUN': [{'name':'command', \
                            'type':types.StringType}]}, \
                  {'RUN_OK': []}, \
                  {'STOP': []}, \
                  {'STOP_OK': []}, \
                  {'FINISHED': [{'name':'error code', \
                                 'type':types.IntType}]}, \
                  {'EVENT': [{'name':'event type', \
                              'type':types.StringType},
                             {'name':'event name', \
                              'type':types.StringType}, \
                             {'name':'timestamp', \
                              'type':types.StringType}, \
                             {'name':'*', \
                              'type':types.StringType, \
                              'min':0,
                              'max':20}]}, \
                  {'QUIT': []}]

class AppControlProtocolServer(object):

    version_major = 1
    version_minor = 0

    def __init__(self, user_name):
        states = [{'name':"READY",
                   'actions':[],
                   'messages':[{'name':"LOAD",
                                'action':self.do_load,
                                'next_state':"LOADING"}],
                  {'name':"LOADING",
                   'actions':[{'name':"load_complete",
                               'action':self.do_load_complete,
                               'next_state':"LOADED"}],
                   'messages':[{'name':"CHUNK",
                                'action':self.do_chunk,
                                'next_state':"LOADING"}]},
                  {'name':"LOADED",
                   'actions':[],
                   'messages':[{'name':"RUN",
                                'action':self.do_run,
                                'next_state':"RUNNING"}],
                  {'name':"RUNNING",
                   'actions':[{'name':"finished",
                               'action':self.do_finished,
                               'next_state':"LOADED"},
                              {'name':"event",
                               'action':self.do_event,
                               'next_state':"RUNNING"}],
                   'messages':[{'name':"STOP",
                                'action':self.do_stop,
                                'next_state':"LOADED"}],
                  {'name':"*",
                   'actions':[{'name':"error",
                               'action':self.do_error,
                               'next_state':"-"}],
                   'messages':[{'name':"QUIT",
                                'action':self.do_quit,
                                'next_state':"READY"},
                               {'name':"HOWDY",
                                'action':self.do_howdy,
                                'next_state':"-"}]}]
        location = {'type':zmq.ROUTER,
                    'protocol':"tcp",
                    'bind_address':"*",
                    'port_range':[8100,8500]}

        self.proto = protocol.ProtocolServer(
                                    "app-ctrl",
                                    location,
                                    AppControlProtocol.messages,
                                    states)
        self.user_name = user_name
        self.file_name = ""
        self.md5sum = ""
        self.f = None
        self.alive = True
        self.client_version_minor = 0

    def do_howdy(self, msg):

        # Verify the username and protocol version information.
        # The username is particularily important as a client may
        # be connecting to this port in error (i.e. for another user).
        msg_list = msg['message']
        user_name = msg_list[1]
        version_major = msg_list[2]
        version_minor = msg_list[3]

        if self.user_name != user_name:
            self.send_error("Invalid user name specified!")
            return

        if self.version_major != version_major:
            self.send_error("Invalid major version!  Version ("
                            + self.version_major + ") supported!")
            return

        self.client_version_minor = version_minor

        state = self.proto.get_state()
        if self.file_name = "":
            file_name = "-"
            md5sum = "-"
        else:
            file_name = self.file_name
            md5sum = self.md5sum

        msg_list = ["HI",
                    self.version_major,
                    self.version_minor,
                    state,
                    file_name,
                    md5sum
                    ]
        msg['message'] = msg_list
        self.proto.send(msg)

    def do_load(self, msg):
        self.file_name = msg['message'][1]
        self.md5sum = msg['message'][2]

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
                msg_list = ["ERROR", "Could not open file: " + self.file_name]
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
            Llog.LogInfo("Executing: " + self.file_name + " " + command)
            self.state = "RUNNING"
            msg_list = ["RUNNING"]
            # XXX do JavaAppExec

        msg['message'] = msg_list
        self.proto.send(msg)

    def do_stop(self, msg):
        if self.state != "RUNNING":
            msg_list = ["ERROR", "Invalid state: " + self.state]
        else:
            Llog.LogInfo("Stopping: " + self.file_name)
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
            self.f = open(self.file_name, "w+")
        except:
            Llog.LogError("Cannot open " + self.file_name + " for writting!")

    def __reset(self):
        if self.f is not None:
            self.f.close()
            self.f = None
        self.state = "INIT"
        self.file_name = ""
        self.md5sum = ""

    def __write_file(self, data_block):
        assert(self.f is not None)
        f.write(data_block)

    def __close_file(self):
        assert(self.f is not None)
        f.close()
        self.f = None

    def __calc_md5sum(self):
        assert(self.f != "")
        return zhelpers.md5sum(self.file_name)


class AppControlProtocolClient(object):
    """
        For a description of the protocol, see AppControlProtocol
        above.
    """
    states = ["INIT", "LOADING", "CHUNKING", "LOADED", "RUNNING"]
    max_chunks_outstanding = 5
    chunksize = 1000

    def __init__(self, address, port):
        location_descriptor = {'name':"appctl",
                               'type':zmq.ROUTER,
                               'protocol':"tcp",
                               'address':address,
                               'port':port}
        protocol_descriptor = {'HELLO':(2,4,self.do_hello),
                               'LOAD':(2,3,self.do_load),
                               'CHUNK':(2,3,self.do_chunk),
                               'RUNNING':(1,1,self.do_running),
                               'STOPPED':(1,1,self.do_stopped),
                               'FINISHED':(1,1,self.do_finished)}
        self.proto = protocol.ProtocolClient(
                                    location_descriptor,
                                    protocol_descriptor)
        self.state = "INIT"
        self.file_name = ""
        self.md5sum = ""
        self.f = None
        self.alive = True
        self.chunks_outstanding = 0
        self.hello()

    def hello(self):
        # Send hello message to the server
        Llog.LogInfo("Sending a HELLO")
        self.proto.send({'message':["HELLO"]})

    def load(self, file_name):
        if self.state != "INIT":
            Llog.LogError("Cannot load file: "
                           + file_name + " in state " + self.state)
            return

        md5sum = zhelpers.md5sum(file_name)
        if md5sum is None:
            Llog.LogError("Cannot find file: " + file_name)
            return

        # Send a LOAD message to the server for this file.  The
        # server may already have a copy.
        self.file_name = file_name
        self.md5sum = md5sum
        self.state = "LOADING"
        self.proto.send({'message':["LOAD", file_name, md5sum]})

    def run(self, command):
        if self.state != "LOADED":
            Llog.LogError("Cannot run application: "
                           + self.file_name + " in state " + self.state)
            return
        self.proto.send({'message':["RUN", command]})

    def stop(self):
        if self.state != "LOADED" and self.state != "RUNNING":
            Llog.LogError("Cannot stop application: "
                           + self.file_name + " in state " + self.state)
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
            self.file_name = msg_list[2]
            self.md5sum = msg_list[3]
            self.state = "LOADED"
        elif msg_list[1] == "running":
            self.file_name = msg_list[2]
            self.md5sum = msg_list[3]
            self.state = "RUNNING"
        else:
            Llog.LogError("Invalid server state: "
                          + msg_list[1] + " received in HELLO message!")
            self.state = "ERROR"
        Llog.LogInfo("Received hello response! " + msg_list[1])

    def do_load(self, msg):
        if self.state != "LOADING":
            Llog.LogError("Received LOAD message in state: " + self.state)
            return

        msg_list = msg['message']
        if msg_list[1] == self.file_name:
            # The file is present on the server.
            if msg_list[2] != self.md5sum:
                Llog.LogError("Server file ("
                              + self.file_name
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
            self.f = open(self.file_name, "rb")
            assert(self.f is not None)
        except:
            Llog.LogError("Cannot open " + self.file_name + " for reading!")
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
            if len(msg_list) != 3:
                Llog.LogError("Received bad 'done' message!")
                self.state = "INIT"
                return
            md5sum = msg_list[2]
            if md5sum != self.md5sum:
                Llog.LogError("Received bad MD5 from completed file copy!")
                self.state = "INIT"
                return
            self.state = "LOADED"
        else:
            Llog.LogError("Received invalid CHUNK message: " + msg_list[1])
            self.state = "ERROR"

    def do_running(self, msg):
        if self.state != "LOADED":
            Llog.LogError("Received RUNNING message in state: " + self.state)
            return
        self.state = "RUNNING"

    def do_stopped(self, msg):
        if self.state != "RUNNING":
            Llog.LogError("Received STOPPED message in state: " + self.state)
            return
        self.state = "LOADED"

    def do_finished(self, msg):
        if self.state != "RUNNING":
            Llog.LogError("Received FINISHED message in state: " + self.state)
            return
        self.return_code = msg['message'][1]
        self.state = "LOADED"

    def __send_file_chunks(self):
        assert(self.f is not None)
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


def test1():

    Llog.SetLevel("I")
    s = AppControlProtocolServer()
    c = AppControlProtocolClient("127.0.0.1", 8100)

    time.sleep(1)
    assert(c.state == "INIT")
    c.load("testfile.bin")
    time.sleep(5)
    assert(c.state == "LOADED")


if __name__ == '__main__':
    test1()
