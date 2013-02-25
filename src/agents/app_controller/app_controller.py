
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
         client <---  HI <major,minor,state,file_name,md5,label> <--- server
                         or
         client <---  ERROR <reason>          <--- server

         client --->  LOAD <file_name,md5,label> ---> server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client <---  CHUNK_OK         <--- server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last=true>  ---> server
         client <---  LOAD_OK <file_name,md5,label> <--- server
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
                           'type':types.StringType}, \
                          {'name':'label', \
                           'type':types.StringType}]}, \
                  {'LOAD': [{'name':'file_name', \
                             'type':types.StringType}, \
                            {'name':'md5sum', \
                             'type':types.StringType}, \
                            {'name':'label', \
                             'type':types.StringType}]}, \
                  {'LOAD_OK': [{'name':'file_name', \
                                'type':types.StringType}, \
                               {'name':'md5sum', \
                                'type':types.StringType}, \
                               {'name':'label', \
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
                   'actions':[{'name':"load_complete",
                               'action':self.do_load_complete,
                               'next_state':"LOADED"}],
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
        if self.file_name == "":
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
                    md5sum]
        msg['message'] = msg_list
        self.proto.send(msg)

    def do_load(self, msg):
        self.file_name = msg['message'][1]
        self.md5sum = msg['message'][2]

        # Check to see if we already have the file.
        # If the md5sum fails below, or is different, we hav
        # our answer.
        md5sum = zhelpers.md5sum(self.file_name)
        if md5sum is not None and md5sum == self.md5sum:
            # We have this file already.  Issue the load_complete action.
            self.proto.action("load_complete", [msg['address']])
        else:
            # Open the file for writting.  We should soon be
            # receiving chunks of file data for this file.
            self.__create_file()

    def do_load_complete(self, action, action_args):
        msg = {'address':action_args[0],
               'message':["LOAD_OK", self.file_name, self.md5sum]}
        self.proto.send(msg)

    def do_chunk(self, msg):
        is_last = msg['message'][1]
        data_block = msg['message'][2]

        self.__write_file(data_block)
        msg_list = ["CHUNK_OK"]
        msg['message'] = msg_list
        self.proto.send(msg)

        if is_last == "true":
            # Close the file and check the md5.  It should match
            # the md5 specified at the start of loading by
            # the client.  If not, error out.
            self.__close_file(data_block)
            md5sum = zhelpers.md5sum(self.file_name)
            assert(md5sum is not None)
            if md5sum != self.md5sum:
                self.log_error("File does not match md5sum specified!")
                self.proto.action("error")
                return

            # File is done.  Issue the load complete action.
            self.proto.action("load_complete")

    def do_run(self, msg):
        command = msg['message'][1]
        msg_list = ["RUN_OK"]
        msg['message'] = msg_list
        self.proto.send(msg)

    def do_stop(self, msg):
        command = msg['message'][1]
        msg_list = ["RUN_OK"]
        msg['message'] = msg_list
        self.proto.send(msg)

    def __create_file(self):
        if self.f is not None:
            self.f.close()
        try:
            self.f = open(self.file_name, "w+")
        except:
            Llog.LogError("Cannot open " + self.file_name + " for writting!")

    def __write_chunk(self, data_block):
        assert(self.f is not None)
        self.f.write(data_block)

    def __close_file(self):
        assert(self.f is not None)
        self.f.close()
        self.f = None


class AppControlProtocolClient(object):
    """
        For a description of the protocol, see AppControlProtocol
        above.
    """
    max_chunks_outstanding = 5
    chunksize = 1000

    def __init__(self, user_name, address, port):
        states = [{'name':"INIT",
                   'actions':[{'name':"say_howdy",
                               'action':self.a_say_howdy,
                               'next_state':"INIT"},
                              {'name':"is_running",
                               'action':None,
                               'next_state':"RUNNING"},
                              {'name':"is_loaded",
                               'action':None,
                               'next_state':"LOADED"},
                              {'name':"start_loading",
                               'action':self.a_start_loading,
                               'next_state':"LOADING"}],
                   'messages':[{'name':"HI",
                                'action':self.do_hi,
                                'next_state':"-"}],
                  {'name':"LOADING",
                   'actions':[{'name':"load_ok",
                               'action':None,
                               'next_state':"LOADED"}],
                   'messages':[{'name':"CHUNK",
                                'action':self.do_chunk,
                                'next_state':"LOADING"}]},
                  {'name':"LOADED",
                   'actions':[{'name':"run",
                               'action':self.do_run,
                               'next_state':"-"}],
                   'messages':[{'name':"RUN_OK",
                                'action':,
                                'next_state':"RUNNING"}],
                  {'name':"RUNNING",
                   'actions':[{'name':"stop",
                               'action':self.do_stop,
                               'next_state':"-"}],
                   'messages':[{'name':"STOP_OK",
                                'action':None,
                                'next_state':"LOADED"},
                               {'name':"EVENT",
                                'action':self.do_event,
                                'next_state':"RUNNING"}]},
                  {'name':"ERROR",
                   'actions':[],
                   'messages':[]},
                  {'name':"DONE",
                   'actions':[],
                   'messages':[]},
                  {'name':"*",
                   'actions':[{'name':"error",
                               'action':self.do_error,
                               'next_state':"ERROR"},
                              {'name':"quit",
                               'action':self.do_quit,
                               'next_state':"DONE"}],
                   'messages':[{'name':"QUIT",
                                'action':self.do_quit,
                                'next_state':"DONE"}]}]

        location_descriptor = {'type':zmq.ROUTER,
                               'protocol':"tcp",
                               'address':address,
                               'port':port}
        self.proto = protocol.ProtocolClient(
                                    "app-ctrl",
                                    location,
                                    AppControlProtocol.messages,
                                    states)
        self.user_name = user_name
        self.state = "INIT"
        self.file_name = ""
        self.md5sum = ""
        self.label = ""
        self.f = None
        self.alive = True
        self.chunks_outstanding = 0
        self.say_howdy()

    def a_say_howdy(self, action_args):
        msg_list = ["HOWDY",
                    self.user_name,
                    self.version_major,
                    self.version_minor]
        msg['message'] = msg_list
        self.proto.send(msg)

    def a_start_loading(self, action_args):
        self.file_name = action_args[0]

        # Open the file, compute the md5, then send the first chunk
        # to get the ball rolling.
        if self.f is not None:
            self.f.close()
            self.f = None

        try:
            self.f = open(self.file_name, "rb")
            assert(self.f is not None)
        except:
            Llog.LogError("Cannot open " + self.file_name + " for reading!")
        self.chunks_outstanding = 0
        self.__send_file_chunks()

    def do_hi(self, msg):
        msg_list = msg{'message'}
        version_major = msg_list[0]
        version_minor = msg_list[1]
        state = msg_list[2]
        file_name = msg_list[3]
        md5sum = msg_list[4]

        if version_major != self.version_major:
            self.error("Invalid major version: (" + version_major + ")")

    def run(self, command):
        pass

    def stop(self):
        pass

    def quit(self):
        pass

    def do_hello(self, msg):
        pass

    def do_load(self, msg):
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
        pass

    def do_stopped(self, msg):
        pass

    def do_finished(self, msg):
        pass

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
