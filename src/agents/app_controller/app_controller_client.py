
import projpath
import system
import vitals
import protocol
import zmq
import zhelpers
import types
import log
import app_controller_protocol


class AppControlClient(log.Logger):
    """
        For a description of the protocol, see app_controller_protocol.py.
    """
    version_major = 1
    version_minor = 0

    max_chunks_outstanding = 5
    chunksize = 1000

    def __init__(self, user_name, address, port):
        log.Logger.__init__(self)
        states = [{'name':"INIT",
                   'actions':[{'name':"say_howdy",
                               'action':self.a_say_howdy,
                               'next_state':"INIT"},
                              {'name':"app_is_running",
                               'action':None,
                               'next_state':"RUNNING"},
                              {'name':"app_is_loaded",
                               'action':None,
                               'next_state':"LOADED"},
                              {'name':"start_loading",
                               'action':self.a_start_loading,
                               'next_state':"LOADING"}],
                   'timeout':{'duration':5,
                               'action':self.t_timeout,
                               'next_state':"ERROR"},
                   'messages':[{'name':"HI",
                                'action':self.m_hi,
                                'next_state':"-"}]},
                  {'name':"LOADING",
                   'actions':[{'name':"load_ok",
                               'action':None,
                               'next_state':"LOADED"}],
                   'timeout':{'duration':60,
                               'action':self.t_timeout,
                               'next_state':"ERROR"},
                   'messages':[{'name':"CHUNK",
                                'action':self.m_chunk,
                                'next_state':"LOADING"},
                               {'name':"LOAD_OK",
                                'action':self.m_load_ok,
                                'next_state':"LOADED"}]},
                  {'name':"LOADED",
                   'actions':[{'name':"run",
                               'action':self.a_run,
                               'next_state':"-"}],
                   'messages':[{'name':"RUN_OK",
                                'action':None,
                                'next_state':"RUNNING"}]},
                  {'name':"RUNNING",
                   'actions':[{'name':"stop",
                               'action':self.a_stop,
                               'next_state':"-"}],
                   'messages':[{'name':"STOP_OK",
                                'action':None,
                                'next_state':"LOADED"},
                               {'name':"EVENT",
                                'action':self.m_event,
                                'next_state':"RUNNING"}]},
                  {'name':"ERROR",
                   'actions':[],
                   'messages':[]},
                  {'name':"DONE",
                   'actions':[],
                   'messages':[]},
                  {'name':"*",
                   'actions':[{'name':"error",
                               'action':self.a_error,
                               'next_state':"ERROR"},
                              {'name':"quit",
                               'action':self.a_quit,
                               'next_state':"DONE"}],
                   'messages':[{'name':"QUIT",
                                'action':self.m_quit,
                                'next_state':"DONE"}]}]
        location = {'type':zmq.ROUTER,
                    'protocol':"tcp",
                    'address':address,
                    'port':port}
        self.proto = protocol.ProtocolClient(
                            "app-ctrl",
                            location,
                            app_controller_protocol.AppControlProtocol.messages,
                            states)
        self.user_name = user_name
        self.state = "INIT"
        self.file_name = ""
        self.md5sum = ""
        self.label = ""
        self.f = None
        self.alive = True
        self.chunks_outstanding = 0
        self.log_info("Connecting to port: " + str(port))
        self.say_howdy()

    def say_howdy(self):
        self.proto.action("say_howdy")

    def run(self, command):
        pass

    def stop(self):
        pass

    def quit(self):
        pass

    def error(self, msg):
        self.proto.action("error", [msg])

    def m_hi(self, msg):
        msg_list = msg['message']
        version_major = msg_list[1]
        version_minor = msg_list[2]
        state = msg_list[3]
        file_name = msg_list[4]
        md5sum = msg_list[5]
        label = msg_list[6]

        if version_major != self.version_major:
            self.error("Invalid major version: (" + version_major + ")")
            return

        # The server state can be either READY|LOADED|RUNNING if all
        # is well.  It can be other states, if it is in a bad way...
        if state != 'READY' and state != "LOADED" and state != "RUNNING":
            # Server is not happy.  Nothing we can do.
            self.error("Server is in an invalid state! (" + state + ")")
            return

        if state != 'READY':
            # Either LOADED or RUNNING.  Either way, the filename and
            # md5sum should be correct.
            self.file_name = file_name
            self.md5sum = md5sum
            self.label = label

        if state == 'LOADED':
            self.proto.action("app_is_loaded")
        elif state == 'RUNNING':
            self.proto.action("app_is_running")

    def t_timeout(self, state_name):
        self.log_info("Timeout in state: " + state_name)

    def m_chunk(self, msg):
        # Received ACK for a chunk.  Send more chunks...
        self.chunks_outstanding -= 1
        self.__send_file_chunks()

    def m_load_ok(self, msg):
        # File has successfully loaded on the server.  Verify the md5sum
        # and name are correct
        file_name = msg['message'][1]
        md5sum = msg['message'][2]
        if self.file_name != file_name:
            msg = "Invalid file name received in LOAD_OK! (" + file_name + ")"
            self.error(msg)
            return
        if self.md5sum != md5sum:
            msg = "Invalid md5 received in LOAD_OK! (" + md5sum + ")"
            self.error(msg)
            return

    def m_event(self, msg):
        pass

    def m_quit(self, msg):
        self.error_code = int(msg['message'][1])
        self.log_info("Application finished (" + str(self.error_code) + ")")

    def a_run(self, action_name, action_args):
        command = action_args[0]
        msg_list = ["RUN", command]
        msg = {'message': msg_list}
        self.proto.send(msg)

    def a_stop(self, action_name, action_args):
        msg = {'message': ["STOP"]}
        self.proto.send(msg)

    def a_say_howdy(self, action_name, action_args):
        msg_list = ["HOWDY",
                    self.user_name,
                    self.version_major,
                    self.version_minor]
        self.proto.send({'message':msg_list})

    def a_start_loading(self, action_name, action_args):
        self.file_name = action_args[0]

        # Open the file, compute the md5, then send the first chunk(s)
        # to get the ball rolling.
        if self.f is not None:
            self.f.close()
            self.f = None

        try:
            self.f = open(self.file_name, "rb")
            assert(self.f is not None)
        except:
            self.log_error("Cannot open " + self.file_name + " for reading!")
        self.chunks_outstanding = 0
        self.__send_file_chunks()

    def a_error(self, action_name, action_args):
        msg = action_args[0]
        self.log_error(msg)

    def a_quit(self, action_name, action_args):
        self.proto.send({'message':["QUIT"]})

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
                self.Bug("Empty chunk read!")


def test1():

    user_name = "sysadmin"
    s = app_controller_server.AppControlServer(user_name)
    time.sleep(3)
    c = AppControlClient(user_name, "127.0.0.1", s.proto.zsocket.port)

    time.sleep(10)
    assert(c.state == "READY")
    c.load("testfile.bin")
    time.sleep(5)
    assert(c.state == "LOADED")


if __name__ == '__main__':
    import app_controller_server
    import time
    test1()
