
from apphost.base import log, protocol, zhelpers
from apphost.protocols import app_controller_protocol
import zmq
import types


class AppControlClient(log.Logger):
    """
        For a description of the protocol, see app_controller_protocol.py.
        The AppControlClient provides API to load and run a specified
        application.  The class also provides a callback interface to relay
        key state transitions and errors to the user.  Among the events
        relayed to the user via the callback are the events coming directly
        from the running application.
        The event names are enumerated below.
    """
    version_major = 1
    version_minor = 0

    max_chunks_outstanding = 10
    chunksize = 15000

    event_names = ["ERROR",
                   "READY",
                   "LOADED",
                   "RUNNING",
                   "FINISHED",
                   "STOPPED",
                   "EVENT"]

    """
        The prototype for the event_cback is as follows:
        event_cback(self, event_name, event_args=[])

        Some events have additional arguments:
          "ERROR" msg
          "DONE"  error_code
          "EVENT" timestamp, event_type, event_name,
                  event_data_type, event_value

                  Some event types are:
                    'STDOUT','STDERR','USER'
    """
    def __init__(self, user_name, address, port, event_cback):
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
                              {'name':"app_is_ready",
                               'action':None,
                               'next_state':"READY"}],
                   'timeout':{'duration':5,
                              'action':self.t_init_timeout,
                              'next_state':"ERROR"},
                   'messages':[{'name':"HI",
                                'action':self.m_hi,
                                'next_state':"INIT"}]},
                  {'name':"READY",
                   'actions':[{'name':"start_loading",
                               'action':self.a_start_loading,
                               'next_state':"LOADING",
                               'error_state':"ERROR"}],
                   'keepalive':{'duration':5,
                                'next_state':"ERROR"},
                   'messages':[]},
                  {'name':"LOADING",
                   'actions':[{'name':"load_ok",
                               'action':None,
                               'next_state':"LOADED"}],
                   'timeout':{'duration':60,
                               'action':self.t_timeout,
                               'next_state':"ERROR"},
                   'messages':[{'name':"CHUNK_OK",
                                'action':self.m_chunk_ok,
                                'next_state':"LOADING"},
                               {'name':"LOAD_OK",
                                'action':self.m_load_ok,
                                'next_state':"LOADED"},
                               {'name':"LOAD_READY",
                                'action':self.m_start_chunking,
                                'next_state':"-"}]},
                  {'name':"LOADED",
                   'actions':[{'name':"run",
                               'action':self.a_run,
                               'next_state':"-",
                               'error_state':"ERROR"}],
                   'keepalive':{'duration':5,
                                'next_state':"ERROR"},
                   'messages':[{'name':"RUN_OK",
                                'action':None,
                                'next_state':"RUNNING"}]},
                  {'name':"RUNNING",
                   'actions':[{'name':"stop",
                               'action':self.a_stop,
                               'next_state':"-"}],
                   'keepalive':{'duration':5,
                                'next_state':"ERROR"},
                   'messages':[{'name':"STOP_OK",
                                'action':self.m_stop_ok,
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
                   'messages':[{'name':"FINISHED",
                                'action':self.m_finished,
                                'next_state':"DONE"},
                               {'name':"ERROR",
                                'action':self.m_error,
                                'next_state':"ERROR"}]}]
        location = {'type':zmq.ROUTER,
                    'protocol':"tcp",
                    'address':address,
                    'port':port}
        self.proto = protocol.ProtocolClient(
                            "app-ctrl",
                            location,
                            app_controller_protocol.AppControlProtocol.messages,
                            states,
                            self.__state_cback)
        self.user_name = user_name
        self.event_cback = event_cback
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

    def load(self, file_name, label):
        self.proto.action("start_loading", [file_name, label])

    def run(self, command):
        self.proto.action("run", [command])

    def stop(self):
        self.proto.action("stop")

    def quit(self):
        self.proto.action("quit")

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
        else:
            self.proto.action("app_is_ready")

    def t_init_timeout(self, state_name):
        self.error("Timeout waiting for HI message response!")

    def t_timeout(self, state_name):
        self.log_info("Timeout in state: " + state_name)

    def m_chunk_ok(self, msg):
        # Received ACK for a chunk.  Send more chunks...
        self.chunks_outstanding -= 1
        self.__send_file_chunks()

    def m_load_ok(self, msg):
        # File has successfully loaded on the server.  Verify the md5sum
        # and name are correct
        file_name = msg['message'][1]
        md5sum = msg['message'][2]
        label = msg['message'][3]
        if self.file_name != file_name:
            msg = "Invalid file name received in LOAD_OK! (" + file_name + ")"
            self.error(msg)
            return
        if self.md5sum != md5sum:
            msg = "Invalid md5 received in LOAD_OK! (" + md5sum + ")"
            self.error(msg)
            return
        if self.label != label:
            msg = "Invalid label received in LOAD_OK! (" + label + ")"
            self.error(msg)
            return

    def m_start_chunking(self, msg):
        # Open the file and send the first chunk(s) to get the ball rolling.
        if self.f is not None:
            self.f.close()
            self.f = None

        try:
            self.f = open(self.file_name, "rb")
            assert(self.f is not None)
        except:
            self.log_error("Cannot open " + self.file_name + " for reading!")
            return

        self.chunks_outstanding = 0
        self.__send_file_chunks()

    def m_stop_ok(self, msg):
        self.__report_event("STOPPED")

    def m_event(self, msg):
        self.__report_event("EVENT", msg)

    def m_finished(self, msg):
        self.error_code = int(msg['message'][1])
        self.log_info("Application finished (" + str(self.error_code) + ")")
        self.__report_event("FINISHED", [msg['message'][1]])

    def m_error(self, msg):
        error_message = msg['message'][1]
        self.log_error("Server error: (" + error_message + ")")

    def a_run(self, action_name, action_args):
        command = action_args[0]
        msg_list = ["RUN", command]
        msg = {'message': msg_list}
        self.proto.send(msg)
        return True

    def a_stop(self, action_name, action_args):
        msg = {'message': ["STOP"]}
        self.proto.send(msg)
        return True

    def a_say_howdy(self, action_name, action_args):
        msg_list = ["HOWDY",
                    self.user_name,
                    self.version_major,
                    self.version_minor]
        self.proto.send({'message':msg_list})
        return True

    def a_start_loading(self, action_name, action_args):
        file_name = action_args[0]
        label = action_args[1]

        md5sum = zhelpers.md5sum(file_name)
        if md5sum is None:
            self.log_error("Cannot find specified file: " + file_name)
            return False

        self.md5sum = md5sum
        self.file_name = file_name
        self.label = label

        # Send the load message to get the server ready to receive chunks.
        msg_list = ["LOAD",
                    self.file_name,
                    self.md5sum,
                    self.label]
        self.proto.send({'message':msg_list})
        return True

    def a_error(self, action_name, action_args):
        msg = action_args[0]
        self.log_error(msg)
        self.__report_event("ERROR", [msg])
        return True

    def a_quit(self, action_name, action_args):
        self.proto.send({'message':["QUIT"]})
        return True

    def __send_file_chunks(self):
        while self.f is not None and \
              self.chunks_outstanding < self.max_chunks_outstanding:
            chunk = self.f.read(self.chunksize)
            if chunk != "":
                # We have a chunk of data.  Check to see if it is
                # the last chunk of data.  We do this by reading the next
                # byte in the file.  If it is empty, we know we are at
                # the end of the file.
                last_chunk = False
                byte = self.f.read(1)
                if byte == "":
                    # Done.  Flag the last chunk
                    last_chunk = True
                    self.f.close()
                    self.f = None
                else:
                    # There is still more data to read.  Put the
                    # file back to where it was.
                    self.f.seek(-1,1)
                self.proto.send({'message':["CHUNK", int(last_chunk), chunk]})
                self.chunks_outstanding += 1
            else:
                # Strange, we have run out of data to read.  This should
                # have been detected above.
                self.bug("Empty chunk read!")

    def __state_cback(self, state_name):
        if state_name == "READY":
            self.__report_event("READY")

        if state_name == "RUNNING":
            self.__report_event("RUNNING")

        if state_name == "LOADED":
            self.__report_event("LOADED")

    def __report_event(self, event_name, event_args=[]):
        assert(event_name in self.event_names)
        if self.event_cback is not None:
            self.event_cback(self, event_name, event_args)

    def get_state(self):
        return self.proto.get_state()

    def close(self):
        if self.f is not None:
            self.f.close()
            self.f = None
        self.proto.close()
        self.alive = False


def test1():

    user_name = "sysadmin"
    s = app_controller_server.AppControlServer(user_name)
    c = AppControlClient(user_name, "127.0.0.1", s.proto.zsocket.port, None)

    time.sleep(2)
    assert(c.get_state() == "READY")

    c.load("testfile.bin", "testapp1")
    time.sleep(2)
    assert(c.get_state() == "LOADED")

    c.run("myapp -d this -f that")
    time.sleep(2)
    assert(c.get_state() == "RUNNING")

    # Error, another run command in the RUNNING state.
    c.run("myapp -d this -f that")
    time.sleep(2)
    assert(c.get_state() == "RUNNING")

    c.stop()
    time.sleep(2)
    assert(c.get_state() == "LOADED")

    c.quit()
    time.sleep(2)
    assert(c.get_state() == "DONE")

    print "test1() PASSED"


if __name__ == '__main__':
    import app_controller_server
    import time
    test1()
