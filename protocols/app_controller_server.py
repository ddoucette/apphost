
from apphost.base import log, protocol, zhelpers
from apphost.protocols import app_controller_protocol
import zmq
import time


class AppControlServer(log.Logger):

    version_major = 1
    version_minor = 0

    def __init__(self, user_name, event_cback=None):
        log.Logger.__init__(self)

        states = [{'name':"READY",
                   'actions':[],
                   'messages':[{'name':"LOAD",
                                'action':self.m_load,
                                'next_state':"LOADING"}]},
                  {'name':"LOADING",
                   'actions':[{'name':"load_complete",
                               'action':self.a_load_complete,
                               'next_state':"LOADED"}],
                   'timeout':{'duration':60,
                               'action':self.t_timeout,
                               'next_state':"READY"},
                   'messages':[{'name':"CHUNK",
                                'action':self.m_chunk,
                                'next_state':"LOADING"}]},
                  {'name':"LOADED",
                   'actions':[],
                   'messages':[{'name':"RUN",
                                'action':self.m_run,
                                'next_state':"RUNNING"}]},
                  {'name':"RUNNING",
                   'actions':[{'name':"finished",
                               'action':self.a_finished,
                               'next_state':"LOADED"},
                              {'name':"event",
                               'action':self.a_event,
                               'next_state':"RUNNING"}],
                   'messages':[{'name':"STOP",
                                'action':self.m_stop,
                                'next_state':"LOADED"}]},
                  {'name':"*",
                   'actions':[{'name':"error",
                               'action':self.a_error,
                               'next_state':"-"},
                              {'name':"quit",
                               'action':self.a_quit,
                               'next_state':"-"}],
                   'messages':[{'name':"QUIT",
                                'action':self.m_quit,
                                'next_state':"-"},
                               {'name':"HOWDY",
                                'action':self.m_howdy,
                                'next_state':"-"}]}]
        location = {'type':zmq.ROUTER,
                    'protocol':"tcp",
                    'bind_address':"*",
                    'port_range':[8100,8500]}

        self.proto = protocol.ProtocolServer(
                        "app-ctrl",
                        location,
                        app_controller_protocol.AppControlProtocol.messages,
                        states,
                        self.__state_cback)
        self.user_name = user_name
        self.event_cback = event_cback
        self.file_name = "-"
        self.md5sum = "-"
        self.label = "-"
        self.error_code = 0
        self.f = None
        self.alive = True
        self.client_version_minor = 0

    # is_alive() is polled by the agent which is managing the life-cycle
    # of this server instance.
    def is_alive(self):
        return self.alive

    def finished(self, error_code):
        self.proto.action("finished", [error_code])

    def event(self, event_name, timestamp, event_data_type, event_data):
        self.proto.action("event", [event_name,
                                    timestamp,
                                    event_data_type,
                                    event_data)

    def close(self):
        self.__close_file()
        self.proto.close()
        self.alive = False

    def __state_cback(self, state_name):
        pass

    def m_howdy(self, msg):

        # Verify the username and protocol version information.
        # The username is particularily important as a client may
        # be connecting to this port in error (i.e. for another user).
        msg_list = msg['message']
        user_name = msg_list[1]
        version_major = msg_list[2]
        version_minor = msg_list[3]

        if self.user_name != user_name:
            self.__send_error("Invalid user name specified!")
            return

        if self.version_major != version_major:
            self.__send_error("Invalid major version!  Version ("
                            + self.version_major + ") supported!")
            return

        self.client_version_minor = version_minor

        state = self.proto.get_state()
        msg_list = ["HI",
                    self.version_major,
                    self.version_minor,
                    state,
                    self.file_name,
                    self.md5sum,
                    self.label]
        self.proto.send({'message': msg_list})

    def t_timeout(self, state_name):
        self.log_info("Timeout in state: " + state_name)

    def a_finished(self, action_name, action_args):
        self.proto.send({'message':["FINISHED", str(action_args[0])]})

    def a_event(self, action_name, action_args):
        self.proto.send({'message':["EVENT", action_args]})

    def a_quit(self, action_name, action_args):
        self.close()

    def a_error(self, action_name, action_args):
        reason = action_args[0]
        self.proto.send({'message':["ERROR", reason]})

    def m_quit(self, msg):
        # Stop the application, if it is running.
        # Return the error code
        self.proto.send({'message':["FINISHED", str(self.error_code)]})
        # Wait briefly to allow the FINISHED message to be sent.
        time.sleep(3)
        self.log_info("Received QUIT message!  Quitting.")
        self.close()

    def m_load(self, msg):
        self.file_name = msg['message'][1]
        self.md5sum = msg['message'][2]
        self.label = msg['message'][3]

        # Check to see if we already have the file.
        # If the md5sum fails below, or is different, we have
        # our answer.
        md5sum = zhelpers.md5sum(self.file_name)
        if md5sum is not None and md5sum == self.md5sum:
            # We have this file already.  Issue the load_complete action.
            # This will then send the LOAD_OK message.
            self.proto.action("load_complete")
        else:
            # Open the file for writting.  We should soon be
            # receiving chunks of file data for this file.
            if self.__create_file() is True:
                msg = {'message':["LOAD_READY", \
                                  self.file_name, \
                                  self.md5sum, \
                                  self.label]}
                self.proto.send(msg)
            else:
                self.__send_error("Cannot open (" + self.file_name
                                  + ") for writting!")

    def a_load_complete(self, action_name, action_args):
        msg = {'message':["LOAD_OK", self.file_name, self.md5sum, self.label]}
        self.proto.send(msg)
        self.__report_event("LOADED")

    def m_chunk(self, msg):
        is_last = msg['message'][1]
        data_block = msg['message'][2]

        self.__write_chunk(data_block)
        msg_list = ["CHUNK_OK"]
        msg['message'] = msg_list
        self.proto.send(msg)

        if is_last is True:
            # Close the file and check the md5.  It should match
            # the md5 specified at the start of loading by
            # the client.  If not, error out.
            self.__close_file()
            md5sum = zhelpers.md5sum(self.file_name)
            assert(md5sum is not None)
            if md5sum != self.md5sum:
                self.__error("File does not match md5sum specified!")
                return

            # File is done.  Issue the load complete action.
            self.proto.action("load_complete")

    def m_run(self, msg):
        command = msg['message'][1]
        self.error_code = 0

        self.__report_event("RUN", [command])

        msg_list = ["RUN_OK"]
        msg['message'] = msg_list
        self.proto.send(msg)

    def m_stop(self, msg):
        self.__report_event("STOP")
        self.proto.send({'message':["STOP_OK"]})

    def __report_event(self, event_name, event_args=[]):
        if self.event_cback is not None:
            self.event_cback(event_name, event_args)

    def __create_file(self):
        self.__close_file()
        try:
            self.f = open(self.file_name, "w+")
            return True
        except:
            self.log_error("Cannot open " + self.file_name + " for writting!")
            return False

    def __write_chunk(self, data_block):
        assert(self.f is not None)
        self.f.write(data_block)

    def __close_file(self):
        if self.f is not None:
            self.f.close()
            self.f = None

    def __send_error(self, error_message):
        self.proto.send({'message':["ERROR", error_message]})

    def __error(self, error_message):
        self.proto.action("error", [error_message])


def test1():

    user_name = "sysadmin"
    s = AppControlServer(user_name)
    time.sleep(1)
    assert(s.proto.get_state() == "READY")
    s.close()
    print "test1() PASSED"


if __name__ == '__main__':
    test1()
