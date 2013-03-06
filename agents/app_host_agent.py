
from apphost.protocols import app_controller_server
from apphost.base import app_exec


class AppHostAgent(object):

    def AppHostAgentFactory(user_name):
        return AppHostAgent(user_name)

    def __init__(self, user_name):
        self.user_name = user_name
        self.acs = app_controller_server.AppControlServer(self.user_name,
                                                          self.__acs_event_cback)
        self.app = None

    def is_running(self):
        return self.acs.is_alive()

    def __app_event_cback(self, event_name, event_args=[]):
        if event_name == "STDOUT":
            self.acs.event("STDOUT", event_args)
        if event_name == "STDERR":
            self.acs.event("STDERR", event_args)
        if event_name == "FINISHED":
            self.acs.finished(event_args[0])

    def __acs_event_cback(self, acs, event_name, event_args=[]):
        print "ACS event: " + event_name

        if event_name == "LOADED":
            # Create the app object if it does not
            # already exist.
            assert(acs.file_name != "")
            assert(acs.label != "")
            if self.app is None:
                self.app = app_exec.JavaAppExec(self.user_name,
                                            acs.file_name,
                                            acs.label,
                                            self.__app_event_cback)
        if event_name == "RUN":
            assert(self.app is not None)
            command = event_args[0]
            self.app.run(command)
        if event_name == "STOP":
            assert(self.app is not None)
            self.app.stop()
