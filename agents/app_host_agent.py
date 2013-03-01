
from apphost.protocols import app_controller_server


class AppHostAgent(object):

    def AppHostAgentFactory(user_name):
        return AppHostAgent(user_name)

    def __init__(self, user_name):
        self.user_name = user_name
        self.acs = app_controller_server.AppControlServer(self.user_name)

    def is_running(self):
        return self.acs.is_running()
