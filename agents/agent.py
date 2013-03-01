
import sys
from apphost.base import system, vitals


class Agent(object):

    agent_crash = vitals.VitalStatisticError("agent_crash", "Crashed")

    def __init__(self, user_name, application_name, module_name):
        self.user_name = user_name
        self.application_name = application_name
        self.module_name = module_name
        system.System.Init(user_name, application_name, module_name)

    def run(self, obj_factory):
        while True:
            try:
                obj = obj_factory(user_name, application_name)
                while obj.is_running() is True:
                    time.sleep(1)
            except KeyboardInterrupt:
                break
            except:
                self.agent_crash += 1
