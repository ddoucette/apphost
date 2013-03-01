
import sys
sys.path.insert(0, "../base")
import system
import vitals

from override import *


class Agent(object):

    agent_crash = vitals.VitalStatisticError("agent_crash", "Crashed")
    def __init__(self, user_name, application_name, module_name):
        self.user_name = user_name
        self.application_name = application_name
        self.module_name = module_name
        system.System.Init(user_name, application_name, module_name)

    def start(self):
        while True:
            try:
                self.run()
            except KeyboardInterrupt:
                break
            except:
                self.agent_crash += 1

    def run(self):
        assert(False)


def test1():

    import time

    class MyTestAgent1(Agent):
        def __init__(self):
            Agent.__init__(self, "sysadm", "testagent1", "agent")

        @overrides(Agent)
        def run(self):
            print "Running"
            time.sleep(5)
            assert(False)

    m = MyTestAgent1()
    m.start()
    print "PASSED"


if __name__ == '__main__':
    test1()
