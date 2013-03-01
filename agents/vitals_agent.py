
import agent
import system
import vitals
from override import *
import time


class VitalEventProcessor(agent.Agent):
        def __init__(self):
            agent.Agent.__init__(self, "sysadmin", "vitals", "vital_agent")
            self.collector = vitals.VitalEventCollector(
                                    ["VITAL"],
                                    self.event_cback)

        def event_cback(self, event):
            print event['vital_type'] + " - " + ":".join(
                                                [event['user_name'],
                                                event['application_name']])
            print "   - " + event['name'] + event['description']

            if event['vital_type'] == "ERROR":
                print "   - value (" + str(event['value']) \
                      + ") change (" + str(event['delta']) + ")"

        @overrides(agent.Agent)
        def run(self):
            time.sleep(1)


if __name__ == '__main__':
    app = VitalEventProcessor()
    app.start()
