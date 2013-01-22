
import sys
import time
sys.path.append("../base")
import event

def event_cback(event):
    print "EVENT: " + str(event)

if __name__ == '__main__':

    collector = event.EventCollector(["*"], event_cback)

    while True:
        time.sleep(1)

