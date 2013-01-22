
import event
import time

def event_cback(event):
    print "EVENT: " + str(event)

if __name__ == '__main__':

    collector = event.EventCollector(event.EventSource.event_types, event_cback)

    while True:
        time.sleep(1)

