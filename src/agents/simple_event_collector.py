
import sys
import time
sys.path.append("../base")
import event_collector
import local_log

def event_cback(event):
    print " ".join([event['type'],
                    event['timestamp'],
                    event['user_name']]) + " ==> " + event['contents']

if __name__ == '__main__':

    local_log.Llog.SetLevel("I")

    user_name = ""
    if len(sys.argv) >= 2:
        # Caller specified username argument
        user_name = sys.argv[1]

    collector = event_collector.EventCollector(["*"], event_cback, user_name)

    while True:
        time.sleep(1)

