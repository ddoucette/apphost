import log
import time

def log_message(timestamp, username, appname, level, filename, line, message):
    msg = " ".join([timestamp,
                    username,
                    appname,
                    level,
                    filename,
                    line,
                    message])
    print "LOG: " + msg


if __name__ == '__main__':

    lc = log.LogCollector(message_cback=log_message)
    while True:
        time.sleep(1)

