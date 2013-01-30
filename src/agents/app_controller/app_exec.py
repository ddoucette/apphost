
import projpath
import system
import vitals
import subprocess
import zmq
import zsocket
import interface
import event_source
import event_collector
import os
from local_log import *


class AppExec(object):

    """
        AppExec controls the execution of a command.
    """
    def __init__(self, cwd=None):
        self.cmdline = []
        self.cwd = cwd
        self.user_name = system.System.GetUserName()
        self.application_name = system.System.GetApplicationName()
        self.proc = None
        self.child_env = []
        self.return_value = -1

    def run(self):
        assert(self.proc is None)
        assert(len(self.cmdline) > 0)

        Llog.LogInfo("Executing: " + " ".join(self.cmdline))

        try:
            self.proc = subprocess.Popen(self.cmdline,
                                         #stdout=subprocess.PIPE,
                                         stdout=None,
                                         #stderr=subprocess.PIPE,
                                         stderr=None,
                                         cwd=self.cwd,
                                         #env=self.child_env)
                                         env=None)
        except OSError, ex:
            print os.strerror(ex.errno)
            return

        assert(self.proc is not None)
        self.pid = self.proc.pid

    def is_running(self):
        assert(self.proc is not None)
        retval = self.proc.poll()
        if retval is None:
            return True
        self.return_value = self.proc.returncode
        return False

    def kill(self):
        if self.proc is not None:
            self.proc.kill()

    def stop(self):
        if self.proc is not None:
            self.proc.terminate()


class JavaAppExec(AppExec):

    def __init__(self, jarfiles, mainappname, args, cwd=None):
        AppExec.__init__(self, cwd)

        classpath = ":".join(jarfiles)
        self.cmdline = ['java',
                        '-classpath',
                        classpath,
                        mainappname] + args


class AppEventProxy(object):

    """
        In order to try to limit the ability of the java application
        to mess with things, we funnel all application events through
        a proxy using ipc.  The Java application uses the standard
        EventSource protocol to send events out a PUSH socket.
        We create a PULL socket server here and receive all events.
        Once received, we verify the sanity of the event message and
        re-send it out a real EventSource socket.
    """

    def __init__(self, user_name, application_name):
        self.user_name = user_name
        self.application_name = application_name
        self.events = []

        # Create our EventSource event.  This is the event we will
        # use to send out all valid, received events from the application.
        # At this point, we really dont know what type of events the
        # application will be sending, so we create our first event of
        # type NULL.  By creating this event here, we are hurrying up the
        # process of event source discovery.
        self.__create_event("null", "NULL")
        self.zsocket = zsocket.ZSocketServer(zmq.PULL,
                                             "ipc",
                                             ":".join([user_name,
                                                      application_name]))
        """
        port_range = [6556,6557]
        self.zsocket = zsocket.ZSocketServer(zmq.PULL,
                                             "tcp",
                                             "*", port_range)
        """
        assert(self.zsocket is not None)
        self.zsocket.bind()

        self.interface = interface.Interface(self.process_app_msgs)
        self.interface.add_socket(self.zsocket)

    def __create_event(self, event_name, event_type):
        # We must enforce a limit on the number of events the
        # user's application an create.  This will be based on the
        # SLA the user has.
        max_events = system.System.GetSLA()['max_event_types']

        if len(self.events) >= max_events + 1:
            Llog.LogDebug("Exceeded max_event_types: " + str(max_events))
            return None

        event = event_source.EventSource(event_name,
                                         event_type,
                                         self.user_name,
                                         self.application_name)
        self.events.append(event)
        return event

    def __find_event(self, event_name, event_type):
        for event in self.events:
            if event.event_name == event_name \
                and event.event_type == event_type:
                return event
        return None

    def __get_event(self, event_name, event_type):
        event = self.__find_event(event_name, event_type)
        if event is None:
            event = self.__create_event(event_name, event_type)
        return event

    def process_app_msgs(self, event_msg):
        Llog.LogInfo("RX: " + event_msg)
        app_event = event_collector.EventCollector.event_msg_parse(event_msg)
        if app_event is None:
            Llog.LogError("Could not parse event message!: " + str(event_msg))
            return

        event = self.__get_event(app_event['name'], app_event['type'])
        if event is not None:
            event.send(app_event['contents'])


def test1():

    user_name = "sysadmin"
    app_name = "test1"
    mod_name = "app_exec"

    system.System.Init(user_name, app_name, mod_name)

    # Start up our event proxy
    evt_proxy = AppEventProxy(user_name, app_name)
    # Give the proxy event a bit of time for discovery
    time.sleep(3)

    app = JavaAppExec(["HelloWorld-1.0-SNAPSHOT.jar",
                       "DukascopyController-1.0-SNAPSHOT.jar"],
                      "com.mycompany.HelloWorld.App",
                      [user_name, app_name],
                      ".")
    app.run()

    is_running = app.is_running()
    assert(is_running is True)

    time.sleep(20)

    app.stop()
    time.sleep(1)
    is_running = app.is_running()
    assert(is_running is False)

    print "PASSED"


if __name__ == '__main__':
    import time
    test1()
