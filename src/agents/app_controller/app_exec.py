
import projpath
import system
import vitals
import subprocess
import zmq
import zsocket
import interface
import threading
import select
import event_source
import event_collector
import os
from local_log import *


class AppExec(object):

    """
        AppExec controls the execution of a command.
    """
    def __init__(self, cwd=None):
        self.user_name = system.System.GetUserName()
        self.application_name = system.System.GetApplicationName()
        self.cmdline = []
        self.cwd = cwd
        self.proc = None
        self.child_env = []
        self.return_value = -1
        self.poller = None
        self.alive = False

        # STDOUT/STDERR messages will be sent out as events
        self.stdout_event = event_source.EventSource(
                                    "stdout/" + self.application_name,
                                    "STDOUT",
                                    self.user_name,
                                    self.application_name)
        self.stderr_event = event_source.EventSource(
                                    "stderr/" + self.application_name,
                                    "STDERR",
                                    self.user_name,
                                    self.application_name)
        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True

    def __thread_entry(self):
        while self.alive is True:
            events = self.poller.poll()
            for fd, event in events:
                if fd == self.proc.stdout.fileno():
                    msg = self.proc.stdout.readline()
                    if msg != "":
                        self.stdout_event.send(msg);

                if fd == self.proc.stderr.fileno():
                    msg = self.proc.stderr.readline()
                    if msg != "":
                        self.stderr_event.send(msg);

            self.proc.poll()
            if self.proc.returncode != None:
                self.return_value = self.proc.returncode
                self.alive = False
                Llog.LogInfo("Process terminated ("
                             + str(self.return_value) + ")")

    def run(self):
        assert(self.proc is None)
        assert(len(self.cmdline) > 0)

        Llog.LogInfo("Executing: " + " ".join(self.cmdline))

        try:
            self.proc = subprocess.Popen(self.cmdline,
                                         stdout=subprocess.PIPE,
                                         #stdout=None,
                                         stderr=subprocess.PIPE,
                                         #stderr=None,
                                         cwd=self.cwd,
                                         #env=self.child_env)
                                         env=None)
        except OSError, ex:
            print os.strerror(ex.errno)
            return

        assert(self.proc is not None)
        self.pid = self.proc.pid
        self.alive = True

        self.poller = select.poll()
        self.poller.register(self.proc.stdout, select.POLLIN)
        self.poller.register(self.proc.stderr, select.POLLIN)

        # Start our thread which monitors stdout/stderr of our app.
        self.thread.start()

    def is_running(self):
        return self.alive

    def kill(self):
        if self.proc is not None:
            self.proc.kill()

    def stop(self):
        if self.proc is not None:
            self.proc.terminate()


class JavaAppExec(AppExec):

    system_jars = ["DukascopyController-1.0-SNAPSHOT.jar"]

    def __init__(self, jarfiles, mainappname, args, cwd=None):
        AppExec.__init__(self, cwd)

        classpath = ":".join(jarfiles + self.system_jars)
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

    def close(self):
        self.interface.close()


class AppFileLoader(object):

    """
        The AppFileLoader class provides an API to verify an application
        file's existence, and receive chunks of application file data
        to build the application file locally.

        To use:

        app_loader = AppFileLoader(file_name, md5-sum)

        if app_loader.exists() is False:
            app_loader.load_file_start()

        ...

        app_loader.file_chunk(bytes)
        app_loader.file_chunk(bytes)
        app_loader.file_chunk(bytes)

        app_loader.load_file_complete()

        if app_loader.exists() is True:
            # file loaded correctly, and the md5 checks out.
        else:
            # bad file load

    """
    def __init__(self, file_name, md5):
        self.file_name = file_name
        self.md5_sum = md5
        self.f = None

    def __del__(self):
        if self.f is not None:
            self.f.close()
            self.f = None;

    def exists(self):
        md5 = self.calc_md5()
        if md5 == self.md5_sum:
            return True
        return False

    def calc_md5(self):
        proc = subprocess.Popen(['md5sum', self.file_name],
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE,
                                         cwd=".",
                                         env=None)
        out_str, err_str = proc.communicate()
        if out_str != "":
            md5, sep, out_str = out_str.partition(" ")
            return md5
        return None

    def load_file_start(self):
        if self.f is not None:
            self.f.close()
            self.f = None

        try:
            self.f = open(self.file_name, 'w+')
        except:
            Llog.LogError("Cannot open file ("
                          + self.file_name + ") for writting!")
            return False
        return True

    def file_chunk(self, file_bytes):
        assert(self.f is not None)
        self.f.write(file_bytes)

    def load_file_complete(self):
        if self.f is not None:
            self.f.close()
            self.f = None;


class AppControllerProtocol(object):

    protocol_signature = ["40a53bappctrl"]
    protocol_cmds = ["HELLO",
                     "LOAD",
                     "CHUNK",
                     "RUN",
                     "STOP",
                     "STOPPED",
                     "DIED"]


class AppControllerProtocolServer(object):
    """
        The AppControllerProtocol is the protocol which is used
        by the user agent to control all aspects of application
        execution, including the loading of the executable images.
        A typical protocol message flow would be:

         user --->  HELLO                   ---> controller
             The server can be either empty, as in just started up,
             or loaded, with a valid application file or
             running, with the specified application file and md5.
         user <---  HELLO <ready> <0>       <--- controller
         user <---  HELLO <loaded> <md5>    <--- controller
         user <---  HELLO <running> <md5>   <--- controller

         user --->  LOAD <filename,md5> ---> controller
         user <---    LOAD unknown      <--- controller
             The controller does not have this file, or the md5 does
             not match.  The user must chunk the file to the
             controller.
         user --->  CHUNK <size,bytes>  ---> controller
         user --->  CHUNK <size,bytes>  ---> controller
         user --->  CHUNK <size,bytes>  ---> controller
         user <---  CHUNK <ok,length>   <--- controller
         user --->  CHUNK <size,bytes>  ---> controller
         user --->  CHUNK <size,bytes>  ---> controller
         user <---  CHUNK <ok,length>   <--- controller
             All bytes have been received.  The controller now 
             acknowledges the presense of the original file.
         user --->  LOAD <filename,md5> ---> controller
         user <---  LOAD <filename,md5> <--- controller
         user --->  RUN <command>       ---> controller
         user <---  RUNNING             <--- controller
         user --->  STOP                ---> controller
         user <---  STOPPED             <--- controller
         user --->  RUN <command>       ---> controller
         user <---  RUNNING             <--- controller
         user <---  EVENT               <--- controller
         user <---  EVENT               <--- controller
         user <---  EVENT               <--- controller
         user <---  EVENT               <--- controller
             If the application stops on its own, the user receives
             the FINISHED message.
         user <---  FINISHED            <--- controller
             If the application crashes, or returns a non-zero value
             to the shell, a DIED message is returned.
         user <---  DIED <return code>  <--- controller
         user --->  STOP                ---> controller
         user <---  STOPPED             <--- controller
         ...
             If the user sends a bad message...
         user --->  LAOD                ---> controller
         user <---  BADMSG              <--- controller
    """
    port_range = [12500,14500]
    protocol_errors = vitals.VStatError(
                                "protocol_errors",
                                "AppController protocol errors")

    def __init__(self, user_name, application_name):
        self.user_name = user_name
        self.application_name = application_name
        self.app_loader = None
        self.app_exec = None

        self.zsocket = zsocket.ZSocketServer(zmq.ROUTER,
                                    "tcp",
                                    "*",
                                    self.port_range,
                                    AppControllerProtocol.protocol_signature)
        self.zsocket.bind()
        self.interface = interface.Interface(self.protocol_msg_cback)
        self.interface.add_socket(self.zsocket)

    def __del__(self):
        self.reset()

    def reset(self):
        if self.app_exec is not None:
            self.app_exec.stop()
            self.app_exec = None
        if self.app_loader is not None:
            self.app_loader.load_file_complete()
            self.app_loader = None

    def protocol_msg_cback(self, msg):
        cmd, sep, msg = msg.partition(" ")
        if cmd not in AppControllerProtocol.protocol_cmds:
            self.protocol_errors++
            self.badmsg()
            return

        if cmd == "HELLO":
            # Start of command sequence.  Reset, just to be sure.
            self.reset()
            self.zsocket.send("HELLO")
        elif cmd == "LOAD":
            file_name, sep, msg = msg.partition(" ")
            if file_name == "":
                self.protocol_errors++
                self.badmsg()
                return
            md5sum, sep, msg = msg.partition(" ")
            if md5sum == "":
                self.protocol_errors++
                self.badmsg()
                return
            # Check to see if we have the file already.  If not,
            # send back the 'unknown' message
            self.app_loader = AppFileLoader(file_name, md5sum)
            if self.app_loader.exists() is True:
                self.app_loader.load_file_complete()
                self.app_loader = None
                self.zsocket.send("LOAD " + file_name + " " + md5sum)
            else:
                # We do not have the specified file.  Prepare to
                # receive file chunks...
                self.app_loader.load_file_start()
                self.zsocket.send("LOAD unknown")
        elif cmd == "CHUNK":
            if self.app_loader is None:
                self.protocol_errors++
                self.badmsg()
                return

            # If we already have the file, we should not be
            # receiving chunks.
            if self.app_loader.exists() is True:
                self.protocol_errors++
                self.badmsg()
                return

            self.app_loader.file_chunk(msg)
            self.zsocket.send("CHUNK ok")
        elif cmd == "RUN":
            if self.app_loader is None:
                self.protocol_errors++
                self.badmsg()
                return

            # We should have the app file now.
            if self.app_loader.exists() is not True:
                self.protocol_errors++
                self.badmsg()
                return

            if self.app_exec is not None:
                self.app_exec.stop()
                self.app_exec = None

            command = msg
            # Start up our event proxy
            self.app_proxy = AppEventProxy(self.user_name,
                                           self.application_name)

            self.app_exec = JavaAppExec([self.app_loader.file_name],
                                        command,
                                        [self.user_name, self.application_name])
            self.app_exec.run()
            time.sleep(1)
            self.zsocket.send("RUNNING")


def test1():

    user_name = "sysadmin"
    app_name = "test1"
    mod_name = "app_exec"

    system.System.Init(user_name, app_name, mod_name)

    # Start up our event proxy
    evt_proxy = AppEventProxy(user_name, app_name)
    # Give the proxy event a bit of time for discovery
    time.sleep(3)

    app = JavaAppExec(["HelloWorld-1.0-SNAPSHOT.jar"]
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

    evt_proxy.close()
    print "PASSED"


def test2():

    app_loader = AppFileLoader("HelloWorld-1.0-SNAPSHOT.jar",
                               "10b461a2f52ec6280ee1ea4bb46b823a")

    md5 = app_loader.calc_md5()
    if md5 is not None:
        print "md5: " + md5
    else:
        print "Cannot calculate md5"

    assert(app_loader.exists() is True)
    print "test2() PASSED"


if __name__ == '__main__':
    import time
    #test1()
    test2()
