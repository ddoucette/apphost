
import subprocess
import zmq
import threading
import select
import os
import types
from apphost.base import interface, zsocket, log, override


class AppExec(log.Logger):

    """
        AppExec controls the execution of a command.

        The prototype for the event_cback API is:

        event_cback(event_name, event_args=[])
    """
    def __init__(self, user_name, file_name, label, event_cback):
        log.Logger.__init__(self)

        self.user_name = user_name
        self.file_name = file_name
        self.event_cback = event_cback
        self.label = label
        self.cwd = "."
        self.proc = None
        self.child_env = []
        self.return_code = -1
        self.alive = False

        self.thread = threading.Thread(target=self.__thread_entry)
        self.thread.daemon = True

    def __thread_entry(self):
        read_list = [self.proc.stdout, self.proc.stderr]
        while self.alive is True:
            (rdfiles, wrfiles, exfiles) = select.select(read_list,[],[])
            for fd in rdfiles:
                if fd == self.proc.stdout:
                    msg = self.proc.stdout.readline()
                    if msg != "":
                        self.__process_stdout(msg);

                if fd == self.proc.stderr:
                    msg = self.proc.stderr.readline()
                    if msg != "":
                        self.__process_stderr(msg);

            self.proc.poll()
            if self.proc.returncode != None:
                self.return_code = self.proc.returncode
                self.alive = False
                self.log_info("Process terminated ("
                             + str(self.return_code) + ")")
                self.event_cback("FINISHED", [self.return_code])

    def __process_stdout(self, msg):
        self.event_cback("STDOUT", [msg])

    def __process_stderr(self, msg):
        self.event_cback("STDERR", [msg])

    def run(self, cmdline):
        assert(isinstance(cmdline, types.ListType))
        assert(self.proc is None)

        self.log_info("Executing: " + " ".join(cmdline))

        try:
            self.proc = subprocess.Popen(cmdline,
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

    def __init__(self, user_name, jarfile, label, event_cback):
        AppExec.__init__(self, user_name, jarfile, label, event_cback)

        self.classpath = ":".join([jarfile] + self.system_jars)

    @override.overrides(AppExec)
    def run(self, cmd_args):
        assert(isinstance(cmd_args, types.ListType))
        cmdline = ['java', '-classpath', self.classpath] + cmd_args
        AppExec.run(self, cmdline)


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

    def process_app_msgs(self, event_msg):
        self.log_info("RX: " + event_msg)

    def close(self):
        self.interface.close()


def test1():

    class MyClass(log.Logger):
        def __init__(self, user_name, file_name, label, command):
            log.Logger.__init__(self)
            self.user_name = user_name
            self.file_name = file_name
            self.label = label

            self.app = JavaAppExec(self.user_name,
                                            self.file_name,
                                            self.label,
                                            self.__app_event_cback)
            self.app.run(command)

        def __app_event_cback(self, event_name, event_args=[]):
            if event_name == "STDOUT":
                self.log_info("STDOUT:" + event_args[0])
            if event_name == "STDERR":
                self.log_info("STDERR:" + event_args[0])
            if event_name == "FINISHED":
                self.log_info("FINISHED:" + str(event_args[0]))

    user_name = "sysadmin"
    file_name = "HelloWorld-1.0-SNAPSHOT.jar"
    label = "myapp1"
    command = ["com.mycompany.HelloWorld.App", user_name, file_name]
    c = MyClass(user_name, file_name, label, command)
    time.sleep(3)
    print "test1() PASSED"

if __name__ == '__main__':
    import time
    test1()
