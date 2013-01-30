
import projpath
import system
import vitals
import subprocess
import os


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
        argstring = " ".join(args)
        self.cmdline = ['java',
                        '-classpath',
                        classpath,
                        mainappname,
                        argstring]


def test1():

    app = JavaAppExec(["target/HelloWorld-1.0-SNAPSHOT.jar",
                       "lib/DukascopyController-1.0-SNAPSHOT.jar"],
                      "com.mycompany.HelloWorld.App",
                      ["user123", "test1"],
                      "../../../app_examples/HelloWorld")
    app.run()

    is_running = app.is_running()
    assert(is_running is True)
    time.sleep(5)

    app.stop()
    time.sleep(1)
    is_running = app.is_running()
    assert(is_running is False)

    print "PASSED"


if __name__ == '__main__':
    import time
    system.System.Init("sysadmin", "myapp", "app_host")
    test1()
