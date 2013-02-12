"""
"""
import projpath
import interface
import zmq
import system
import vitals
import zsocket
from local_log import *



class AppControllerProtocol(object):

    protocol_signature = "40a53bappctrl"
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

         client --->  HELLO                   ---> server
             The server can be either empty, as in just started up,
             or loaded, with a valid application file or
             running, with the specified application file and md5.
         client <---  HELLO <ready>           <--- server
         client <---  HELLO <loaded> <filename,md5>    <--- server
         client <---  HELLO <running> <filename,md5>   <--- server

         client --->  LOAD <filename,md5> ---> server
         client <---    LOAD unknown      <--- server
             The server does not have this file, or the md5 does
             not match.  The client must chunk the file to the
             server.
         client --->  CHUNK <size,index>  ---> server
         client --->  CHUNK <size,index>  ---> server
         client --->  CHUNK <size,index>  ---> server
         client <---  CHUNK <ok,indices>   <--- server
         client --->  CHUNK <size,index>  ---> server
         client --->  CHUNK <size,index>  ---> server
         client <---  CHUNK <ok,length,md5> <--- server
             All bytes have been received.  The server now 
             acknowledges the presense of the original file.
         client --->  LOAD <filename,md5> ---> server
         client <---  LOAD <filename,md5> <--- server
         client --->  RUN <command>       ---> server
         client <---  RUNNING             <--- server
         client --->  STOP                ---> server
         client <---  STOPPED             <--- server
         client --->  RUN <command>       ---> server
         client <---  RUNNING             <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
             If the application stops on its own, the client receives
             the FINISHED message.
         client <---  FINISHED            <--- server
             If the application crashes, or returns a non-zero value
             to the shell, a DIED message is returned.
         client <---  DIED <return code>  <--- server
         client --->  STOP                ---> server
         client <---  STOPPED             <--- server
         ...
             If the client sends a bad message...
         client --->  LAOD                ---> server
         client <---  BADMSG              <--- server
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


