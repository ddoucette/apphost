
import types


class AppControlProtocol(object):
    """
        AppControlProtocol:

         client --->  HOWDY <username><major><minor>   ---> server
             The server can be either empty, as in just started up,
             or loaded, with a valid application file or
             running, with the specified application file and md5.
             state is either READY,LOADED,RUNNING
         client <---  HI <major,minor,state,file_name,md5,label> <--- server
                         or
         client <---  ERROR <reason>          <--- server

         client --->  LOAD <file_name,md5,label> ---> server
         client <---  LOAD_READY <file_name,md5,label> <--- server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client <---  CHUNK_OK         <--- server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last>  ---> server
         client --->  CHUNK <is_last=true>  ---> server
         client <---  LOAD_OK <file_name,md5,label> <--- server
             All bytes have been received.  The server now 
             acknowledges the presense of the original file.

         client --->  RUN <command>       ---> server
         client <---  RUN_OK              <--- server
         client --->  STOP                ---> server
         client <---  STOP_OK             <--- server
         client --->  RUN <command>       ---> server
         client <---  RUN_OK              <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
         client <---  EVENT               <--- server
             If the application stops on its own, the client receives
             the FINISHED message.
         client <---  FINISHED <return code>  <--- server
         ...
             If the client sends a bad message...
         client --->  LAOD                ---> server
         client <---  ERROR <reason/description> <--- server
    """
    messages = {'HOWDY': [{'name':'user name', \
                            'type':types.StringType}, \
                           {'name':'major version', \
                            'type':types.IntType}, \
                           {'name':'minor version', \
                            'type':types.IntType}], \
                'HI': [{'name':'major version', \
                           'type':types.IntType}, \
                          {'name':'minor version', \
                           'type':types.IntType}, \
                          {'name':'state', \
                           'type':types.StringType}, \
                          {'name':'file_name', \
                           'type':types.StringType}, \
                          {'name':'md5sum', \
                           'type':types.StringType}, \
                          {'name':'label', \
                           'type':types.StringType}], \
                 'LOAD': [{'name':'file_name', \
                             'type':types.StringType}, \
                            {'name':'md5sum', \
                             'type':types.StringType}, \
                            {'name':'label', \
                             'type':types.StringType}], \
                 'LOAD_OK': [{'name':'file_name', \
                                'type':types.StringType}, \
                               {'name':'md5sum', \
                                'type':types.StringType}, \
                               {'name':'label', \
                                'type':types.StringType}], \
                 'LOAD_READY': [{'name':'file_name', \
                                   'type':types.StringType}, \
                                  {'name':'md5sum', \
                                   'type':types.StringType}, \
                                  {'name':'label', \
                                   'type':types.StringType}], \
                 'CHUNK': [{'name':'is last', \
                              'type':types.BooleanType}, \
                             {'name':'data block', \
                              'type':types.StringType}], \
                 'CHUNK_OK': [], \
                 'RUN': [{'name':'command', \
                            'type':types.StringType}], \
                 'RUN_OK': [], \
                 'STOP': [], \
                 'STOP_OK': [], \
                 'FINISHED': [{'name':'error code', \
                                 'type':types.IntType}], \
                 'EVENT': [{'name':'event type', \
                              'type':types.StringType},
                             {'name':'event name', \
                              'type':types.StringType}, \
                             {'name':'timestamp', \
                              'type':types.StringType}, \
                             {'name':'event data type', \
                              'type':types.StringType}, \
                             {'name':'event data', \
                              'type':types.StringType}], \
                 'ERROR': [{'name':'message', \
                              'type':types.StringType}], \
                 'QUIT': []}
