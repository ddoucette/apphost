"""
    Global system instance.
    This instance provides global system information to all
    modules.
"""


class System():

    __user_name = ""
    __application_name = ""
    __module_name = ""

    @staticmethod
    def GetUserName():
        assert(System.__user_name != "")
        return System.__user_name

    @staticmethod
    def GetApplicationName():
        assert(System.__application_name != "")
        return System.__application_name

    @staticmethod
    def GetModuleName():
        assert(System.__module_name != "")
        return System.__module_name

    @staticmethod
    def Init(user_name, app_name, module_name):
        System.__user_name = user_name
        System.__application_name = app_name
        System.__module_name = module_name
