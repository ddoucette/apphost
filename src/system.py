"""
    Global system instance.
    This instance provides global system information to all
    modules.
"""

class System():

    __user_name = None
    __application_name = None

    @staticmethod
    def get_user_name():
        assert(System.__user_name is not None)
        return System.__user_name

    @staticmethod
    def set_user_name(user_name):
        assert(System.__user_name is None)
        System.__user_name = user_name

    @staticmethod
    def get_application_name():
        assert(System.__application_name is not None)
        return System.__application_name

    @staticmethod
    def set_application_name(application_name):
        assert(System.__application_name is None)
        System.__application_name = application_name
