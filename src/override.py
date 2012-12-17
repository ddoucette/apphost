"""
    Definition of the common decorator for method override in an
    inheriting class.
"""


def overrides(interface_class):
    def overrider(method):
        assert(method.__name__ in dir(interface_class))
        return method
    return overrider
