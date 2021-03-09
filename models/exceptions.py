
class InvalidTagException(Exception):
    pass

class TagNotFoundException(Exception):
    pass

class DictException(Exception):
    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)

class NotInWarException(DictException):
    pass

class WarLogPrivateException(DictException):
    pass
