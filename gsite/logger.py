import inspect
import logging
import sys
from functools import wraps

from flask import jsonify

mainlogger = logging.getLogger("mainLogger")
LOG_TO_STDOUT = True

### one time initialization
if not mainlogger.handlers:
    mainlogger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s : %(message)s")
    if LOG_TO_STDOUT:
        logsh = logging.StreamHandler(sys.stdout)
        logsh.setLevel(logging.DEBUG)
        logsh.setFormatter(formatter)
        mainlogger.addHandler(logsh)
    import os

    os.makedirs("logs", exist_ok=True)

    logsh = logging.StreamHandler(open("logs/all_log.log", "a"))
    logsh.setLevel(logging.INFO)
    logsh.setFormatter(formatter)
    mainlogger.addHandler(logsh)
    logsh = logging.StreamHandler(open("logs/instance_log.log", "w"))
    logsh.setLevel(logging.INFO)
    logsh.setFormatter(formatter)
    mainlogger.addHandler(logsh)


def __fmt(*args):
    if issubclass(type(args), list) or issubclass(type(args), tuple):
        return " ".join([str(x) for x in args])
    else:
        return args


def printd(*args):
    mainlogger.debug(__fmt(*args))


def printl(*args):
    mainlogger.info(__fmt(*args))


def printw(*args):
    mainlogger.warning(__fmt(*args))


def printe(*args):
    mainlogger.error(__fmt(*args))


def stacktrace(msg, *args, **kwargs):
    try:
        msg = str(msg)
        msg += "(%r, %r)" % (args, kwargs)
        mainlogger.exception(msg)
    except:
        printe("Error formatting stack msg")
        mainlogger.exception(msg)


def strsignature(func, *args, **kwargs):
    try:
        args = inspect.signature(func).bind(*args, **kwargs).arguments
    except:
        pass
    strarg = ", ".join("{!r}".format(*e) for e in args)
    strarg += ", ".join("{!r}".format(*k)
    for k, v in kwargs.items())
    return f"{func.__module__}.{func.__qualname__}({strarg})"


class HtmlException(Exception):
    status_code = 400

    def __init__(self, message, url_redirect=None, attempts=0):
        super().__init__(message)
        self.url_redirect = url_redirect
        self.attempts = attempts
        self.message = message
        self.success = False
        self.status_code = HtmlException.status_code

    def to_dict(self):
        return {k: str(v) for k, v in vars(self).items() if not k.startswith("_")}

    def jsonify(self):
        dat = jsonify(self.to_dict())
        dat.success = self.success
        dat.status_code = self.status_code
        dat.message = self.message
        return dat

class UncaughtException(HtmlException):
    def __init__(self, exception, func, *args, **kwargs):
        """
        The original exception with the parameters used in the function call
        """
        super().__init__("%s" % exception)
        self.exception = exception
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.fargs = strsignature(func, *args, **kwargs)

        self.message = str(self.exception) if not "message" in kwargs else kwargs["message"]

    def __str__(self):
        return "%s('%s'): func=%s, args=%s" % (
            self.__class__.__name__,
            str(self.exception),
            self.func.__name__,
            str(self.fargs),
        )

    def __reduce__(self):
        """
        To make our Exception work with pickle
        append our exception to the beginning of args
        """
        ps = super().__reduce__()
        state = (ps[2]["exception"],) + ps[1]
        return (ps[0], state, ps[2])


def HandleException(blueprint, url_redirect, message="There was a server error, please try again."):
    """
    """

    def the_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if not isinstance(e, HtmlException):
                    e = UncaughtException(e, func, *args, **kwargs).with_traceback(e.__traceback__)
                    e.url_redirect = url_redirect
                    e.message = message
                    raise e from None
                else:
                    raise
            except:
                stacktrace("Error of some sort")

        return wrapper

    return the_decorator


def ExceptionReturn(data={}, print_stack=True):
    def the_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                try:
                    if print_stack:
                        stacktrace(strsignature(func, *args, **kwargs))
                    if isinstance(e, HtmlException):
                        return e.jsonify()
                    dat = jsonify(data)
                    dat.success = False
                    dat.status_code = 400
                    dat.error = e
                    dat.message = str(e)
                    return dat
                except:
                    stacktrace("Error of some sort")

        return wrapper

    return the_decorator
