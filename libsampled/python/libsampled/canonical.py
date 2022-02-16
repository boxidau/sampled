import socket
import os

from functools import lru_cache


# functools cache is only available in py >= 3.9
# use this shim for greater compatibility
def cache(user_function, /):
    return lru_cache(maxsize=None)(user_function)


@cache
def hostname() -> str:
    return socket.gethostname()


@cache
def kernel() -> str:
    uname = os.uname()
    return f"{uname.sysname} {uname.release}"


@cache
def sysname() -> str:
    return os.uname().sysname