import socket
import os

from functools import lru_cache


@lru_cache(maxsize=None)
def hostname() -> str:
    return socket.gethostname()


@lru_cache(maxsize=None)
def kernel() -> str:
    uname = os.uname()
    return f"{uname.sysname} {uname.release}"


@lru_cache(maxsize=None)
def sysname() -> str:
    return os.uname().sysname
