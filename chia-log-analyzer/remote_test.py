import os
import socket

from chia.base.ChiaFunction import ChiaFunction, get


@ChiaFunction(resources={"remote_test": 1})
def where_am_i():
    return {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "user": os.getenv("USER"),
    }


def main():
    print("Driver hostname:", socket.gethostname())
    print("Driver PID:", os.getpid())

    ref = where_am_i.chia_remote()

    print("Remote task submitted.")
    print("ObjectRef:", ref)

    result = get(ref)

    print("\nRemote worker result:")
    print(result)


if __name__ == "__main__":
    main()
