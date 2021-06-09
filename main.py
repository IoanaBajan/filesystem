import os
import subprocess
import threading
import time
from threading import Thread

import globals

from DBManager.DBConnection import DBConnect
from fuse import FUSE
from sqlfs import SQLFS

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    args = parser.parse_args()
    print(os.getpid())
    globals.initialize()
    port = int(input('enter port'))
    port2 = str(port)
    node = str(input('enter node number'))

    process = Thread(target=subprocess.call, args=(('./rqlite.sh', node, port2),))
    process.start()
    time.sleep(15)
    print(threading.get_ident())
    connection = DBConnect(port)
    cursor = connection.getDB()
    globals.cursor = cursor
    globals.connection = connection.getConnection()
    connection.init_tables()

    FUSE(SQLFS(), args.mount, nothreads=True, foreground=True, allow_other=True)
