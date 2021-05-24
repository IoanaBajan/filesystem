import subprocess
from threading import Thread

import globals

from multiprocessing import Process
from DBManager.DBConnection import DBConnect
from fuse import FUSE
from userCommands import MySQLFS

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mount')
    args = parser.parse_args()

    globals.initialize()
    port = int(input('enter port'))
    port2 = str(port)
    node = str(input('enter node number'))

    # process = Thread(target=subprocess.call, args=(('./rqlite.sh', node,port2),))
    # process.start()
    connection = DBConnect(port)
    cursor = connection.getDB()
    globals.cursor = cursor
    globals.connection = connection.getConnection()
    connection.init_tables()

    FUSE(MySQLFS(), args.mount, nothreads=True,foreground=True, allow_other=True)
    # process.join()

