import sqlite3

class DBConnect:

    def __init__(self):
        try:
            self.connection = sqlite3.connect('/home/ioana/PycharmProjects/dbconection/filesys.sqlite')

            print("msql connected")
        except sqlite3.Error as error:
            print("Failed connection \n {}".format(error))

    def getDB(self):
        return self.connection

    def __del__(self):
        self.connection.close()
        print("msql connection is closed")

    def commit(self):
        self.connection.commit()