import sqlite3
import  logging
class DBConnect:

    def __init__(self):
        try:
            self.connection = sqlite3.connect('/home/ioana/PycharmProjects/dbconection/filesys.sqlite')
            logging.info('Msql connected')

        except sqlite3.Error as error:
            logging.error('Failed connection \n {}'.format(error))

    def getDB(self):
        return self.connection

    def __del__(self):
        self.connection.close()
        logging.info('Msql connection is closed')

    def commit(self):
        self.connection.commit()