import pyrqlite.dbapi2 as db

import logging

from fileManager import createTables, sqlInitRoot


class DBConnect:

    def __init__(self, port):
        try:
            self.connection = db.connect(
                host='localhost',
                port=port
            )

        except db.Error as error:
            logging.error('Failed connection \n {}'.format(error))

    def init_tables(self):
        try:
            createTables()
            self.commit()
        except:
            logging.error('database already exists')

        try:
            sqlInitRoot()
            self.commit()
        except:
            logging.error("root directory already exists")

    def getDB(self):
        return self.connection.cursor()

    def getConnection(self):
        return self.connection

    def __del__(self):
        self.connection.close()
        logging.info('Msql connection is closed')

    def commit(self):
        logging.info("commited changes")
        self.connection.commit()
