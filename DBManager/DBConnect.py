import mysql.connector


class DBConnect:

    def __init__(self):
        try:
            self.connection = mysql.connector.connect(host='localhost',
                                                      database='filesystem',
                                                      user='root',
                                                      password='my-secret-pw')

            print("msql connected")
        except mysql.connector.Error as error:
            print("Failed connection \n {}".format(error))

    def getDB(self):
        return self.connection

    def __del__(self):
        self.connection.close()
        print("msql connection is closed")

    def commit(self):
        self.connection.commit()
