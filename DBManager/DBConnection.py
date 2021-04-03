import pyrqlite.dbapi2 as db
import logging


class DBConnect:

    def __init__(self, port):
        try:
            self.connection = db.connect(
                host='localhost',
                port=port,
            )

        except db.Error as error:
            logging.error('Failed connection \n {}'.format(error))

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    'CREATE TABLE meta_data(key text,type text,inode integer,uid integer,gid integer,mode integer,' +
                    'acl text,attribute text,atime integer,mtime integer,ctime integer,size integer,' +
                    'block_size integer,primary key (key),unique(key));')
                cursor.execute(
                    'CREATE TABLE value_data (key text,block_no integer,data_block blob,unique(key, block_no));')
                cursor.execute('create index meta_index on meta_data (key);' +
                               'create index value_index on value_data (key, block_no);')
        except:
            logging.error('database already exists')
        logging.info('Msql connected')

    def getDB(self):
        return self.connection.cursor()

    def __del__(self):
        self.connection.close()
        logging.info('Msql connection is closed')

    def commit(self):
        self.connection.commit()
