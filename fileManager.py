import base64
import time
from pathlib import PurePath
import random

from DBManager.DBConnect import DBConnect
from _datetime import datetime
import os, stat

cnt = 0
seed = os.urandom(int(160 / 8))


#
# def get_new_id():
#     global cnt
#     cnt = globals().get('cnt')
#     cnt = cnt + 1
#     myid = base64.b64decode(bytes(cnt) + seed)
#     return myid
def uniqueid():
    seed = random.getrandbits(32)
    while True:
        yield seed
        seed += 1


def convertToBinaryData(filename):
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData


def convertToFile(data, filename):
    with open(filename, 'wb') as file:
        file.write(data)


def createBlob(filePath, fileName, input):
    print("inserting blob into file table")

    connection = DBConnect()
    cursor = connection.getDB().cursor()
    filePath = filePath + '/' + fileName

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"

    insert_blob_tuple = (filePath, 0, input)

    cursor.execute(sql_statement, insert_blob_tuple)

    now = int(datetime.now().strftime("%Y%d%M%H"))
    id = next(uniqueid())

    sql_statement = """INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    insert_blob_tuple = (filePath, 'blob', id, os.getuid(), os.getgid(), stat.S_IXUSR, None, None, now,
                         now, now,
                         20, 131072)
    cursor.execute(sql_statement, insert_blob_tuple)

    connection.commit()


def copyFile(filePath, fileName, input):
    print("inserting blob into file table")

    connection = DBConnect()
    cursor = connection.getDB().cursor()

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"

    binFile = convertToBinaryData(input)
    insert_blob_tuple = (filePath + '/' + fileName, 0, binFile)

    cursor.execute(sql_statement, insert_blob_tuple)

    # id = int(datetime.now().strftime("%m%d%H%M%S"))
    id = next(uniqueid())
    filePath = filePath + '/' + fileName
    sql_statement = """INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    insert_blob_tuple = (
        filePath, 'blob', id, os.getuid(), os.getgid(), stat.S_IXUSR, None, None, datetime.now(),
        datetime.now().strftime("%m%d%H%M%S"), datetime.now().strftime("%m%d%H%M%S"),
        os.stat(input).st_size, 131072)
    cursor.execute(sql_statement, insert_blob_tuple)

    connection.commit()


def deleteBLOB(fileName):
    print("deleting blob from file table")

    connection = DBConnect()
    cursor = connection.getDB().cursor()

    sql_statement = """DELETE FROM meta_data 
                        WHERE key= ? 
                        LIMIT 1"""

    cursor.execute(sql_statement, (fileName,))

    sql_statement = """DELETE FROM value_data
                        WHERE key= ? 
                        LIMIT 1"""

    cursor.execute(sql_statement, (fileName,))
    connection.commit()


def retrieveBLOB(fileName):
    print("retrieving file from table")

    connection = DBConnect()
    cursor = connection.getDB().cursor()

    sql_statement1 = "SELECT key,type,uid FROM meta_data WHERE key LIKE ?"
    fileName = '%' + fileName + '%'
    cursor.execute(sql_statement1, (fileName,))

    for key, type, uid in cursor.fetchall():
        fileName = key
        print(key, type)

    sql_statement = "SELECT key,data_block FROM value_data WHERE key like ? "
    cursor.execute(sql_statement, (fileName,))

    for key, data_block in cursor.fetchall():
        print("for loop")
        if (data_block):
            print(data_block)
        else:
            print("file does not exist")


def showFiles():
    connection = DBConnect()
    cursor = connection.getDB().cursor()
    sql_statement = "SELECT key, type FROM meta_data"
    cursor.execute(sql_statement)
    for key, type in cursor.fetchall():
        print(key, type)


def createDir(dirName, path):
    connection = DBConnect()
    cursor = connection.getDB().cursor()

    id = next(uniqueid())
    print(id)
    path = path + '/' + dirName
    sql_statement = """INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    insert_blob_tuple = (path, 'blob', id, os.getuid(), os.getgid(), stat.S_IXUSR, None, None, datetime.now(),
                         datetime.now().strftime("%m%d%H%M%S"), datetime.now().strftime("%m%d%H%M%S"),
                         0, 131072)

    cursor.execute(sql_statement, insert_blob_tuple)
    connection.commit()