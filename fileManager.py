import datetime
import time

from DBManager.DBConnection import DBConnect
import os
import stat
import random
import logging

from File import File

cnt = 0
seed = os.urandom(int(160 / 8))

logging.basicConfig(format='%(asctime)s -%(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
logging.root.setLevel(logging.DEBUG)


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
    logging.debug('inserting blob into file table')
    logging.info('created file')

    connection = DBConnect()
    cursor = connection.getDB().cursor()
    filePath = filePath + fileName

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"

    insert_blob_tuple = (filePath, 0, input)

    cursor.execute(sql_statement, insert_blob_tuple)

    id = next(uniqueid())

    sql_statement = """INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    insert_blob_tuple = (filePath, 'blob', id, os.getuid(), os.getgid(), stat.S_IXUSR, None, None, time.time(),
                         time.time(), time.time(),
                         20, 131072)
    cursor.execute(sql_statement, insert_blob_tuple)

    connection.commit()


def copyFile(filePath, fileName, input):
    logging.debug('inserting blob into file table')
    logging.info('copied file')

    connection = DBConnect()
    cursor = connection.getDB().cursor()

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"

    # binFile = convertToBinaryData(input)
    if filePath == '/':
        filePath = filePath + fileName
    else:
        filePath = filePath + '/' + fileName

    insert_blob_tuple = (filePath, 0, input)
    cursor.execute(sql_statement, insert_blob_tuple)

    id = next(uniqueid())
    logging.debug('id for file ' + fileName + 'is ' + str(id))

    sql_statement = """INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    insert_blob_tuple = (
        filePath, 'blob', id, os.getuid(), os.getgid(), stat.S_IXUSR, None, None, time.time(),
        time.time(), time.time(),
        40, 131072)
    cursor.execute(sql_statement, insert_blob_tuple)
    connection.commit()


def deleteBLOB(fileName, current_dir):
    connection = DBConnect()
    cursor = connection.getDB().cursor()

    if current_dir == '/':
        filepath = current_dir + fileName
    else:
        filepath = current_dir + '/' + fileName

    sql_statement = """DELETE FROM meta_data WHERE key like ? LIMIT 1"""
    cursor.execute(sql_statement, (filepath,))

    sql_statement = """DELETE FROM value_data WHERE key like ? LIMIT 1"""
    logging.info('deleted file ' + filepath)
    cursor.execute(sql_statement, (filepath,))
    connection.commit()


def retrieveBLOB(fileName):
    logging.debug('searching file with name ' + fileName)
    connection = DBConnect()
    cursor = connection.getDB().cursor()

    sql_statement1 = "SELECT md.key, type, uid, gid,mode, ctime, data_block FROM meta_data md left join value_data vd on md.key = vd.key WHERE md.key LIKE ?"
    fileName = '%' + fileName + '%'
    cursor.execute(sql_statement1, (fileName,))

    for key, type, uid, gid, mode, ctime, data_block in cursor.fetchall():
        if (key):
            RetrievedFile = File(key, type, uid, gid, mode, ctime, data_block)
            return RetrievedFile
        else:
            logging.debug("file does not exist")
            return None


def createDir(path, dirName):
    logging.debug('inserting dir in table')
    connection = DBConnect()
    cursor = connection.getDB().cursor()

    id = next(uniqueid())
    logging.debug('id for file ' + dirName + 'is ' + str(id))
    if path == '/':
        path = path + dirName
    else:
        path = path + '/' + dirName

    sql_statement = """INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    insert_blob_tuple = (path, 'dir', id, os.getuid(), os.getgid(), stat.S_IXUSR, None, None, time.time(),
                         time.time(), time.time(),
                         0, 131072)

    cursor.execute(sql_statement, insert_blob_tuple)
    connection.commit()


def update_entry_filepath(filename, new_path):
    logging.debug('updating entry in table')
    connection = DBConnect()
    cursor = connection.getDB().cursor()
    old_path = retrieveBLOB(filename)
    if old_path is not None:
        old_path = old_path.getFilePath()
    else:
        logging.error("could not find the file you want to move")
        return
    sql_statement = """UPDATE meta_data SET key = ? WHERE key like ?"""
    update_blob_tuple = (new_path + '/' + filename, old_path)
    cursor.execute(sql_statement, (update_blob_tuple))
    sql_statement = """UPDATE value_data SET key = ? WHERE key like ?"""
    update_blob_tuple = (new_path + '/' + filename, old_path)
    cursor.execute(sql_statement, (update_blob_tuple))

    connection.commit()


def update_entry_fileName(filename, new_filename):
    logging.debug('updating entry in table')
    connection = DBConnect()
    cursor = connection.getDB().cursor()
    old_path = retrieveBLOB(filename)
    if old_path is not None:
        new_path = old_path.key.replace(filename, new_filename)
    else:
        logging.error("could not find the file you want to rename")
        return

    sql_statement = """UPDATE meta_data SET key = ? WHERE key like ?"""
    update_blob_tuple = (new_path, old_path.key)
    cursor.execute(sql_statement, update_blob_tuple)

    sql_statement = """UPDATE value_data SET key = ? WHERE key like ?"""
    update_blob_tuple = (new_path, old_path.key)
    cursor.execute(sql_statement, update_blob_tuple)

    connection.commit()


def showFileData(current_dir):
    files = []
    connection = DBConnect()
    cursor = connection.getDB().cursor()
    sql_statement = """SELECT md.key, type, uid, gid, mode, ctime, data_block FROM meta_data md left join value_data vd on md.key = vd.key WHERE md.key like ?"""
    if current_dir == '/':
        current_dir = "%" + current_dir + "%"
    else:
        current_dir = "%" + current_dir + "/%"

    logging.debug("showing all files from " + current_dir)
    cursor.execute(sql_statement, (current_dir,))

    for key, type, uid, gid, mode, ctime, data_block in cursor.fetchall():
        time = datetime.datetime.fromtimestamp(ctime).strftime("%d %B %I:%M")
        RetrievedFile = File(key, type, uid, gid, mode, time, data_block)
        files.append(RetrievedFile)
    return files
