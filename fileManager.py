import base64
import datetime
import time

import os
import stat
import random
import logging

import fuse

import globals

from File import File

seed = os.urandom(int(160 / 8))
globals.initialize()
BLOCK_SIZE = 1024


def uniqueid():
    seed = random.getrandbits(16)
    while True:
        yield seed
        seed += 1


def createTables():
    logging.info("creating tables")
    cursor = globals.cursor

    cursor.execute("CREATE TABLE meta_data(key text, type text, id integer, uid integer, gid integer,mode integer,"
                   "atime integer, mtime integer, ctime integer, size integer, block_size integer, primary key (key),"
                   " unique(key));")
    cursor.execute("CREATE TABLE value_data (key text, block_no integer, data_block blob, unique(key, block_no));")
    cursor.execute(
        "create index meta_index on meta_data (key); create index value_index on value_data (key, block_no);")


def sqlInitRoot():
    logging.info("in sqlinitroot - adding root directory /")
    cursor = globals.cursor
    sql_statement = "INSERT INTO meta_data (key, type, id, uid, gid, mode, atime, mtime, ctime, size, block_size) " \
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
    permissions = stat.S_IFDIR | 0o0777
    insert_blob_tuple = ('/', 'dir', 1, os.getuid(), os.getgid(), permissions, time.time(), time.time(), time.time(), 0,
                         BLOCK_SIZE)
    cursor.execute(sql_statement, insert_blob_tuple)

    permissions = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    permissions = permissions | 32768
    insert_blob_tuple = ('/autorun.inf', 'blob', 2, os.getuid(), os.getgid(), permissions, time.time(), time.time(),
                         time.time(), 0, BLOCK_SIZE)
    cursor.execute(sql_statement, insert_blob_tuple)


def createBlob(fileName, input):
    logging.info("in createblob - file " + fileName)

    cursor = globals.cursor

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"
    insert_blob_tuple = (fileName, 0, str(input))
    cursor.execute(sql_statement, insert_blob_tuple)

    id = next(uniqueid())

    sql_statement = "INSERT INTO meta_data (key, type, id, uid, gid, mode, atime, mtime, ctime, size, block_size) " \
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
    permissions = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    permissions = permissions | 32768
    uid = fuse.fuse_get_context()[0]
    gid = fuse.fuse_get_context()[1]
    insert_blob_tuple = (fileName, 'blob', id, uid, gid, permissions, time.time(), time.time(), time.time(), len(input),
                         BLOCK_SIZE)
    cursor.execute(sql_statement, insert_blob_tuple)
    return id


def createSymlink(target, source):
    logging.info("in createsym - target " + target)
    logging.info("in createsym - source " + source)

    cursor = globals.cursor
    id = next(uniqueid())

    source_data = source.encode('ascii')
    data = base64.b64encode(source_data)
    s = data.decode('utf-8')

    sql_statement = "INSERT INTO meta_data (key, type, id, uid, gid, mode, atime, mtime, ctime, size, block_size) " \
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
    permissions = 0o0777
    permissions = permissions | stat.S_IFLNK
    uid = fuse.fuse_get_context()[0]
    gid = fuse.fuse_get_context()[1]
    insert_blob_tuple = (target, 'sym link', id, uid, gid, permissions, time.time(), time.time(), time.time(),
                         len(s) + 1, BLOCK_SIZE)
    cursor.execute(sql_statement, insert_blob_tuple)
    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"
    insert_blob_tuple = (target, 0, s)
    cursor.execute(sql_statement, insert_blob_tuple)


def editFile(fileName, datainput, offset):
    logging.info("in editfile input size " + str(len(datainput)))
    cursor = globals.cursor
    datainput = bytes(datainput)
    sql_statement1 = "UPDATE value_data SET data_block =? WHERE key = ? and block_no=?"
    sql_statement2 = "INSERT or Ignore into value_data (key,block_no, data_block) VALUES (?,?,?)"

    block_number = round(offset / BLOCK_SIZE)
    logging.info("block number" + str(block_number))

    insert_tuple = (fileName, block_number, datainput)
    cursor.execute(sql_statement2, insert_tuple)
    globals.connection.commit()

    update_tuple = (datainput, fileName, block_number)
    cursor.execute(sql_statement1, update_tuple)
    globals.connection.commit()

    sql_statement = "UPDATE meta_data SET size=?, mtime=?, atime=? WHERE key = ?"
    update_blob_tuple = (offset + len(datainput), time.time(), time.time(), fileName)
    cursor.execute(sql_statement, update_blob_tuple)
    globals.connection.commit()

    return id


def setSize(length, path, filesize):
    cursor = globals.cursor

    sql_statement = "UPDATE meta_data SET size = ?, mtime=?, atime=? WHERE key = ?"
    update_blob_tuple = (length, time.time(), time.time(), path)
    cursor.execute(sql_statement, update_blob_tuple)

    if length < filesize:
        block_no = round(length / BLOCK_SIZE) + 1
        sql_statement2 = "DELETE FROM value_data WHERE key = ? and block_no>?"
        delete_tuple = (path, block_no)
        logging.info("deleted blocks higher than " + str(block_no))
        cursor.execute(sql_statement2, delete_tuple)
    globals.connection.commit()


def deleteBLOB(fileName):
    cursor = globals.cursor

    file_swp = '.' + fileName[1:] + '.swp'
    sql_statement = 'DELETE FROM meta_data WHERE key = ?'
    cursor.execute(sql_statement, (fileName,))
    cursor.execute(sql_statement, (file_swp,))

    sql_statement = 'DELETE FROM value_data WHERE key = ?'
    logging.info('deleted file ' + fileName)
    cursor.execute(sql_statement, (fileName,))
    cursor.execute(sql_statement, (file_swp,))


def retrieveBLOB(fileName):
    cursor = globals.cursor

    logging.info('searching file with name ' + fileName)
    sql_statement1 = "SELECT key, type, id, uid, gid, mode, atime, mtime, ctime, size, block_size from meta_data " \
                     "WHERE key = ? "
    sql_statement2 = "SELECT key, data_block from value_data WHERE key = ?"
    cursor.execute(sql_statement1, (fileName,))

    try:
        record = cursor.fetchone()
        file = File(record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8],
                    record[9], record[10], "")
        updateTime(record[0])

    except:
        logging.error("could not find the file or directory")
        return None
    if record[1] != 'dir':
        data = b''
        cursor.execute(sql_statement2, (fileName,))
        for key, data_block in cursor.fetchall():
            try:
                data_block = data_block.encode()
            except(UnicodeDecodeError, AttributeError):
                pass
            data += bytes(data_block)

        file.setData_block(data)
    return file


def createDir(dirName):
    cursor = globals.cursor

    id = next(uniqueid())

    sql_statement = "INSERT INTO meta_data (key, type, id, uid, gid, mode, atime, mtime, ctime, size, block_size) " \
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
    permissions = stat.S_IFDIR | stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    uid = fuse.fuse_get_context()[0]
    gid = fuse.fuse_get_context()[1]
    insert_blob_tuple = (dirName, 'dir', id, uid, gid, permissions, time.time(),
                         time.time(), time.time(),
                         0, BLOCK_SIZE)
    logging.info('in createDir - inserting dir in table ' + dirName)
    cursor.execute(sql_statement, insert_blob_tuple)


def update_entry_fileName(filename, new_filename):
    logging.info('in update_entry_filename - to ' + new_filename)
    cursor = globals.cursor

    try:
        old_file = retrieveBLOB(filename)
    except:
        logging.error("could not find the file to rename " + filename)
        return

    sql_statement = "UPDATE meta_data SET key = ?, mtime=? WHERE key = ?"
    update_blob_tuple = (new_filename, time.time(), old_file.getFilePath())
    cursor.execute(sql_statement, update_blob_tuple)

    sql_statement = "UPDATE value_data SET key = ? WHERE key = ?"
    update_blob_tuple = (new_filename, old_file.getFilePath())
    cursor.execute(sql_statement, update_blob_tuple)

    if old_file.getFileType() == 'dir':
        sql_statement = "UPDATE meta_data SET key = REPLACE(key, ?,?), mtime=? WHERE key like ?"
        filepath = old_file.getFilePath() + "/%"
        update_blob_tuple = (filename, new_filename, time.time(), filepath)
        cursor.execute(sql_statement, update_blob_tuple)

        sql_statement = "UPDATE value_data SET key = REPLACE(key, ?,?) WHERE key like ?"
        filepath = old_file.getFilePath() + "/%"
        update_blob_tuple = (filename, new_filename, filepath)
        cursor.execute(sql_statement, update_blob_tuple)


def showFileData(target_dir):
    """
    Returns a list o filepaths for the directory given as parameter
    It verifies if the path of target_dir point to a directory
    Returns only files and directories which are direct node to the given directory
    """
    cursor = globals.cursor
    logging.info("in showFileData - target dir is " + target_dir)
    sql_statement0 = "SELECT type from meta_data WHERE key = ?"
    cursor.execute(sql_statement0, (target_dir,))
    type = cursor.fetchone()[0]
    if type != 'dir':
        logging.error("cannot list from a non directory")
        return
    sql_statement = "SELECT key, type, id, uid, gid, mode, atime, mtime, ctime, size, block_size FROM meta_data WHERE " \
                    "key like ? "
    if target_dir != '/':
        target_dir = target_dir + "/%"
    else:
        target_dir = target_dir + "%"

    cursor.execute(sql_statement, (target_dir,))
    files = []
    for key, type, id, uid, gid, mode, atime, mtime, ctime, size, block_size in cursor.fetchall():
        size = len(target_dir.split('/'))
        if len(key.split('/')) == size:
            time = datetime.datetime.fromtimestamp(ctime).strftime("%d %B %I:%M")
            file = File(key, type, id, uid, gid, mode, time, mtime, ctime, size, block_size, " ")
            files.append(file)
    return files


def verifySubtree(fileName):
    logging.info("in verifySubtree - filename= " + fileName)
    cursor = globals.cursor
    fileName += '/%'
    sql_statement1 = "SELECT key, type, uid, gid, mode, ctime FROM meta_data WHERE key LIKE ?"
    cursor.execute(sql_statement1, (fileName,))
    nr = cursor.rowcount
    logging.info("number of files in directory " + str(nr))
    return nr


def updatePermission(fileName, permission):
    cursor = globals.cursor

    sql_statement = "UPDATE meta_data SET mode = ? WHERE key = ?"
    update_blob_tuple = (permission, fileName)
    cursor.execute(sql_statement, update_blob_tuple)


def updateUSER(fileName, uid, gid):
    cursor = globals.cursor

    sql_statement = "UPDATE meta_data SET uid = ?, gid = ? WHERE key = ?"
    update_blob_tuple = (uid, gid, fileName)
    cursor.execute(sql_statement, update_blob_tuple)


def updateTime(path, atime=None, mtime=None):
    cursor = globals.cursor
    if atime is None:
        atime = time.time()
    if mtime is None:
        mtime = time.time()
    sql_statement = "UPDATE meta_data SET atime = ?, mtime = ? WHERE key = ?"
    update_blob_tuple = (atime, mtime, path)
    cursor.execute(sql_statement, update_blob_tuple)


def getPermission(fileName):
    try:
        file = retrieveBLOB(fileName)
        permission = file.getFileMode()
        uid = file.getFileUid()
        gid = file.getFileGid()
        updateTime(fileName)
    except:
        permission = -1
        uid = -1
        gid = -1
        logging.error("in getPermission - could not find file")
    return permission, uid, gid

def getBlobId(path):
    cursor = globals.cursor

    logging.info('searching file with name ' + path)
    sql_statement1 = "SELECT type, id from meta_data WHERE key = ? "
    cursor.execute(sql_statement1, (path,))

    try:
        record = cursor.fetchone()
        type = record[0]
        id = record[1]
        return type, id
    except:
        logging.error("Could not find file")
        return None, None

def getBlobSize(path):
    cursor = globals.cursor

    logging.info('searching file with name ' + path)
    sql_statement1 = "SELECT size from meta_data WHERE key = ? "
    cursor.execute(sql_statement1, (path,))

    try:
        record = cursor.fetchone()
        size = record[0]
        return size
    except:
        logging.error("Could not find file")
        return None
