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
BLOCK_SIZE = 128*1024


def uniqueid():
    seed = random.getrandbits(32)
    while True:
        yield seed
        seed += 1


def createTables():
    logging.info("creating tables")
    cursor = globals.cursor

    cursor.execute(
        "CREATE TABLE meta_data(key text,type text,inode integer,uid integer,gid integer,mode integer,acl text,attribute "
        "text,atime integer,mtime integer,ctime integer,size integer,block_size integer,primary key (key),unique(key));")
    cursor.execute(
        "CREATE TABLE value_data (key text,block_no integer,data_block blob,unique(key, block_no));")
    cursor.execute(
        "create index meta_index on meta_data (key); create index value_index on value_data (key, block_no);")


def sqlInitRoot():
    logging.info("in sqlinitroot - adding root directory /")
    cursor = globals.cursor
    sql_statement = "INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    permissions = stat.S_IFDIR | 0o0777
    insert_blob_tuple = (
        '/', 'dir', 1, os.getuid(), os.getgid(), permissions, None, None, time.time(), time.time(), time.time(), 0,
        BLOCK_SIZE)
    print(insert_blob_tuple)
    cursor.execute(sql_statement, insert_blob_tuple)


def createBlob(fileName, input):
    logging.info("in createblob - file " + fileName)

    cursor = globals.cursor

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"
    insert_blob_tuple = (fileName, 0, str(input))
    cursor.execute(sql_statement, insert_blob_tuple)

    id = next(uniqueid())

    sql_statement = "INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    permissions = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    permissions = permissions | 32768
    uid = fuse.fuse_get_context()[0]
    gid = fuse.fuse_get_context()[1]
    insert_blob_tuple = (fileName, 'blob', id, uid, gid, permissions, None, None, time.time(),
                         time.time(), time.time(),
                         len(input), BLOCK_SIZE)
    cursor.execute(sql_statement, insert_blob_tuple)
    return id

def createSymlink(target,source):
    logging.info("in createblob - file " + target)

    cursor = globals.cursor
    id = next(uniqueid())

    sql_statement = "INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    permissions = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    permissions = permissions | 32768
    uid = fuse.fuse_get_context()[0]
    gid = fuse.fuse_get_context()[1]
    insert_blob_tuple = (target, 'sym link', id, uid, gid, permissions, None, None, time.time(),
                         time.time(), time.time(),
                         len(source)+1, BLOCK_SIZE)
    cursor.execute(sql_statement, insert_blob_tuple)

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"
    insert_blob_tuple = (target, 0, str(source))
    cursor.execute(sql_statement, insert_blob_tuple)

    return id


def editFile(fileName, input, size):
    logging.info("in editfile - input " + str(input))
    cursor = globals.cursor
    input = bytes(input)
    nr_blocks = round(size / BLOCK_SIZE) + 1
    logging.info("no of blocks" + str(nr_blocks))
    sql_statement1 = "UPDATE value_data SET data_block =? WHERE key = ? and block_no=?"
    sql_statement2 = "INSERT or Ignore into value_data (key,block_no) VALUES (?,?)"
    for i in range(0, nr_blocks):
        insert_tuple = (fileName, i)
        cursor.execute(sql_statement2, insert_tuple)
        globals.connection.commit()
        data_block = input[i * BLOCK_SIZE:(i + 1) * BLOCK_SIZE]
        update_tuple = (data_block, fileName, i)
        cursor.execute(sql_statement1, update_tuple)
        globals.connection.commit()

    sql_statement = "UPDATE meta_data SET size = ?, mtime=?, atime=? WHERE key = ?"
    update_blob_tuple = (size, time.time(), time.time(), fileName)
    cursor.execute(sql_statement, update_blob_tuple)

    return id


def deleteBLOB(fileName):
    cursor = globals.cursor

    file_swp = '.'+fileName[1:]+'.swp'
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
    sql_statement1 = "SELECT key, type,inode, uid, gid, mode, atime, mtime, ctime,size,block_size from meta_data WHERE key = ?"
    sql_statement2 = "SELECT key, data_block from value_data WHERE key = ?"
    cursor.execute(sql_statement1, (fileName,))

    try:
        record = cursor.fetchone()
        file = File(record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8],
                    record[9], record[10], "")
        keyAccessed(record[0])

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

    sql_statement = "INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    permissions = stat.S_IFDIR | stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    uid = fuse.fuse_get_context()[0]
    gid = fuse.fuse_get_context()[1]
    insert_blob_tuple = (dirName, 'dir', id, uid, gid, permissions, None, None, time.time(),
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
    sql_statement = "SELECT key, type, inode, uid, gid, mode, atime, mtime, ctime,size,block_size FROM meta_data WHERE key like ?"
    if target_dir != '/':
        target_dir = target_dir + "/%"
    else:
        target_dir = target_dir + "%"

    cursor.execute(sql_statement, (target_dir,))
    files = []
    for key, type, inode, uid, gid, mode, atime, mtime, ctime, size, block_size in cursor.fetchall():
        size = len(target_dir.split('/'))
        if len(key.split('/')) == size:
            time = datetime.datetime.fromtimestamp(ctime).strftime("%d %B %I:%M")
            file = File(key, type, inode, uid, gid, mode, time, mtime, ctime, size, block_size, " ")
            files.append(file)
    return files


def keyAccessed(fileName):
    cursor = globals.cursor

    sql_statement = "UPDATE meta_data SET atime = ? WHERE key = ?"
    update_blob_tuple = (time.time(), fileName)
    cursor.execute(sql_statement, update_blob_tuple)


def verifySubtree(fileName):
    logging.info("in verifySubtree - filename= " + fileName)
    cursor = globals.cursor
    fileName += '/%'
    sql_statement1 = "SELECT key, type, uid, gid,mode, ctime FROM meta_data WHERE key LIKE ?"
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

    sql_statement = "UPDATE meta_data SET uid = ?,gid = ? WHERE key = ?"
    update_blob_tuple = (uid, gid, fileName)
    cursor.execute(sql_statement, update_blob_tuple)


def updateTime(path, atime, mtime):
    cursor = globals.cursor
    sql_statement = "UPDATE meta_data SET atime = ?, mtime = ? WHERE key = ?"
    update_blob_tuple = (atime, mtime, path)
    cursor.execute(sql_statement, update_blob_tuple)


def getPermission(fileName):
    try:
        file = retrieveBLOB(fileName)
        permission = file.getFileMode()
        uid = file.getFileUid()
        gid = file.getFileGid()
        keyAccessed(fileName)
    except:
        permission = -1
        uid = -1
        gid = -1
        logging.error("in getPermission - could not find file")
    return permission, uid, gid
