import datetime
import time

import os
import stat
import random
import logging
import globals

from File import File

seed = os.urandom(int(160 / 8))


def uniqueid():
    seed = random.getrandbits(32)
    while True:
        yield seed
        seed += 1


def convertToBinaryData(filename):
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData


def createBlob(fileName, input):
    logging.debug('inserting blob into file table')
    logging.info('created file')

    cursor = globals.cursor
    filePath = fullPath(fileName)

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"
    insert_blob_tuple = (filePath, 0, input)
    cursor.execute(sql_statement, insert_blob_tuple)

    id = next(uniqueid())

    sql_statement = "INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    permissions = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    insert_blob_tuple = (filePath, 'blob', id, os.getuid(), os.getgid(), permissions, None, None, time.time(),
                         time.time(), time.time(),
                         20, 131072)
    cursor.execute(sql_statement, insert_blob_tuple)


def copyFile(fileName, input):
    logging.debug('inserting blob into file table')

    cursor = globals.cursor

    sql_statement = "INSERT INTO value_data(key, block_no, data_block) VALUES(?,?,?)"

    binFile = convertToBinaryData(input)
    insert_blob_tuple = (fileName, 0, binFile)
    cursor.execute(sql_statement, insert_blob_tuple)

    id = next(uniqueid())
    logging.debug('id for file ' + fileName + 'is ' + str(id))

    sql_statement = "INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    permissions = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    insert_blob_tuple = (
        fileName, 'blob', id, os.getuid(), os.getgid(), permissions, None, None, time.time(),
        time.time(), time.time(),
        40, 131072)
    cursor.execute(sql_statement, insert_blob_tuple)


def deleteBLOB(fileName):
    cursor = globals.cursor

    fileName = getPathForSubtree(fileName)

    sql_statement = 'DELETE FROM meta_data WHERE key = ?'
    cursor.execute(sql_statement, (fileName,))

    sql_statement = 'DELETE FROM value_data WHERE key = ?'
    logging.info('deleted file ' + fileName)
    cursor.execute(sql_statement, (fileName,))


def retrieveBLOB(fileName):
    cursor = globals.cursor

    logging.debug('searching file with name ' + fileName)
    sql_statement1 = "SELECT md.key, type, uid, gid,mode, ctime, data_block FROM meta_data md left join value_data vd on md.key = vd.key WHERE md.key = ?"
    cursor.execute(sql_statement1, (fileName,))
    try:
        record = cursor.fetchone()
        RetrievedFile = File(record[0], record[1], record[2], record[3], record[4], record[5], record[6])
        return RetrievedFile
    except:
        logging.error("could not find the file or directory")
        return None

def createDir(dirName):
    logging.debug('inserting dir in table')
    cursor = globals.cursor

    id = next(uniqueid())
    logging.debug('id for file ' + dirName + 'is ' + str(id))

    path = fullPath(dirName)

    sql_statement = "INSERT INTO meta_data (key,type,inode,uid,gid,mode,acl,attribute,atime,mtime,ctime,size,block_size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"
    permissions = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
    insert_blob_tuple = (path, 'dir', id, os.getuid(), os.getgid(), permissions, None, None, time.time(),
                         time.time(), time.time(),
                         0, 131072)

    cursor.execute(sql_statement, insert_blob_tuple)


def update_entry_filepath(filename, new_path):
    logging.debug('updating entry in table')

    cursor = globals.cursor
    filename = getPathForSubtree(filename)
    new_path = fullPath(new_path)

    Directory = retrieveBLOB(new_path)
    if Directory is None:
        logging.error("the location from the new path does not exist")
        return


    try:
        old_path = retrieveBLOB(filename)
        old_path = old_path.getFilePath()
    except:
        logging.error("could not find the file you want to move")
        return

    file = filename.split('/')[-1]
    logging.debug("file name " + file)
    sql_statement = "UPDATE meta_data SET key = ?, mtime=? WHERE key = ?"
    update_blob_tuple = (new_path + '/' + file, time.time(), old_path)
    cursor.execute(sql_statement, update_blob_tuple)
    sql_statement = "UPDATE value_data SET key = ? WHERE key = ?"
    update_blob_tuple = (new_path + '/' + file, old_path)
    cursor.execute(sql_statement, update_blob_tuple)


def update_entry_fileName(filename, new_filename):
    logging.debug('updating entry in table')
    cursor = globals.cursor
    filename = getPathForSubtree(filename)

    file = filename.split('/')[-1]
    try:
        old_file = retrieveBLOB(filename)
        new_path = old_file.getFilePath().replace(file, new_filename)
    except:
        logging.error("could not find the file you want to rename")
        return

    sql_statement = "UPDATE meta_data SET key = ?, mtime=? WHERE key = ?"
    update_blob_tuple = (new_path, time.time(), old_file.getFilePath())
    cursor.execute(sql_statement, update_blob_tuple)

    sql_statement = "UPDATE value_data SET key = ? WHERE key = ?"
    update_blob_tuple = (new_path, old_file.getFilePath())
    cursor.execute(sql_statement, update_blob_tuple)

    if old_file.getFileType() == 'dir':
        sql_statement = "UPDATE meta_data SET key = REPLACE(key, ?,?), mtime=? WHERE key like ?"
        filepath = old_file.getFilePath() +"/%"
        update_blob_tuple = (file,new_filename, time.time(), filepath)
        cursor.execute(sql_statement, update_blob_tuple)

        sql_statement = "UPDATE value_data SET key = REPLACE(key, ?,?) WHERE key like ?"
        filepath = old_file.getFilePath() +"/%"
        update_blob_tuple = (file,new_filename, filepath)
        cursor.execute(sql_statement, update_blob_tuple)


def showFileData(target_dir):
    files = []
    cursor = globals.cursor
    sql_statement0 = "SELECT type from meta_data WHERE key = ?"
    cursor.execute(sql_statement0,(target_dir,))
    type = cursor.fetchone()[0]
    if type != 'dir':
        logging.error("cannot list from a non directory")
        return
    sql_statement = "SELECT md.key, type, uid, gid, mode, ctime FROM meta_data md left join value_data vd on md.key = vd.key WHERE md.key like ?"
    if target_dir != '/':
        target_dir = target_dir + "/%"
    else:
        target_dir = target_dir + "%"

    logging.debug("showing all files from " + target_dir)
    cursor.execute(sql_statement, (target_dir,))
    size = len(target_dir.split('/'))

    for key, type, uid, gid, mode, ctime in cursor.fetchall():
        if len(key.split('/')) == size:
            time = datetime.datetime.fromtimestamp(ctime).strftime("%d %B %I:%M")
            RetrievedFile = File(key, type, uid, gid, mode, time, " ")
            files.append(RetrievedFile)
    return files


def keyAccessed(fileName):
    cursor = globals.cursor

    fileName = getPathForSubtree(fileName)

    logging.debug("accessing file " + fileName)
    sql_statement = "UPDATE meta_data SET atime = ? WHERE key = ?"
    update_blob_tuple = (time.time(), fileName)
    cursor.execute(sql_statement, update_blob_tuple)


def verifySubtree(fileName):
    fileName = getPathForSubtree(fileName)
    cursor = globals.cursor
    fileName += '/%'
    logging.debug('searching file with name ' + fileName)
    sql_statement1 = "SELECT key, type, uid, gid,mode, ctime FROM meta_data WHERE key LIKE ?"
    cursor.execute(sql_statement1, (fileName,))

    nr = cursor.rowcount
    # for key in cursor.fetchall():
    #     nr += 1
    logging.debug("number of files in directory " + str(nr))
    return nr

def getPathForSubtree(fileName):
    """
    Return full path of given file, adding the current directory's path.

    If the path does not exist, returns the filepath it was given.
    Adds slash at the beginning, removes the one at the end for consistency (it's also the way paths are stored in db)
    """

    if fileName[0] != '/':
        fileName = '/' + fileName
    if not fileName.__contains__(globals.current_dir):
        if globals.current_dir[-1] == '/' or fileName[0] == '/':
            file = globals.current_dir + fileName
        else:
            file = globals.current_dir + '/' + fileName

        if retrieveBLOB(file) is not None:
            fileName = file

    if len(fileName)!=1 and fileName[-1] == '/':
        fileName = fileName[:-1]
    return fileName

def fullPath(fileName):
    if fileName[0] != '/':
        fileName = '/' + fileName
    if not fileName.__contains__(globals.current_dir):
        if globals.current_dir[-1] == '/' or fileName[0] == '/':
            fileName = globals.current_dir + fileName
        else:
            fileName = globals.current_dir + '/' + fileName
    logging.debug("path of the new file: " + fileName)
    return fileName

def updatePermission(fileName, permission):
    cursor = globals.cursor
    getPathForSubtree(fileName)

    logging.debug("updating mode for " + fileName + " " + str(permission))
    sql_statement = "UPDATE meta_data SET mode = ? WHERE key = ?"
    update_blob_tuple = (permission, fileName)
    cursor.execute(sql_statement, update_blob_tuple)


def getPermission(fileName):
    getPathForSubtree(fileName)
    try:
        mode = retrieveBLOB(fileName).getFileMode()

    except:
        mode = 0
        logging.error("could not find file")

    bits = "{0:b}".format(mode)
    bits = bits[-9:]
    permision = ""
    for i in range(0, len(bits)):
        if bits[i] == '1':
            if i % 3 == 0:
                permision += 'r'
            elif i % 3 == 1:
                permision += 'w'
            elif i % 3 == 2:
                permision += 'x'
        else:
            permision += '-'
    return permision
