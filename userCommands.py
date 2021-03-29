import logging
import os
import stat
import globals
from DBManager.DBConnection import DBConnect
from fileManager import createDir, deleteBLOB, retrieveBLOB, createBlob, copyFile, update_entry_filepath, \
    update_entry_fileName, showFileData, verifySubtree, keyAccessed, updatePermission, getPermission, addDirToPath

from pip._vendor.distlib.compat import raw_input

globals.initialize()
connection = DBConnect()
cursor = connection.getDB()
globals.cursor = cursor


def ls(command):
    logging.debug("called ls method")
    words = command.split(' ')
    if command.__contains__('-l'):
        ls_l()

    elif len(words) == 1:
        logging.debug('retrieving files from table, brief information')
        files = showFileData(globals.current_dir)
        for CurrentFile in files:
            print(CurrentFile.key)
    elif len(words) == 2:

        targetDir = command.split(' ')[1]
        targetDir = addDirToPath(targetDir)
        if not retrieveBLOB(targetDir):
            logging.error("No such file or directory")
            globals.history = globals.history.append({'success': '-', 'failure': command}, ignore_index=True)
            return
        else:
            files = showFileData(targetDir)
            for CurrentFile in files:
                print(CurrentFile.key)
    else:
        logging.error("ls has too many arguments")
        globals.history = globals.history.append({'success': '-', 'failure': command}, ignore_index=True)
        return

    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)


def ls_l():
    logging.debug('retrieving all information for files from table')

    files = showFileData(globals.current_dir)

    for CurrentFile in files:
        print(getPermission(CurrentFile.getFilePath()))
        print(CurrentFile.getFile())


def mkdir(command):
    words = command.split(' ')
    if len(words) != 2:
        logging.error("wrong number of arguments for mkdir")
        return

    logging.info('the name of the directory you want to create is ' + words[1])
    dirname = words[1]
    Directory = retrieveBLOB(globals.current_dir + dirname)
    if Directory is not None:
        logging.error("a file with the same name already exists")
        return

    createDir(dirname)
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)
    connection.commit()


def rm(command):
    words = command.split(' ')
    if len(words) != 2:
        logging.error("wrong number of arguments for rm")
        return
    logging.info('the name of the file you want to delete is ' + words[1])
    fileName = words[1]
    try:
        fileType = retrieveBLOB(fileName).getFileType()
        if fileType == 'dir':
            rmdir(fileName)
        elif fileType == 'blob':
            rmfile(fileName)
        globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)

    except:
        logging.error("could not find file")


def rmdir(fileName):
    if verifySubtree(fileName) == 0:
        deleteBLOB(fileName=fileName)
        connection.commit()
    else:
        logging.error("directory is not empty")


def rmfile(fileName):
    deleteBLOB(fileName=fileName)
    connection.commit()


def cat(command):
    words = command.split(' ')

    if len(words) == 5 and words[3] == '>':
        merge_files(words)
    elif len(words) == 2:
        fileName = words[1]
        file = retrieveBLOB(fileName=fileName)
        if file is not None:
            if file.getFileType() == 'dir':
                logging.error("Is a directory")
                return
            print(file.getFilePath(), file.getFileData())
            keyAccessed(fileName)
            connection.commit()
        else:
            logging.error("file could not be found")
            return
    else:
        logging.error("too many arguments for cat")
        return
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)


def create(command):
    words = command.split(' ')
    if (len(words) != 2):
        logging.error('too many arguments for nano')
        return
    fileName = words[1]

    if retrieveBLOB(globals.current_dir + fileName) is not None:
        logging.error("a file with the same name already exists")
        return

    logging.info('enter file input')
    data = raw_input()
    createBlob(fileName=fileName, input=data)
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)
    connection.commit()


def merge_files(words):
    # cat file1 file2 > file3
    logging.info('merging files ' + words[1] + words[2])

    if retrieveBLOB(globals.current_dir + words[4]) is not None:
        logging.error("a file with the same name already exists")
        return
    try:
        content1 = retrieveBLOB(words[1]).getFileData()
        content2 = retrieveBLOB(words[2]).getFileData()
        data = content1 + content2
        createBlob(fileName=words[4], input=data)
    except:
        logging.error("could not find files")
    connection.commit()

def mv_file(command):
    # mv file /test/dir1
    logging.debug("updating file")
    words = command.split(' ')
    if (len(words) != 3):
        logging.error("too many arguments for mv")
        return
    if words[2].__contains__('/'):
        mv_file_path(words)
    else:
        mv_file_name(words)
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)

def mv_file_path(words):
    update_entry_filepath(words[1], words[2])
    connection.commit()

def mv_file_name(words):
    update_entry_fileName(words[1], words[2])
    connection.commit()

def cd(command):
    if len(command.split(' ')) > 2:
        logging.error("too many arguments for cd function")
        return

    if command.__contains__(' ..'):
        dir = globals.current_dir[:-1]
        dir = dir.split('/')
        dir.pop()
        for string in dir:
            globals.current_dir = '/' + string
        logging.debug("back to " + globals.current_dir)

    else:
        goto_dir = addDirToPath(command.split(' ')[1])
        RetrievedFile = retrieveBLOB(goto_dir)
        if RetrievedFile is None:
            logging.error("this directory does not exist")
            return
        elif RetrievedFile.getFileType() == 'dir':
            globals.current_dir = goto_dir
            logging.info("current directory is " + globals.current_dir)

        else:
            logging.debug(goto_dir + " is not a directory")
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)
    keyAccessed(globals.current_dir)
    connection.commit()


def addFile(command):
    words = command.split(' ')
    if len(words) != 2:
        logging.error('too many arguments for nano')
        return
    fileName = words[1]

    # check if file is in resource if it is call copyFile
    if os.path.exists("../filesystem/resources/" + fileName):
        input = "../filesystem/resources/" + fileName
        fileName = addDirToPath(fileName)
        if retrieveBLOB(fileName):
            logging.error("a file with the same name already exists")
            return
        copyFile(fileName, input)
        logging.debug("added file")
    else:
        logging.error("file is not accessible")
    connection.commit()


def chmod(command):
    words = command.split(' ')
    if len(words) > 3:
        logging.error('too many arguments for chmod')
    try:
        logging.debug(words[2])
        permission = int(words[2])
    except:
        permission = 0
        logging.error("enter permissions as numbers")
    if not retrieveBLOB(words[1]):
        logging.error("could not find file")
        return
    owner = getMode(permission // 100, 1)
    group = getMode(permission // 10 % 10, 2)
    other = getMode(permission % 10, 3)
    print(owner | group | other)
    updatePermission(words[1], owner | group | other)
    connection.commit()

def getMode(i, pos):
    if pos == 1:
        return {
            1: stat.S_IXUSR,
            2: stat.S_IWUSR,
            3: stat.S_IXUSR | stat.S_IWUSR,
            4: stat.S_IRUSR,
            5: stat.S_IRUSR | stat.S_IXUSR,
            6: stat.S_IRUSR | stat.S_IWUSR,
            7: stat.S_IRWXU
        }.get(i, 0)
    elif pos == 2:
        return {
            1: stat.S_IXGRP,
            2: stat.S_IWGRP,
            3: stat.S_IXGRP | stat.S_IWGRP,
            4: stat.S_IRGRP,
            5: stat.S_IRGRP | stat.S_IXGRP,
            6: stat.S_IRGRP | stat.S_IWGRP,
            7: stat.S_IRWXG

        }.get(i, 0)
    elif pos == 3:
        return {
            1: stat.S_IXOTH,
            2: stat.S_IWOTH,
            3: stat.S_IXOTH | stat.S_IWOTH,
            4: stat.S_IROTH,
            5: stat.S_IROTH | stat.S_IXOTH,
            6: stat.S_IROTH | stat.S_IWOTH,
            7: stat.S_IRWXO

        }.get(i, 0)


def getMode(command):
    words = command.split(' ')
    if len(words) > 3:
        logging.error('too many arguments for chmod')
        logging.debug(words[1])
    permmission = getPermission(words[1])
    print(permmission)
