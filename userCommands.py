import logging

import globals
from fileManager import createDir, deleteBLOB, retrieveBLOB, createBlob, copyFile, update_entry_filepath, \
    update_entry_fileName, showFileData

from pip._vendor.distlib.compat import raw_input

globals.initialize()


def ls(command):
    logging.debug("called ls method")
    if command.__contains__('al'):
        ls_a()
    elif command == 'ls':
        logging.debug('retrieving files from table, brief information')
        files = showFileData(globals.current_dir)
        for CurrentFile in files:
            print(CurrentFile.key)
    else:
        logging.error("ls does not have arguments")
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)


def ls_a():
    logging.debug('retrieving all information for files from table')
    files = showFileData(globals.current_dir)

    for CurrentFile in files:
        print(CurrentFile.getFile())


def mkdir(command):
    words = command.split(' ')
    if len(words) != 2:
        logging.error("wrong number of arguments for mkdir")
        return

    if globals.current_dir == '/':
        path = globals.current_dir
    else:
        path = globals.current_dir + '/'

    logging.info('the name of the directory you want to create is ' + words[1])
    dirname = words[1]

    if retrieveBLOB(path + dirname) is not None:
        logging.error("a file with the same name already exists")
        return

    createDir(globals.current_dir, dirname)
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)


def rm(command):
    words = command.split(' ')
    if len(words) != 2:
        logging.error("wrong number of arguments for rm")
        return
    logging.info('the name of the file you want to delete is ' + words[1])
    fileName = words[1]
    deleteBLOB(fileName=fileName, current_dir=globals.current_dir)
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)


def cat(command):
    words = command.split(' ')

    if len(words) == 3:
        merge_files(words)
    elif len(words) == 2:
        fileName = words[1]
        file = retrieveBLOB(fileName=fileName)
        if file is not None:
            print(file.getFilePath(), file.getFileData())
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
    if globals.current_dir == '/':
        path = globals.current_dir
    else:
        path = globals.current_dir + '/'

    if retrieveBLOB(path + fileName) is not None:
        logging.error("a file with the same name already exists")
        return

    logging.info('enter file input')
    data = raw_input()
    createBlob(filePath=path, fileName=fileName, input=data)
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)


def merge_files(words):
    logging.info('merging files ' + words[2] + words[3])
    if globals.current_dir == '/':
        path = globals.current_dir
    else:
        path = globals.current_dir + '/'

    if retrieveBLOB(path + words[1]) is not None:
        logging.error("a file with the same name already exists")
        return

    content1 = retrieveBLOB(words[2]).getFileData()
    content2 = retrieveBLOB(words[3]).getFileData()
    data = content1 + content2
    copyFile(filePath=globals.current_dir, fileName=words[1], input=data)


def mv_file(command):
    logging.debug("updating file")
    words = command.split(' ')
    if (len(words) != 3):
        logging.error("too many arguments for mv")
        return
    if command.__contains__('/'):
        mv_file_path(words)
    else:
        mv_file_name(words)
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)


def mv_file_path(words):
    update_entry_filepath(words[1], words[2])


def mv_file_name(words):
    update_entry_fileName(words[1], words[2])

def cd(command):
    # logging.debug(command.split(' ')[1])
    if len(command.split(' ')) > 2:
        logging.error("too many arguments for cd function")
        return

    if command.__contains__(' . '):
        print(globals.current_dir)
    if command.__contains__(' ..'):
        dir = globals.current_dir.split('/')
        dir.pop()
        for string in dir:
            globals.current_dir = "/" + string
            logging.debug("back to " + globals.current_dir)

    if globals.current_dir == '/':
        goto_dir = globals.current_dir + command.split(' ')[1]
    else:
        goto_dir = globals.current_dir + '/' + command.split(' ')[1]

    RetrievedFile = retrieveBLOB(goto_dir)
    if RetrievedFile is None:
        logging.error("this directory does not exist")
        return
    elif RetrievedFile.type == 'dir':
        globals.current_dir = goto_dir
        logging.info("current directory is " + globals.current_dir)
    else:
        logging.debug(goto_dir + " is not a directory")
    globals.history = globals.history.append({'success': command, 'failure': '-'}, ignore_index=True)
