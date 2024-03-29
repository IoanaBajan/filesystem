import base64
import errno
import logging
import os
import stat
import time
from errno import ENOENT

import fuse
from fuse import Operations, FuseOSError

import globals
from fileManager import createDir, deleteBLOB, retrieveBLOB, createBlob, \
    update_entry_fileName, showFileData, verifySubtree, updatePermission, getPermission, \
    editFile, updateTime, updateUSER, createSymlink, setSize, BLOCK_SIZE, getBlobId, getBlobSize

globals.initialize()


class SQLFS(Operations):
    def __init__(self):
        for attribute in self.__dict__:
            value = getattr(self, attribute)
            setattr(self, attribute, str(value))
        logging.info("Mounted filesystem")

    def getattr(self, path, fh=None):
        try:
            file = retrieveBLOB(path)
            attr = dict(
                st_mode=file.getFileMode(),
                st_ctime=file.getCtime(),
                st_mtime=file.getMtime(),
                st_atime=file.getAtime(),
                st_uid=file.getFileUid(),
                st_gid=file.getFileGid(),
                st_blksize=file.getBlockSize(),
                st_size=file.getSize())

            if file.getFileType() == 'dir' or file.getFileType() == "sym link":
                attr['st_nlink'] = 1
            else:
                attr['st_nlink'] = 2
            return attr
        except:
            logging.error("in getattr - could not find file " + path)
            raise FuseOSError(ENOENT)

    def access(self, path, amode):
        uid = fuse.fuse_get_context()[0]
        gid = fuse.fuse_get_context()[1]

        sql_mode, sql_uid, sql_gid = getPermission(path)

        logging.info("in access - mode uid gid " + str(amode) + " " + str(uid) + " " + str(gid))
        logging.info("in access - sqlmode sqluid sqlgid " + str(sql_mode) + " " + str(sql_uid) + " " + str(sql_gid))

        if sql_uid == -1 or sql_uid == -1:
            raise FuseOSError(errno.EACCES)

        access = 0
        if uid == 0:
            access = 0

        if sql_uid == uid:
            if ((amode & os.R_OK) and not (stat.S_IRUSR & sql_mode)) or (
                    (amode & os.W_OK) and not (stat.S_IWUSR & sql_mode)) or (
                    (amode & os.X_OK) and not (stat.S_IXUSR & sql_mode)):
                access = -errno.EACCES
        elif sql_gid == gid:
            if ((amode & os.R_OK) and not (stat.S_IRGRP & sql_mode)) or (
                    (amode & os.W_OK) and not (stat.S_IWGRP & sql_mode)) or (
                    (amode & os.X_OK) and not (stat.S_IXGRP & sql_mode)):
                access = -errno.EACCES

        else:
            if ((amode & os.R_OK) and not (stat.S_IROTH & sql_mode)) or (
                    (amode & os.W_OK) and not (stat.S_IWOTH & sql_mode)) or (
                    (amode & os.X_OK) and not (stat.S_IXOTH & sql_mode)):
                access = -errno.EACCES

        return access

    def opendir(self, path):
        logging.info("in opendir - accessing directory" + path)
        if self.access(path, os.R_OK | os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)
        try:
            type,id = getBlobId(path)

            if type == dir:
                globals.current_dir = path
                return id
        except:
            logging.error("in open - could not find directory " + path)
            raise FuseOSError(errno.ENOENT)
        return 0

    def readdir(self, path, fh):
        logging.info("in readdir - called ls ")
        dirents = ['.', '..']
        self.current_dir(path)

        if self.access(globals.current_dir, os.R_OK | os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)
        try:
            files = showFileData(path)
            for CurrentFile in files:
                path = CurrentFile.getFilePath()
                if path != globals.current_dir:
                    dirents.append(path.split('/')[-1])
            for r in dirents:
                yield r
        except:
            raise FuseOSError(errno.EROFS)

    def mkdir(self, path, mode):
        logging.info('in mkdir - the name of the directory you want to create is ' + path)
        self.current_dir(path)

        if self.access(globals.current_dir, os.W_OK | os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)

        type, id = getBlobId(path)
        if id:
            logging.error("in mkdir - a file with the same name already exists")
            raise FuseOSError(errno.EEXIST)

        createDir(path)
        globals.connection.commit()

    def rmdir(self, path):
        self.current_dir(path)
        if self.access(path, os.W_OK | os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)
        if verifySubtree(path) < 1:
            deleteBLOB(fileName=path)
            globals.connection.commit()
        else:
            logging.error("in rmdir - directory is not empty")
            raise FuseOSError(errno.ENOTEMPTY)

    def create(self, path, mode, fi=None):
        logging.info("in create - " + path)
        self.current_dir(path)
        if self.access(globals.current_dir, os.W_OK | os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)
        type,id = getBlobId(path)
        if id:
            logging.error("in create - a file with the same name already exists")
            raise FuseOSError(errno.EEXIST)
        else:
            id = createBlob(fileName=path, input="")

            globals.connection.commit()
            logging.info("in create - file with path " + path + " created ")
            return id

    def unlink(self, path):
        logging.info("in unlink - file with path " + path + " deleted ")

        self.current_dir(path)
        if self.access(path, os.W_OK | os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)
        deleteBLOB(fileName=path)
        globals.connection.commit()

    def open(self, path, flags):
        logging.info("in open - file " + path)
        logging.info(flags)

        if self.access(path, os.R_OK) != 0:
            raise FuseOSError(errno.EACCES)

        if flags & 0x0400:
            self.truncate(path, 0)
        try:
            type, id = getBlobId(path)
            updateTime(path)
            globals.connection.commit()
            if type == 'blob':
                return id

        except:
            logging.error("in open - could not find file " + path)
            raise FuseOSError(errno.ENOENT)

    def read(self, path, size, offset, fh):
        logging.error("in read - file " + path)
        self.current_dir(path)

        if self.access(globals.current_dir, os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)
        if self.access(path, os.R_OK | os.F_OK) != 0:
            raise FuseOSError(errno.EACCES)

        try:
            file = retrieveBLOB(path)
            id, name, sz, filedata = file.getFile()
            updateTime(path)
            if not filedata:
                return bytes()
            length = min(sz, offset + size)
            return filedata
        except:
            logging.error("in read - could not find file " + path)
            raise FuseOSError(errno.ENOENT)

    def write(self, path, data, offset, fh):
        logging.info("in write - file " + path + " " + str(len(data)))
        self.current_dir(path)

        if self.access(path, os.W_OK) != 0:
            raise FuseOSError(errno.EACCES)
        if self.access(globals.current_dir, os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)

        file = retrieveBLOB(path)
        if file is not None:
            id, name, size, filedata = file.getFile()

            if not filedata:
                filedata = bytes()

            sql_data_block = filedata[offset:BLOCK_SIZE]

            if sql_data_block != data or len(data) < len(sql_data_block):
                editFile(path, data, offset)
            globals.connection.commit()

        else:
            logging.error("in write - could not find file")
            raise FuseOSError(errno.ENOENT)
        return len(data)

    def truncate(self, path, length, fh=None):
        logging.info("in truncate - edit file " + path)
        self.current_dir(path)

        if self.access(path, os.W_OK) != 0:
            raise FuseOSError(errno.EACCES)
        if self.access(globals.current_dir, os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)

        try:
            size = getBlobSize(path)
            setSize(length, path, size)
            globals.connection.commit()

        except:
            logging.error("in truncate - could not find file")

    def rename(self, old, new):
        moveto = self.current_dir(new)
        if self.access(old, os.W_OK | os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)
        if self.access(moveto, os.W_OK | os.X_OK) != 0:
            raise FuseOSError(errno.EACCES)
        logging.info("in rename - new file " + new)
        update_entry_fileName(old, new)
        globals.connection.commit()

    def chmod(self, path, mode):
        logging.info('in chmod ' + str(mode))
        uid = fuse.fuse_get_context()[0]
        gid = fuse.fuse_get_context()[1]
        permission, sql_uid, sql_gid = getPermission(path)
        try:
            if sql_uid == uid and sql_gid == gid:
                updatePermission(path, mode)
                updateTime(path)
                globals.connection.commit()
            else:
                logging.info("Operation not permitted")
                raise FuseOSError(errno.EACCES)
        except:
            logging.error("could not find file")
            raise FuseOSError(errno.EACCES)

    def chown(self, path, uid, gid):
        logging.info("in chown - new uid " + str(uid) + " for " + path)
        uid = fuse.fuse_get_context()[0]
        gid = fuse.fuse_get_context()[1]
        permission, sql_uid, sql_gid = getPermission(path)
        try:
            if sql_uid == uid and sql_gid == gid:
                updateUSER(path, uid, gid)
                updateTime(path)
                globals.connection.commit()
            else:
                logging.info("Operation not permitted")
                raise FuseOSError(errno.EACCES)

        except:
            logging.error("could not find file")
            raise FuseOSError(errno.EACCES)

    def utimens(self, path, times=None):
        if times is None:
            times = (time.time(), time.time())
        updateTime(path, times[0], times[1])
        globals.connection.commit()

    def symlink(self, target, source):
        logging.info("in symlink")
        self.current_dir(target)

        createSymlink(target, source)
        globals.connection.commit()

    def readlink(self, path):
        logging.info("in readlink" + path)
        file = retrieveBLOB(path)
        if not file:
            logging.error("could not find file")
            raise FuseOSError(errno.ENOENT)

        buf = file.getFileData()
        buf = buf.decode("utf-8")
        return buf

    def current_dir(self, path):
        dir = path.split('/')
        dir.pop()
        globals.current_dir = '/'.join(dir)

        if globals.current_dir == '':
            globals.current_dir = '/'
        logging.info("CURRENT DIRECTORY " + globals.current_dir)
        return globals.current_dir

    def fsync(self, path, datasync, fh):
        logging.info("in fsync")
        globals.connection.commit()

    def flush(self, path, fh):
        globals.connection.commit()

    def setxattr(self, path, name, value, options, position=0):
        raise FuseOSError(errno.ENOTSUP)

    def listxattr(self, path):
        raise FuseOSError(errno.ENOTSUP)

    def removexattr(self, path, name):
        raise FuseOSError(errno.ENOTSUP)

    def statfs(self, path):
        raise FuseOSError(errno.ENOTSUP)

    def release(self, path, fh):
        return 0
