class File:
    def __init__(self, key, type, inode,uid, gid, mode, atime, mtime, ctime,size,block_size, data_block):
        self.key = key
        self.type = type
        self.uid = uid
        self.gid = gid
        self.mode = mode
        self.atime = atime
        self.mtime = mtime
        self.ctime = ctime
        self.size = size
        self.block_size = block_size
        self.data_block = data_block
        self.inode = inode
    def getFilePath(self):
        return self.key

    def getFileType(self):
        return self.type
    def getFileInode(self):
        return self.inode

    def getFileMode(self):
        return self.mode

    def getMtime(self):
        return self.mtime

    def getCtime(self):
        return self.ctime

    def getAtime(self):
        return self.atime

    def getFileUid(self):
        return self.uid

    def getFileGid(self):
        return self.gid

    def getSize(self):
        return self.size
    def getBlockSize(self):
        return self.block_size

    def getFileData(self):
        return self.data_block

    def setData_block(self, data_block):
        self.data_block = data_block

    def getFile(self):
        return self.inode,self.key,self.size, self.data_block