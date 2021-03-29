class File:
    def __init__(self, key, type, uid, gid, mode, ctime,data_block):
        self.key = key
        self.type = type
        self.uid = uid
        self.gid = gid
        self.mode = mode
        self.ctime = ctime
        self.data_block = data_block

    def getFilePath(self):
        return self.key

    def getFileType(self):
        return self.type

    def getFileMode(self):
        return self.mode

    def getFileData(self):
        return self.data_block

    def setData_block(self, data_block):
        self.data_block = data_block

    def getFile(self):
        return self.key, self.type, self.uid, self.gid, self.mode, self.ctime
