from pip._vendor.distlib.compat import raw_input

from fileManager import showFiles, insertBLOB, retrieveBLOB, deleteBLOB

print(" 1.ls\n 2.create file\n 3.find file \n 4.delete file")

input = int(input())


def switch_option():
    if input == 1:
        showFiles()
    elif input == 2:
        fileName = raw_input()
        data = raw_input()
        insertBLOB(filePath=data, fileName=fileName)
    elif input == 3:
        fileName = raw_input()
        retrieveBLOB(fileName=fileName)
    elif input == 4:
        fileName = raw_input()
        deleteBLOB(fileName=fileName)


switch_option()
