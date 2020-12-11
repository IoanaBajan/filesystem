from _datetime import datetime
import stat

from pip._vendor.distlib.compat import raw_input

from fileManager import showFiles, createBlob, retrieveBLOB, deleteBLOB, createDir, copyFile

print(
    " 1.ls\n 2.create file\n 3.copy file/move\n 4.find file \n 5.delete file/directory \n 6.create directory")


def main():
    print(datetime.now().strftime("%Y%m%d%H%M%S"))
    while True:
        try:
            input = int(raw_input())
        except:
            return
        if input == 1:
            showFiles()
        elif input == 2:
            print("name the file")
            fileName = raw_input()
            print("path where you want to create it")
            path = raw_input()
            print("file input")
            data = raw_input()
            createBlob(filePath=path, fileName=fileName, input=data)

        elif input == 3:
            print("name the file")
            fileName = raw_input()
            print("path where you want to create it")
            path = raw_input()
            print("file input")
            data = raw_input()
            copyFile(filePath=path, fileName=fileName, input=data)

        elif input == 4:
            fileName = raw_input()
            retrieveBLOB(fileName=fileName)
        elif input == 5:
            fileName = raw_input()
            deleteBLOB(fileName=fileName)
        elif input == 6:
            dirname = raw_input()
            path = raw_input()
            createDir(dirname, path)
        else:
            return

if _name_ == '_main_':
    main()