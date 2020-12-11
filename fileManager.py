from mysql.connector import cursor

from DBManager.DBConnect import DBConnect


def convertToBinaryData(filename):
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData


def convertToFile(data, filename):
    with open(filename, 'wb') as file:
        file.write(data)


def insertBLOB(filePath, fileName):
    print("inserting blob into file table")

    connection = DBConnect()
    cursor = connection.getDB().cursor()

    sql_statement = """INSERT INTO file 
                        (data,file_name) VALUES (%s,%s)"""

    binFile = convertToBinaryData(filePath)
    insert_blob_tuple = (binFile, fileName)
    cursor.execute(sql_statement, insert_blob_tuple)
    connection.commit()


def deleteBLOB(fileName):
    print("deleting blob from file table")

    connection = DBConnect()
    cursor = connection.getDB().cursor()

    sql_statement = """DELETE FROM file 
                        WHERE file_name= %s 
                        LIMIT 1"""

    cursor.execute(sql_statement, (fileName,))
    connection.commit()


def retrieveBLOB(fileName):
    print("retrieving file from table")

    connection = DBConnect()
    cursor = connection.getDB().cursor()

    sql_statement = "SELECT file_name,data FROM file WHERE file_name= %s"

    cursor.execute(sql_statement, (fileName,))

for fileName, data in cursor.fetchall():
        print(fileName, data)
        convertToFile(data, fileName)


def showFiles():
    connection = DBConnect()
    cursor = connection.getDB().cursor()
    sql_statement = "SELECT * FROM file "
    cursor.execute(sql_statement)
    for fileid, fileName, data in cursor.fetchall():
        print(fileid, fileName)
