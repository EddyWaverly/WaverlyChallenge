import csv
import psycopg2
import requests
import json


# Interface for Database Connection
class DatabaseConnection:
    def connect(self):
        pass

    def disconnect(self):
        pass

    def executeQuery(self, query, params=None):
        pass


# PostgreSQL implementation
class PostgreSQLConnection(DatabaseConnection):
    def __init__(self, dbname, user, password, host='localhost', port='5432'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def executeQuery(self, query, params=None):
        self.cursor.execute(query, params)
        return self.cursor.fetchall()


# Process csv data
class ProcessCsvFile:
    def readCsvFile(csvFile):
        with open(csvFile, mode='r') as file:
            csvReader = csv.reader(file, delimiter='\t')
            return csvReader


# Retrieve and read Json file from the url
class ProcessJsonFile:
    def processJsonFromUrl(url):
        try:
            response = requests.get(url)
            response.raiseForStatus()

            data = response.json()
            return data

        except requests.exceptions.HTTPError as httpErr:
            print(f"HTTP error occurred: {httpErr}")

        except Exception as err:
            print(f"Other error occurred: {err}")


class ProcessData:
    sql = PostgreSQLConnection('test', 'eddy', 'password')

    @staticmethod
    def saveData():
        csvFile = 'test.csv'
        data = [row for row in ProcessCsvFile.readCsvFile(csvFile)]
        insert_query = """
            INSERT INTO Process_GDELT (GLOBALEVENTID, SQLDATE, EventCode, EventBaseCode, EventRootCode, 
            ActionGeo_FullName, ActionGeo_CountryCode, ActionGeo_Lat, ActionGeo_Long, DATEADDED, SOURCEURL)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for row in data:
            PostgreSQLConnection.connection.cursor(insert_query, (row[0], row[1], row[26], row[27], row[28], row[52],
                                                       row[53], row[56], row[57], row[59], row[60]))
        PostgreSQLConnection.connection.commit()
        PostgreSQLConnection.cursor.close()
        PostgreSQLConnection.connection.close()
