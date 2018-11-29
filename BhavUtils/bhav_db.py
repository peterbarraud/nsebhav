import glob
from zipfile import ZipFile, ZIP_DEFLATED, ZipInfo
# import pymysql
from pymysql import Connect, ProgrammingError, cursors, InternalError, IntegrityError, DataError
import calendar
from datetime import date, timedelta
import re
from os import path
import os


class BadDatabaseFilesError(Exception):
    pass


class BadTimestampYearError(Exception):
    pass


class BhavDB:
    def __init__(self, mysqldb='bhavdata'):
        self.__connection = Connect(host='localhost', user='root', password='',
                                     db=mysqldb, charset='utf8mb4', cursorclass=cursors.DictCursor)
        with self.__connection.cursor() as cursor:
            tables_sql = "SELECT table_name FROM information_schema.tables where table_schema = 'bhavdata';"
            cursor.execute(tables_sql)
            self._bhavcopy_tables = [t['table_name'] for t in cursor.fetchall()]
        if not path.exists("./logs"):
            os.mkdir("./logs")
        self._logger = open("./logs/bhav_db.log", "w")
        self._month_dict = {v.upper(): k for k,v in enumerate(calendar.month_abbr)}

    # get the first nse bhav date in recorded history
    # return: date
    @property
    def the_first_date(self) -> date:
        first_date = date(1994, 11, 3)
        return first_date


    def last_saved_date(self) -> date:
        with self.__connection.cursor() as cursor:
            sql_statement = "SELECT max(table_name) first_year_table FROM information_schema.tables where table_name like 'bhavcopy_%';"
            cursor.execute(sql_statement)
            result = cursor.fetchone()
            sql_statement = "select max(timestamp) last_saved_date from {}".format(result['first_year_table'])
            cursor.execute(sql_statement)
            result = cursor.fetchone()
            return result['last_saved_date']

    def _log_err(self, row:dict, err:Exception, zip_file_name:str):
        self._logger.write("Error: {}\n".format(str(err)))
        self._logger.write("For: (Symbol: {}; Series: {}; Timestamp: {})\n".format(row['SYMBOL'], row['SERIES'], row['TIMESTAMP']))
        self._logger.write("In: ({})\n".format(path.basename(zip_file_name)))
        self._logger.write("+"*max([len("Record: Symbol: {}; Series: {}; Timestamp: {}".format(row['SYMBOL'], row['SERIES'], row['TIMESTAMP'])), len(str(err)), len(os.path.basename(zip_file_name))]) + "\n")

    # we will say that data by year
    # this is for github purposes not for production
    # so that we don't have to upload one humongous file every time we push updates to github
    # only the most recent year table is effected.
    # previous year tables are completely historic data so they won't every change once that year is done
    # PRIMARY KEY: This is on the symbol, series and timestamp fields since this combination is unique
    def _create_year_table(self, year: int):
        table_name = "bhavcopy_{}".format(year)
        create_table_sql = "CREATE table " + " if not exists " + table_name + "("
        create_table_sql += "symbol varchar(15),"
        create_table_sql += "series char(2),"
        create_table_sql += "open DECIMAL(8,2),"
        create_table_sql += "high DECIMAL(8,2),"
        create_table_sql += "low DECIMAL(8,2),"
        create_table_sql += "close DECIMAL(8,2),"
        create_table_sql += "last DECIMAL(8,2),"
        create_table_sql += "prevclose DECIMAL(8,2),"
        create_table_sql += "tottrdqty int unsigned,"
        create_table_sql += "tottrdval bigint unsigned,"
        if year >= 2011:
            create_table_sql += "totaltrades mediumint unsigned default null,"
            create_table_sql += "isin char(12) default null,"
        create_table_sql += "timestamp date,"
        create_table_sql += "primary key(symbol, series, timestamp)"
        create_table_sql += ");"
        with self.__connection.cursor() as cursor:
            cursor.execute(create_table_sql)

    def __del__(self):
        self.__connection.commit()
        self.__connection.close()
        self._logger.close()

    def _get_mysql_date(self, timestamp):
        time_parts = timestamp.split("-")
        year = time_parts[2]
        month = str(self._month_dict[time_parts[1]]).zfill(2)
        day = str(time_parts[0]).zfill(2)
        return year + "-" + month + "-" + day

    def discard_tablespace(self, year):
        with self.__connection.cursor() as cursor:
            try:
                cursor.execute("alter table bhavcopy_{} discard tablespace;".format(year))
                self.__connection.commit()
            except InternalError as int_err:
                if str(int_err) == "(1932, \"Table 'bhavdata.bhavcopy_{}' doesn't exist in engine\")".format(year):
                    raise BadDatabaseFilesError("Bad database files. The bhavdata database needs to be dropped and recreated. ")

    def import_tablespace(self, year):
        with self.__connection.cursor() as cursor:
            cursor.execute("alter table bhavcopy_{} import tablespace;".format(year))

    def recreate_bhavdata(self):
        with self.__connection.cursor() as cursor:
            cursor.execute("use mysql; drop database bhavdata; create database bhavdata; use bhavdata")

    def insert_bhav_row(self, row: dict, zip_file_name: str):
        bhavcopy_table = "bhavcopy_{}".format(row['TIMESTAMP'][-4:])
        field_sql_list: str = ""
        value_sql_list:str = ""
        for field_name, value in row.items():
            if field_name.strip() != "":
                field_sql_list += field_name + ", "
                if field_name == 'TIMESTAMP':
                    value_sql_list += "'" + self._get_mysql_date(value) + "', "
                else:
                    value_sql_list += "'" + value + "', "
        field_sql_list = field_sql_list.strip(", ")
        value_sql_list = value_sql_list.strip(", ")
        sql_insert = "insert into {table_name} ({field_list}) values({value_list})".format(table_name=bhavcopy_table, field_list=field_sql_list, value_list=value_sql_list)
        with self.__connection.cursor() as cursor:
            try:
                if bhavcopy_table not in self._bhavcopy_tables:
                    self.create_year_table(row['TIMESTAMP'])
                    self._bhavcopy_tables.append(bhavcopy_table)
                cursor.execute(sql_insert)
            except IntegrityError as integrity_err:
                # IMPORTANT: Rather than check, we're just going to let the db fail on duplicates
                # and then not log the duplicate error
                if "(1062, \"Duplicate entry '" + row['SYMBOL'] + "-" + row['SERIES'] + "-" + self._get_mysql_date(row['TIMESTAMP']) + "\' for key 'PRIMARY'\")" in str(integrity_err):
                    pass
                else:
                    self._log_err(row, integrity_err, zip_file_name)
            except BadTimestampYearError as bad_timestamp_year_err:
                self._log_err(row, bad_timestamp_year_err, zip_file_name)
            except DataError as data_err:
                self._log_err(row, data_err, zip_file_name)
            except InternalError as interal_err:
                self._log_err(row, interal_err, zip_file_name)

    def get_last_saved_date(self):
        with self.__connection.cursor() as cursor:
            tables_sql = "SELECT table_name FROM information_schema.tables where table_name like 'bhavcopy_%';"
            cursor.execute(tables_sql)
            result = cursor.fetchall()
            first_year = min([int(re.search('\d{4}$', x['table_name']).group()) for x in result])
        return first_year

    def prepare_data_for_git(self):
        with self.__connection.cursor() as cursor:
            for bhavcopy_table in self._bhavcopy_tables:
                print("Doing: {}".format(bhavcopy_table))
                try:
                    cursor.execute("select * into outfile '../../bhavcopy.sql.zip/{0}.sql' from {0};".format(bhavcopy_table))
                    with ZipFile("bhavcopy.sql.zip/{0}.sql.zip".format(bhavcopy_table), 'w', ZIP_DEFLATED) as sql_zip:
                        sql_zip.write("bhavcopy.sql.zip/{0}.sql".format(bhavcopy_table), "{}.sql".format(bhavcopy_table))
                    os.remove("bhavcopy.sql.zip/{0}.sql".format(bhavcopy_table))
                except InternalError as int_err:
                    print(int_err)

    def get_data_of_git(self):
        with self.__connection.cursor() as cursor:
            try:
                cursor.execute("create database bhavdata;")
                cursor.execute("use bhavdata;")
                bhavcopy_sql_zip_files = glob.glob("bhavcopy.sql.zip/*.zip")
                for bhavcopy_sql_zip_file in bhavcopy_sql_zip_files:
                    bhavcopy_sql_zip = ZipFile(bhavcopy_sql_zip_file)
                    file_zip_info: ZipInfo = bhavcopy_sql_zip.filelist[0]
                    print(file_zip_info.filename)
                    bhavcopy_sql_zip.extract(file_zip_info.filename, "mariadb.bhav/data/bhavdata")
                    bhavcopy_sql_zip.close()
                    (table_name, year) = re.search("(bhavcopy_(\d{4}))", file_zip_info.filename).groups()
                    self._create_year_table(int(year))
                    cursor.execute("load data infile '{}' into table {};".format(file_zip_info.filename, table_name))
                    os.remove("mariadb.bhav/data/bhavdata/{}".format(file_zip_info.filename))
            except ProgrammingError as prog_err:
                print(prog_err)


# this is a package class but we've got the main function in here only for testing purposes
def main():
    bhav_db = BhavDB()
    bhav_db.last_saved_date()


if __name__ == '__main__':
    main()