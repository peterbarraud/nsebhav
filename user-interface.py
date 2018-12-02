from BhavUtils.bhav_db import BhavDB, DBConnectionError
from BhavUtils.bhav_data_files import BhavFiles
from BhavUtils.bhav_data_files import BhavFiles
import glob
from os import stat
from stat import *
import glob
from zipfile import ZipFile, ZIP_DEFLATED, BadZipFile
import os

def store_bhav_data(bhav_dir):

    bhav_files = BhavFiles(bhav_dir)
    bhav_db = BhavDB()
    for csv_data, zip_file_name in bhav_files.get_csv_data():
        if csv_data is not None:
            for csv_row in csv_data:
                # we're passing in the zip file name so we can log db errors with the name
                # should be easier to track down where exactly the rogue data was
                bhav_db.insert_bhav_row(csv_row, zip_file_name)
    logs: list = glob.glob("./logs/*.log")
    for log in logs:
        if stat(log)[ST_SIZE] > 0:
            print("Log {} seems to have some stuff in it. You might want to check if errors happened".format(log))


def get_data_of_git():
    bhav_db = BhavDB('mysql')
    bhav_db.get_data_of_git()


def bhav_count_by_year():
    bhav_db = BhavDB()
    for year, count in bhav_db.bhav_count_by_year:
        print(year, ":" , count)


def prepare_data_for_git():
    bhav_db = BhavDB()
    bhav_db.prepare_data_for_git()

def last_saved_date():
    bhav_db = BhavDB()
    print(bhav_db.last_saved_date)


def main():
    try:
        prepare_data_for_git()
    except DBConnectionError as db_conn_err:
        print(db_conn_err)
    # prepare_data_for_git()


if __name__ == '__main__':
    main()
    print("ALL Done!")
