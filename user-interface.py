from BhavUtils.bhav_db import BhavDB
from BhavUtils.bhav_data_files import BhavFiles
import glob
from zipfile import ZipFile, ZIP_DEFLATED, BadZipFile
import os


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

def main():
    prepare_data_for_git()


if __name__ == '__main__':
    main()
    print("ALL Done!")
