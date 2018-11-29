from BhavUtils.bhav_db import BhavDB
from BhavUtils.bhav_data_files import BhavFiles
import glob
from zipfile import ZipFile, ZIP_DEFLATED, BadZipFile
import os



def main():
    bhav_db = BhavDB('mysql')
    bhav_db.get_data_of_git()

if __name__ == '__main__':
    main()
    print("ALL Done!")
