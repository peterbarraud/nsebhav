from BhavUtils.bhav_db import BhavDB
from BhavUtils.bhav_data_files import BhavFiles
import glob
from zipfile import ZipFile, ZIP_DEFLATED, BadZipFile
import os



def main():
    bhav_db = BhavDB()
    bhav_db.prepare_data_for_git()

if __name__ == '__main__':
    main()
    print("ALL Done!")
