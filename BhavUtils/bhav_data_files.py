from zipfile import ZipFile
from zipfile import BadZipFile
import csv
import glob
import os


class CSVNotFoundInZipError(Exception):
    pass

class BhavDownloadedFolderNotFound(Exception):
    pass

class BhavFiles:
    def __init__(self):
        self._logger = open("./logs/bhav_file.log", "w")
        if not os.path.exists("bhav_downloaded"):
            raise BhavDownloadedFolderNotFound("Folder not found."
                                               "The downloaded bhav files must exist in a folder"
                                               "bhav_downloaded in the root project folder."
                                               "At the same level as user-interface.py")
        self._zip_files = glob.glob("bhav_downloaded/cm*bhav.csv.zip")

    def __del__(self):
        self._logger.close()

    @staticmethod
    def _get_csv_filename_in_zip(zip_file_name: str, zip_contents):
        if len(zip_contents.filelist) == 1:
            if zip_contents.filelist[0].filename[-3:] == 'csv':
                return zip_contents.filelist[0].filename
            else:
                raise CSVNotFoundInZipError("The zip file contains a single file but it doens't seem to be a CSV file. "
                                            "Possibly invalid zip file: " + zip_file_name)
        else:
            raise CSVNotFoundInZipError("We're expecting a single csv in each zip archive."
                                        "Possibly invalid zip file: " + zip_file_name)

    def _get_data_from_zip(self, zip_file_name: str):
        try:
            with ZipFile(zip_file_name, 'r') as zip_contents:
                try:
                    csv_file_name = BhavFiles._get_csv_filename_in_zip(zip_file_name, zip_contents)
                    with zip_contents.open(csv_file_name) as csv_file:
                        return csv.DictReader(csv_file.read().decode("utf-8").split("\n"))
                except CSVNotFoundInZipError as cnfiz:
                    print("csv file not found in zip: ", zip_file_name)
                    self._logger.write("csv file not found in zip: " + zip_file_name)
        except BadZipFile as bzf:
            print("bad zip file: ", zip_file_name)
            self._logger.write("bad zip file: " + zip_file_name + "\n")

    def get_csv_data(self):
        for zip_file_name in self._zip_files:
            print("Doing file: ", zip_file_name)
            yield self._get_data_from_zip(zip_file_name), zip_file_name


# this is a class lib. the following two are only for internal testing
def main():
    bhav_files = BhavFiles(r"C:\Users\barraud\Downloads\bhav\archive\\")
    for csv_data in bhav_files.get_csv_file_contents():
        for row in csv_data:
            print(row)

if __name__ == '__main__':
    main()
