from bs4 import BeautifulSoup as beautyS, Tag
from collections import namedtuple
from datetime import date
from re import search


class NseHolidays:
    def __init__(self):
        with open(r'C:\Users\barraud\Downloads\bhav\archive\automation\nse-holidays.htm', 'r') as f:
            soup = beautyS(f, 'html.parser')
            self._tables: list = soup.find_all("table")
            self._months = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}

    def _get_date(self, cell_data) -> date:
        result = search("\s*([A-Za-z]+)\s*(\d{1,2})\s*\,\s*(\d{4})", cell_data.strip())
        if result:
            month:int = self._months[result.groups()[0][0:3].upper()]
            year:int = int(result.groups()[2])
            day:int = int(result.groups()[1])
            return date(year, month, day)
        else:
            if cell_data.strip() != 'Date':
                print(cell_data.strip())

    @property
    def holidays(self) -> list:
        holiday_list = []
        table: Tag
        for table in self._tables:
            table_rows: list = table.find_all("tr")
            table_row: Tag
            for table_row in table_rows:
                table_cells: list = table_row.find_all("td")
                reason: str = table_cells[1].text.strip().replace("\n", " ")
                reason = reason.replace("  ", " ")
                timestamp = self._get_date(table_cells[2].text)
                if timestamp:
                    holiday_list.append({'reason':reason,'timestamp':timestamp})
        return holiday_list


if __name__ == '__main__':
    nse_holidays = NseHolidays()
    for holiday in nse_holidays.holidays:
        print(holiday[0])
    print("ALL Done!")
