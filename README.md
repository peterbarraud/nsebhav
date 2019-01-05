After the clone
#Setup
1. Goto: `mariadb.bhav`
2. Double-click `start.db`
3. Goto user-iinterface.py and put `get_data_of_git()` into `main`
4. Run `what you want to do`

*Good to go!*

#Store data
1. First, ensure that the folder `bhav_downloaded` is available at the root project dir. At the same level as `user-iinterface.py`
2. Goto user-iinterface.py and put `unsaved_dates_till_today()` into `main`
3. Run `what you want to do`
4. Save the output into a word doc so you can easily copy these into the nse archive date field
4. Go to [nse archive](https://nseindia.com/products/content/equities/equities/archieve_eq.htm)
5. Choose Report type as `Bhavcopy`
6. Get all zips till *last saved date*
7. Goto user-iinterface.py and put `store_bhav_data()` into `main`
8. Run `what you want to do`

#Git commit
So that we don't commit whole piles of bhav data back, we need to run a procedure that will only give us the changes .sql files
1. Goto user-iinterface.py and put `prepare_data_for_git()` into `main`
2. Run `what you want to do`
3. In `git bash`, start with `git status` and then go on from there
