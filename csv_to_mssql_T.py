import pandas as pd
import pyodbc
import time
import config
import os
from zipfile import ZipFile
from datetime import datetime
from dateutil.relativedelta import relativedelta


def histdata_date_parser(dtstr):
    return datetime.strptime(dtstr, '%Y%m%d %H%M%S%f')


def histdata_date_parser_m1(dtstr):
    return datetime.strptime(dtstr, '%Y%m%d%H%M')


def import_csv_to_mssql_tick(path, name, symbol):
    print(path+"--"+name+"--"+symbol)
    start = time.process_time()
    myzip = ZipFile(path)
    csv = myzip.open(name+'.csv')
    # Import CSV
    data = pd.read_csv(csv, header=None,
                       names=['time', 'ask', 'bid', 'vol'], date_parser=histdata_date_parser,
                       parse_dates=[0])
    df = pd.DataFrame(data, columns=['time', 'ask', 'bid'])
    insert_list = []
    print(df.count())
    conn = pyodbc.connect(config.mssql_constr)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    for row in df.itertuples():
        insert_params = (
            symbol,
            row[1],
            row[2],
            row[3]
        )

        insert_list.append(insert_params)
    cursor.executemany('''
                    INSERT INTO Prices.dbo.T (symbol,time, ask, bid)
                    VALUES (?,?,?,?)
                    ''', insert_list)
    conn.commit()
    end = time.process_time()
    print(str(end-start))


def import_csv_to_mssql_m1(path, name, symbol):
    print(path+"--"+name+"--"+symbol)
    start = time.process_time()
    myzip = ZipFile(path)
    csv = myzip.open(name+'.csv')
    # Import CSV
    data = pd.read_csv(csv, header=None,
                       names=['symbol', 'time', 'open', 'high', 'low', 'close', 'vol'], date_parser=histdata_date_parser_m1,
                       parse_dates=[1])
    df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close'])
    print(df.count())
    insert_list = []
    conn = pyodbc.connect(config.mssql_constr)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    for row in df.itertuples():
        insert_params = (
            symbol,
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
        )
        insert_list.append(insert_params)
    cursor.executemany('''
                    INSERT INTO Prices.dbo.M1 (symbol,time,o, h,l,c)
                    VALUES (?,?,?,?,?,?)
                    ''', insert_list)
    conn.commit()
    end = time.process_time()
    print(str(end-start))


for symbol in config.symbol_list:
    datetime_start = datetime.strptime(config.start_date, '%Y-%m-%d')
    datetime_end = datetime.strptime(config.end_date, '%Y-%m-%d')
    cur_datetime = datetime_start
    while(cur_datetime <= datetime_end):
        folder_path = config.root_path+"\\" + \
            str(cur_datetime.year) + "\\"+symbol.upper()
        if os.path.isdir(folder_path):
            files = os.listdir(folder_path)
            for file in files:
                if not os.path.isdir(file):
                    if "DAT_ASCII_XAUUSD_T" in file and "zip" in file and config.import_type == "tick":
                        filename_arr = os.path.splitext(file)
                        import_csv_to_mssql_tick(
                            folder_path+"\\"+file, filename_arr[0], symbol)
                    if "DAT_MS_XAUUSD_M1" in file and "zip" in file and config.import_type == "m1":
                        filename_arr = os.path.splitext(file)
                        if len(filename_arr[0]) == 21:
                            import_csv_to_mssql_m1(
                                folder_path+"\\"+file, filename_arr[0], symbol)
        cur_datetime = cur_datetime+relativedelta(years=1)
