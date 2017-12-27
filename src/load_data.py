# -*- coding: utf-8 -*-

import sqlite3
import os
import pandas as pd
import datetime

DB_NAME = 'kaggle_corporacion_db'

def create_train_db(f_name, db_loc):    
    
    types = {'id': 'int64',
             'item_nbr': 'int32',
             'store_nbr': 'int16',
             'unit_sales': 'float32',
             'onpromotion': bool,
             'date': str}
    
    connex = sqlite3.connect(os.path.join(db_loc, DB_NAME + '.db'))
    #cur = connex.cursor()
    
    chunk_cnt = 0
    f = open(f_name, 'r')
    #for chunk in pd.read_csv(
    #        f, parse_dates=['date'],  dtype=types, chunksize=10000000, 
    #        infer_datetime_format=True):
    
    for chunk in pd.read_csv(dtype=types, chunksize=10000000):
        
        date_year = []
        date_month = []
        date_day = []
        for x in chunk['date']:
            dt = datetime.datetime.strptime(x, '%Y-%m-%d')
            date_year.append(dt.year)
            date_month.append(dt.month)
            date_day.append(dt.day)
        
        chunk['date_year'] = date_year
        chunk['date_month'] = date_month
        chunk['date_day'] = date_day
        
        del chunk['date']
        
        chunk.to_sql(name="train_data", con=connex, if_exists="append", index=False)
        chunk_cnt = chunk_cnt + 1
        print chunk_cnt
    f.close()
    connex.close()
    
def create_train_db_indices(db_loc):
    
    connex = sqlite3.connect(os.path.join(db_loc, DB_NAME + '.db'))
    c = connex.cursor()
    c.execute('CREATE INDEX date_year ON train_data(date_year)')
    c.execute('CREATE INDEX date_month ON train_data(date_month)')
    c.execute('CREATE INDEX date_day ON train_data(date_day)')
    c.execute('CREATE INDEX store_nbr ON train_data(store_nbr)')
    c.execute('CREATE INDEX item_nbr ON train_data(item_nbr)')
    
    connex.close()
    
def get_year_data(db_loc, d_year):
    connex = sqlite3.connect(os.path.join(db_loc, DB_NAME + '.db'))

    data = pd.read_sql_query(
            'SELECT * FROM train_data WHERE date_year = ' + str(d_year), 
            connex)
    connex.close()
    return data
    
def parse_date(date_str):
    #dt = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    
    return (dt - datetime.datetime(2013,1,1)).total_seconds()