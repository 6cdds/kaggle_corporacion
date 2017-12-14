# -*- coding: utf-8 -*-

import sqlite3
import os
import pandas as pd

DB_NAME = 'kaggle_corporacion_db'

def create_train_db(f_name, db_loc):    
    
    types = {'id': 'int64',
             'item_nbr': 'int32',
             'store_nbr': 'int16',
             'unit_sales': 'float32',
             'onpromotion': bool}
    
    connex = sqlite3.connect(os.path.join(db_loc, DB_NAME + '.db'))
    #cur = connex.cursor()
    
    chunk_cnt = 0
    f = open(f_name, 'r')
    for chunk in pd.read_csv(
            f, parse_dates=['date'],  dtype=types, chunksize=10000000, 
            infer_datetime_format=True):
        chunk.to_sql(name="train_data", con=connex, if_exists="append", index=False)
        chunk_cnt = chunk_cnt + 1
        print chunk_cnt
    f.close()