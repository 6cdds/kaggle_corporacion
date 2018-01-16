# -*- coding: utf-8 -*-

import sqlite3
import os
import pandas as pd
import datetime
import pickle

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
    
    for chunk in pd.read_csv(f, dtype=types, chunksize=1000000, low_memory = False):
        
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


def load_data(f_name, save_dir, other_data):
    data_chunks = []
    f = open(f_name, 'r')
    c_cnt = 0
    for chunk in pd.read_csv(f, dtype = {'item_nbr': str,
                 'store_nbr': str,
                 'unit_sales': 'float32',
                 'onpromotion': bool}, 
    usecols=['item_nbr', 'store_nbr', 'unit_sales', 'onpromotion', 'date'], chunksize = 1000000, low_memory = False, parse_dates=['date'], 
    infer_datetime_format=True ):
        data_chunks.append(chunk)
        print c_cnt    
        c_cnt = c_cnt + 1
    f.close()
    
    df = pd.concat(data_chunks)
    
    df['store_nbr'] = df['store_nbr'].astype(int).astype('category', categories = other_data['stores']['store_nbr'].cat.categories)
    df['item_nbr'] = df['item_nbr'].astype(int).astype('category', categories = other_data['items']['item_nbr'].cat.categories)  
    
    df['onpromotion'] = df['onpromotion'].fillna(value = False)
    
    return df

def split_data(df, save_dir):
    
    train_set = df.loc[df['date'] < datetime.datetime(2015, 1, 1),:]
    f = open(os.path.join(save_dir, 'train_set.p'), 'w')
    pickle.dump(train_set, f)
    f.close()
    
    test_set = df.loc[(df['date'] >= datetime.datetime(2015, 1, 1)) &\
                      (df['date'] < datetime.datetime(2016, 1, 1)),:]
    f = open(os.path.join(save_dir, 'test_set.p'), 'w')
    pickle.dump(test_set, f)
    f.close()        
   
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


def get_stores(f_name):
    f = open(f_name, 'r')
    df = pd.read_csv(f, dtype = {'store_nbr': 'int32', 
                                 'city': 'category',
                                 'state': 'category',
                                 'type': 'category',
                                 'cluster': 'category'})
    f.close()
    #df.index = list(df['store_nbr'])
    
    df['store_nbr'] = df['store_nbr'].astype('category')    
    df['city'] = df['city'].astype('category')
    df['state'] = df['state'].astype('category')
    df['type'] = df['type'].astype('category')  
    df['cluster'] = df['cluster'].astype('category')
    
    df.columns = ['store_nbr', 'store_city', 'store_state', 
                  'store_type', 'store_cluster']
    return df
    
def get_items(f_name):
    f = open(f_name, 'r')
    df = pd.read_csv(f, dtype = {'item_nbr': 'int32', 
                                 'family': 'category',
                                 'class': 'category',
                                 'perishable': 'float32'})
    
    df['item_nbr'] = df['item_nbr'].astype('category')
    df['family'] = df['family'].astype('category')
    df['class'] = df['class'].astype('category')  
        
    #df.index = list(df['item_nbr'])
    df.columns = ['item_nbr', 'item_family', 'item_class', 'item_perishable']
    return df

def get_oil(f_name):
    f = open(f_name, 'r')
    df = pd.read_csv(f, parse_dates=['date'], infer_datetime_format=True ) 

    df['dcoilwtico'] = pd.to_numeric(df['dcoilwtico'], 
          downcast = 'float')
    
    f.close()
    #date_year = []
    #date_month = []
    #date_day = []    
    #for x in df['date']:
    #    dt = datetime.datetime.strptime(x, '%Y-%m-%d')
    #    date_year.append(dt.year)
    #    date_month.append(dt.month)
    #    date_day.append(dt.day)

    #df['date_year'] = df['date'].dt.year
    #df['date_month'] = df['date'].dt.month
    #df['date_day'] = df['date'].dt.day
    
    #del df['date']
    
    return df

def get_holidays(f_name):
    f = open(f_name, 'r')
    
    df = pd.read_csv(f, dtype = {'type': str,
                             'locale': str,
                             'locale_name': str,
                             'description': str,
                             'transferred': bool}, 
        parse_dates=['date'],
        infer_datetime_format = True)

    df['type'] = df['type'].astype('category')    
    df['locale'] = df['locale'].astype('category')
    df['locale_name'] = df['locale_name'].astype('category')
    
    
    df.columns = ['date', 'holiday_type', 'holiday_locale', 
                  'holiday_locale_name', 'holiday_description',
                  'holiday_transferred']
    
    return df

def get_other_data(data_dir):
    
    oil = get_oil(os.path.join(data_dir, 'oil.csv'))

    holidays = get_holidays(os.path.join(data_dir, 'holidays_events.csv'))

    stores = get_stores(os.path.join(data_dir, 'stores.csv'))

    items = get_items(os.path.join(data_dir, 'items.csv'))
    
    return {'oil': oil, 'holidays': holidays, 'stores': stores, 'items': items}