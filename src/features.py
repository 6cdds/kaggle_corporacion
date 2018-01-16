# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import numpy as np

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder

def get_features(data_df, other_data, label_encoders):
    
    feats = pd.merge(data_df, other_data['stores'], how = 'left',
                     left_on = 'store_nbr', right_on = 'store_nbr')
    feats = pd.merge(feats, other_data['items'], how = 'left',
                     left_on = 'item_nbr', right_on = 'item_nbr')
    
    feats['date_year'] = data_df['date'].dt.year.astype('category', 
         categories = [2013, 2014, 2015, 2016])
    feats['date_month'] = data_df['date'].dt.month.astype('category')
    feats['date_day'] = data_df['date'].dt.day.astype('category')
    feats['weekday'] = data_df['date'].dt.dayofweek.astype('category')
    
    feats = pd.merge(feats, other_data['oil'], how='left', 
                     left_on = ['date'], right_on = ['date'])    
        
    set_is_holiday_event(feats, other_data['holidays'])
    
    feats['onpromotion'] = data_df['onpromotion'].astype('uint8')    
    
    # Interpolate missing oil prices
    feats['dcoilwtico'] = feats['dcoilwtico'].fillna(method = 'ffill')
    feats['dcoilwtico'] = feats['dcoilwtico'].fillna(method = 'bfill')
    

    # Encode label features to integers
    #feats = label_encode_feats(feats, other_data, label_encoders)
    
    return feats
    


def get_weekday(row):
    return datetime.datetime(row['date_year'], row['date_month'], 
                             row['date_day']).weekday()
    
def get_store_data(row, stores):
    return dict(stores.loc[row['store_nbr'],['city', 'state', 'cluster', 'type']])

def get_item_data(row, items):
    return dict(items.loc[row['item_nbr'],['family', 'class', 'perishable']])

def get_oil_price(row, oil):
    return float(oil.loc[(oil['date_year'] == row['date_year']) &\
                   (oil['date_month'] == row['date_month']) &\
                   (oil['date_day'] == row['date_day']), 'dcoilwtico'])

def get_is_holiday(row, holidays):
    res = holidays.loc[(holidays['date_year'] == row['date_year']) &\
                   (holidays['date_month'] == row['date_month']) &\
                   (holidays['date_day'] == row['date_day']), :]
    
    if res.shape[0] == 0:
        return 0
    
    if (res['type'].iloc[0] == 'Work Day') or\
    (res['type'].iloc[0] == 'Transfer') or\
    (res['type'].iloc[0] == 'Event'):
        return 0
    else:
        return 1
    
def set_is_holiday_event(feats_df, holidays):
    
    feats_df['is_holiday'] = [0] * feats_df.shape[0]
    feats_df['is_event'] = [0] * feats_df.shape[0]
    
    feats_df['is_holiday'] = feats_df['is_holiday'].astype('uint8')
    feats_df['is_event'] = feats_df['is_event'].astype('uint8')
    
    for _,h in holidays.iterrows():
        
        if (h['holiday_type'] == 'Additional') or\
        (h['holiday_type'] == 'Bridge') or\
        (h['holiday_type'] == 'Holiday'):
        
            #date_inds = (feats_df['date_year'] == h['date_year']) &\
            #(feats_df['date_month'] == h['date_month']) &\
            #(feats_df['date_day'] == h['date_day'])
            
            date_inds = feats_df['date'] == h['date']
            
            
            if h['holiday_locale'] == 'National':
                feats_df.loc[date_inds, 'is_holiday'] = 1
                            
            if h['holiday_locale'] == 'Regional':
                feats_df.loc[(date_inds) & (feats_df['store_state'] ==\
                             h['holiday_locale_name']), 'is_holiday'] = 1  
    
            if h['holiday_locale'] == 'Local':
                feats_df.loc[(date_inds) & (feats_df['store_city'] ==\
                             h['holiday_locale_name']), 'is_holiday'] = 1  
                             
        elif h['holiday_type'] == 'Event':
            
            #date_inds = (feats_df['date_year'] == h['date_year']) &\
            #(feats_df['date_month'] == h['date_month']) &\
            #(feats_df['date_day'] == h['date_day'])
            
            date_inds = feats_df['date'] == h['date']
            
            
            if h['holiday_locale'] == 'National':
                feats_df.loc[date_inds, 'is_holiday'] = 1
                            
            if h['holiday_locale'] == 'Regional':
                feats_df.loc[(date_inds) & (feats_df['store_state'] ==\
                             h['holiday_locale_name']), 'is_holiday'] = 1 
    
            if h['holiday_locale'] == 'Local':
                feats_df.loc[(date_inds) & (feats_df['store_city'] ==\
                             h['holiday_locale_name']), 'is_holiday'] = 1
        
def fit_label_encoders(feats):
    
    encoders = {}
    for col in feats.columns:
        if (feats[col].dtype.name == 'category') and\
        (not issubclass(feats[col].cat.categories.dtype.type, np.integer)):
            encoders[col] = LabelEncoder()
            encoders[col].fit(list(feats[col].cat.categories))
    
    return encoders

def label_encode_feats(feats, encoders):
    
    enc_feats = feats.copy()
    for col in encoders.keys():  
        enc_feats[col] = encoders[col].transform(feats[col])
    
    return enc_feats
                               

def get_dummy_feats(train_feats, test_feats):
    
    num_train = train_feats.shape[0]
    num_test = test_feats.shape[0]
    
    train_inds = range(num_train)
    test_inds = range(num_train, num_train + num_test)
    
    cols = ['onpromotion', 'store_city', 'store_state',
            'store_type', 'store_cluster', 'item_family', 'item_class', 
            'date_month', 'date_day', 'weekday', 'dcoilwtico', 'is_holiday',
            'is_event']
    
    temp = pd.get_dummies(pd.concat([train_feats.loc[:,cols], test_feats.loc[:,cols]]), sparse = True)
    
    return temp.iloc[train_inds,:], temp.iloc[test_inds, :]
    