# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import numpy as np

from sklearn.preprocessing import LabelEncoder

def get_features(data_df, other_data, label_encoders):
    
    feats = pd.merge(data_df, other_data['stores'], on = 'store_nbr')
    feats = pd.merge(feats, other_data['items'], on = 'item_nbr')
    
    feats['date_year'] = data_df['date'].dt.year
    feats['date_month'] = data_df['date'].dt.month
    feats['date_day'] = data_df['date'].dt.day
    feats['weekday'] = data_df['date'].dt.dayofweek
    
    feats = pd.merge(feats, other_data['oil'], how='left', 
                     left_on = ['date'], right_on = ['date'])    
        
    set_is_holiday_event(feats, other_data['holidays'])
    
    feats['onpromotion'] = data_df['onpromotion'].astype('uint8')
    
    del feats['id']
    
    # Interpolate missing oil prices
    feats = feats.interpolate()
    feats['dcoilwtico'].iloc[0] = feats['dcoilwtico'].iloc[1]    
    

    # Encode label features to integers
    feats = label_encode_feats(feats, other_data, label_encoders)
    
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
    
    for _,h in holidays.iterrows():
        
        if (h['holiday_type'] == 'Additional') or\
        (h['holiday_type'] == 'Bridge') or\
        (h['holiday_type'] == 'Holiday'):
        
            date_inds = (feats_df['date_year'] == h['date_year']) &\
            (feats_df['date_month'] == h['date_month']) &\
            (feats_df['date_day'] == h['date_day'])
            
            
            if h['holiday_locale'] == 'National':
                feats_df.loc[date_inds, 'is_holiday'] = 1
                            
            if h['holiday_locale'] == 'Regional':
                feats_df.loc[(date_inds) & (feats_df['store_state'] ==\
                             h['holiday_locale_name']), 'is_holiday'] = 1  
    
            if h['holiday_locale'] == 'Local':
                feats_df.loc[(date_inds) & (feats_df['store_city'] ==\
                             h['holiday_locale_name']), 'is_holiday'] = 1  
                             
        elif h['holiday_type'] == 'Event':
            
            date_inds = (feats_df['date_year'] == h['date_year']) &\
            (feats_df['date_month'] == h['date_month']) &\
            (feats_df['date_day'] == h['date_day'])
            
            
            if h['holiday_locale'] == 'National':
                feats_df.loc[date_inds, 'is_holiday'] = 1
                            
            if h['holiday_locale'] == 'Regional':
                feats_df.loc[(date_inds) & (feats_df['store_state'] ==\
                             h['holiday_locale_name']), 'is_holiday'] = 1 
    
            if h['holiday_locale'] == 'Local':
                feats_df.loc[(date_inds) & (feats_df['store_city'] ==\
                             h['holiday_locale_name']), 'is_holiday'] = 1
        
def fit_label_encoders(other_data):
    
    le_item_family = LabelEncoder()
    le_item_family.fit(np.unique(list(other_data['items']['item_family'])))
    
    le_store_city = LabelEncoder()
    le_store_city.fit(np.unique(list(other_data['stores']['store_city'])))
    
    le_store_state = LabelEncoder()
    le_store_state.fit(np.unique(list(other_data['stores']['store_state'])))
    
    le_store_type = LabelEncoder()
    le_store_type.fit(np.unique(list(other_data['stores']['store_type'])))  
    
    return {'item_family': le_item_family, 
            'store_city': le_store_city,
            'store_state': le_store_state,
            'store_type': le_store_type}

def label_encode_feats(feats, other_data, encoders):
    
    enc_feats = feats.copy()
    
    enc_feats['item_family'] = encoders['item_family'].transform(feats['item_family'])
    enc_feats['store_city'] = encoders['store_city'].transform(feats['store_city'])
    enc_feats['store_state'] = encoders['store_state'].transform(feats['store_state'])
    enc_feats['store_type'] = encoders['store_type'].transform(feats['store_type'])

    return enc_feats

                            