# -*- coding: utf-8 -*-

import datetime
import pandas as pd

def get_features(data_df, stores, items, oil, holidays):
    
    feats = pd.merge(data_df, stores, on = 'store_nbr')
    feats = pd.merge(feats, items, on = 'item_nbr')
    
    merge_date_cols = ['date_year', 'date_month', 'date_day']
    
    feats = pd.merge(feats, holidays, 
                     left_on = merge_date_cols, right_on = merge_date_cols)
    
    feats = pd.merge(feats, oil, 
                     left_on = merge_date_cols, right_on = merge_date_cols)    
    
    '''
    feats = data_df.loc[:,['item_nbr', 'store_nbr', 'date_month', ]]

    other_feats = []
    for ind,row in data_df.iterrows():
        print ind
        feat_samp = {}
        feat_samp.update({'weekday': get_weekday(row),
                          'oil_price': get_oil_price(row, oil),
                          'is_holiday': get_is_holiday(row, holidays)})
    
        feat_samp.update(get_store_data(row, stores))
        feat_samp.update(get_item_data(row, items))
        other_feats.append(feat_samp)
    
    other_feats_df = pd.DataFrame(other_feats, index = feats.index)
    
    feats = pd.concat([feats, other_feats_df])
    '''
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
    
    feats_df['is_holiday'] = [False] * feats_df.shape[0]
    feats_df['is_event'] = [False] * feats_df.shape[0]
    
    for _,h in holidays.iterrows():
        
        if (h['holiday_type'] == 'Additional') or\
        (h['holiday_type'] == 'Bridge') or\
        (h['holiday_type'] == 'Holiday'):
        
            date_inds = (feats_df['date_year'] == h['date_year']) &\
            (feats_df['date_month'] == h['date_month']) &\
            (feats_df['date_day'] == h['date_day'])
            
            
            if h['holiday_locale'] == 'National':
                feats_df.loc[date_inds, 'is_holiday'] = True
                            
            if h['holiday_locale'] == 'Regional':
                feats_df.loc[date_inds & feats_df['store_state'] ==\
                             h['holiday_locale_name'], 'is_holiday'] = True  
    
            if h['holiday_locale'] == 'Local':
                feats_df.loc[date_inds & feats_df['store_city'] ==\
                             h['holiday_locale_name'], 'is_holiday'] = True  
                             
        elif h['holiday_type'] == 'Event':
            
            date_inds = (feats_df['date_year'] == h['date_year']) &\
            (feats_df['date_month'] == h['date_month']) &\
            (feats_df['date_day'] == h['date_day'])
            
            
            if h['holiday_locale'] == 'National':
                feats_df.loc[date_inds, 'is_holiday'] = True
                            
            if h['holiday_locale'] == 'Regional':
                feats_df.loc[date_inds & feats_df['store_state'] ==\
                             h['holiday_locale_name'], 'is_holiday'] = True  
    
            if h['holiday_locale'] == 'Local':
                feats_df.loc[date_inds & feats_df['store_city'] ==\
                             h['holiday_locale_name'], 'is_holiday'] = True  
                            