# -*- coding: utf-8 -*-

import datetime

def get_features(data_df):
    
    feats = data_df.loc[:,['item_nbr', 'store_nbr', 'date_month', ]]
    


def get_weekday(row):
    return datetime.datetime(row['date_year'], row['date_month'], 
                             row['date_day']).weekday()