# -*- coding: utf-8 -*-

import numpy as np

def get_error(pred_res, actual_res, item_w):
        
    vals = []
    for i in range(len(pred_res)):
        vals.append(item_w[i]*(np.log(pred_res[i] + 1) - np.log(actual_res[i] + 1))**2)
        
    return np.sqrt(sum(vals) / sum(item_w))
