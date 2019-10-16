# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 23:20:00 2019

@author: User Ambev
"""
defaultdict = defaultdict(dict)


final_keys = [
        'X_train',
        'X_covars_train',
        'y_train',
        ...
        ]


fit()
def rec(val):
    for key,value in defaultdict.items():
        if not  key in final_keys:
            return rec(value)
        else:
            transform()