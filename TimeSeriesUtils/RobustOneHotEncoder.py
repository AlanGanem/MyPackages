# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 09:30:37 2019

@author: User Ambev
"""

import pandas as pd

class RobustOneHotEncoder():
    
    def __init__(self):
        return
    
    def fit(self,df,cat_columns, prefix_sep = '__'):
        self.cat_columns = cat_columns
        self.prefix_sep = prefix_sep
        assert all([col in df.columns for col in self.cat_columns])
        
        print('applying pd.get_dummies method')
        one_hot_fit = pd.get_dummies(df, columns = cat_columns, prefix_sep = prefix_sep)
        print('Done')
        
        self.cat_dummies = [col for col in one_hot_fit
               if prefix_sep in col 
               and col.split(prefix_sep)[0] in self.cat_columns]
        
        return self
    
    def transform(self, df, verbose = True):
        one_hot_transform = pd.get_dummies(df, prefix_sep=self.prefix_sep, 
                                   columns=self.cat_columns)
        
        # Remove additional columns
        for col in one_hot_transform.columns:
            if ("__" in col) and (col.split("__")[0] in self.cat_columns) and col not in self.cat_dummies:
                if verbose:
                    print("Removing additional feature {}".format(col))
                one_hot_transform.drop(col, axis=1, inplace=True)
                
        for col in self.cat_dummies:
            if col not in one_hot_transform.columns:
                if verbose:
                    print("Adding missing feature {}".format(col))
                one_hot_transform[col] = 0
                
        return one_hot_transform
