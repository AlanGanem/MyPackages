# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 19:14:26 2019

@author: User Ambev
"""
import numpy as np
import pandas as pd
import xarray as xnp

class Seq2SeqTransformer():

    def fill_date_gaps(self,df,freq,fillna_value = None,fillna_method = None , **kwargs):
        date_min = df.index.min()
        date_max = df.index.max()
        full_period = pd.DataFrame(index = pd.date_range(start = date_min, end = date_max, **kwargs))
        df = pd.concat((full_period,df),axis = 1)
        df.index.freq = freq
        if fillna_method and not fillna_value:
            df.fillna(method = fillna_method)
        elif not fillna_method and  fillna_value:
            df.fillna(value = fillna_value)
        
        assert df.index.freq == freq
        return df
    
    def __init__(self,            
            past_variables,
            dependent_vars,
            future_covars,
            look_back_period,
            pred_period ,
            freq = 'D'
            ):
        
        self.past_variables = past_variables 
        self.dependent_vars = dependent_vars
        self.future_covars = future_covars
        self.look_back_period = look_back_period
        self.pred_period = pred_period
        self.freq = freq
        self.unit_period = 1
        
        return
    
    def fit(self, df):
        
        df = self.fill_date_gaps(df, self.freq)
        extd_freq = df.index.freq
        assert self.freq == df.index.freq
        
        lbpargs = {str(extd_freq)[1:-1].lower()+'s':self.look_back_period} 
        ppargs = {str(extd_freq)[1:-1].lower()+'s':self.pred_period}
        p1args = {str(extd_freq)[1:-1].lower()+'s':1}
    
        self.unit_period_dt = pd.DateOffset(**p1args)
        self.look_back_period_dt= pd.DateOffset(**lbpargs)
        self.pred_period_dt = pd.DateOffset(**ppargs)
        
        date_dict_X ={}
        date_dict_y ={}
        date_dict_covars ={}
        for date in df.index:        

            date_dict_X[date] = df.loc[pd.to_datetime(date)-self.look_back_period_dt + self.unit_period_dt:pd.to_datetime(date), self.past_variables].reset_index(drop = True)
            if date_dict_X[date].shape[0] != self.look_back_period:
                date_dict_X.pop(date,None)
                
            date_dict_y[date] = df.loc[date+self.unit_period_dt:pd.to_datetime(date)+self.pred_period_dt, self.dependent_vars].reset_index(drop = True)
            if date_dict_y[date].shape[0] != self.pred_period:
                date_dict_y.pop(pd.to_datetime(date),None) 
        
            date_dict_covars[date]= df.loc[date+self.unit_period_dt:pd.to_datetime(date)+self.pred_period_dt, self.future_covars].reset_index(drop = True)
            if date_dict_covars[date].shape[0] != self.pred_period:
                    date_dict_covars.pop(pd.to_datetime(date),None) 

        
        self.min_past_date = min(list(date_dict_X))
        self.max_future_date = max(list(date_dict_y))
        self.max_past_date = max(list(date_dict_X))
        self.min_future_date = min(list(date_dict_y))
        
        self.fitted_dict = {
                'X_dict':pd.concat(date_dict_X),
                'y_dict':pd.concat(date_dict_y),
                'covars_dict':pd.concat(date_dict_covars)
                }
        
        self.train_split_start = None
        self.train_split_end = None
        self.test_split_start = None
        self.test_split_end = None
        
        return
    
    def create_multiindex(self):
        return
    
    def train_test_split(           
            self,
            train_split_start,
            train_split_end,
            test_split_start,
            test_split_end           
            ):
        
        self.train_split_start = pd.to_datetime(train_split_start) 
        self.train_split_end = pd.to_datetime(train_split_end)
        self.test_split_start = pd.to_datetime(test_split_start)
        self.test_split_end = pd.to_datetime(test_split_end)
        
        assert self.train_split_start >= self.min_past_date
        assert self.test_split_end <= self.max_future_date
        assert self.train_split_end < self.test_split_start
        
        return
    
    def transform(self):    
        
        tr_s = self.train_split_start
        tr_e = self.train_split_end
        ts_s = self.test_split_start
        ts_e = self.test_split_end 
        
        
        split_dict = {
                'X_train': self.fitted_dict['X_dict'].loc[tr_s:tr_e],
                'X_covars_train': self.fitted_dict['covars_dict'].loc[tr_s:tr_e],
                'y_train': self.fitted_dict['y_dict'].loc[tr_s:tr_e],
                'X_test': self.fitted_dict['X_dict'].loc[ts_s:ts_e],
                'X_covars_test': self.fitted_dict['covars_dict'].loc[ts_s:ts_e],
                'y_test': self.fitted_dict['y_dict'].loc[ts_s:ts_e]                 
                }
        
        transformed_dict = {}
        for key,df in split_dict.items():
            grouper = df.groupby(level = 0)
            teste_list = []
            for group , df in grouper:
                teste_list.append(df.values)
            transformed_dict[key] = np.array(teste_list)    
        
        return transformed_dict