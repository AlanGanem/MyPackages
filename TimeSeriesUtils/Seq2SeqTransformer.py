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
        '''
        look_back_period includes the actual date (look_back_period = 1 is the same as today's info and not yesterday's)
        pred_period does not include today (look_back_period = 1 is the same as tomorrows info)
        '''

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
    
    def __init__(
    	self,            
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
        self.split_params = False
        
        return
    
    def fit(
            self,
            df,
            ):
            
        df = self.fill_date_gaps(df, self.freq)
        extd_freq = df.index.freq
        assert self.freq == df.index.freq
        
        lbpargs = {str(extd_freq)[1:-1].lower()+'s':self.look_back_period} 
        ppargs = {str(extd_freq)[1:-1].lower()+'s':self.pred_period}
        p1args = {str(extd_freq)[1:-1].lower()+'s':1}
    
        self.unit_period_dt = pd.DateOffset(**p1args)
        self.look_back_period_dt= pd.DateOffset(**lbpargs)
        self.pred_period_dt = pd.DateOffset(**ppargs)
        
        self.min_past_date = min(df.index)+self.look_back_period_dt - self.unit_period_dt    
        self.max_future_date = max(df.index) - self.pred_period_dt - self.unit_period_dt

        return self

    def transform(
    	self,
    	df,
    	as_array_dict = False,
    	train_test_split = False,
    	train_split_start = None,
    	train_split_end = None,
    	test_split_start = None,
    	test_split_end = None,
    	**fillnaargs
    	):
        
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
        
        fitted_dict = {
                'X_dict':pd.concat(date_dict_X).fillna(**fillnaargs),
                'y_dict':pd.concat(date_dict_y).fillna(**fillnaargs),
                'covars_dict':pd.concat(date_dict_covars).fillna(**fillnaargs),
                }

        self.train_split_start = None
        self.train_split_end = None
        self.test_split_start = None
        self.test_split_end = None
        
        if train_test_split:
        	self.train_test_split(
				train_split_start = train_split_start,
		    	train_split_end = train_split_end,
		    	test_split_start = test_split_start,
		    	test_split_end = test_split_end
        		)
        	fitted_dict =self.split_transform(fitted_dict)

        if as_array_dict:
            return self.transform_array(fitted_dict)
        else:
            return fitted_dict

    def create_multiindex(self):
        return    

    def transform_array(
    	self,
    	fitted_dict
    	):    

        transformed_dict = {}
        date_dict = {}
        for key,df in fitted_dict.items():
            grouper = df.groupby(level = 0)
            df_list = []
            date_list = []
            for date, df in grouper:
                df_list.append(df.values)
                date_list.append(date)
            transformed_dict[key] = np.array(df_list)
            date_dict[key] = date_list
        self.date_dict = date_dict

        return transformed_dict

    def split_transform(
        self,
        df,
        train_split_start,
        train_split_end,
        test_split_start,
        test_split_end,
        **fillnaargs
        ):
    
	    self.train_test_split(
	        train_split_start = train_split_start,
	        train_split_end = train_split_end,
	        test_split_start = test_split_start,
	        test_split_end = test_split_end,
	        )
	    
	    tr_s = pd.to_datetime(self.train_split_start)
	    tr_e = pd.to_datetime(self.train_split_end)
	    ts_s = pd.to_datetime(self.test_split_start)
	    ts_e = pd.to_datetime(self.test_split_end) 
		    
	    fitted_dict = self.transform(df,**fillnaargs)
		    
	    split_dict = {
	        'X_train': fitted_dict['X_dict'].loc[tr_s:tr_e],
	        'X_covars_train': fitted_dict['covars_dict'].loc[tr_s:tr_e],
	        'y_train': fitted_dict['y_dict'].loc[tr_s:tr_e],
	        'X_test': fitted_dict['X_dict'].loc[ts_s:ts_e],
	        'X_covars_test': fitted_dict['covars_dict'].loc[ts_s:ts_e],
	        'y_test': fitted_dict['y_dict'].loc[ts_s:ts_e]                 
		            }
	    
	    return split_dict

    def split_transform_array(
        self,
        df,
        train_split_start,
        train_split_end,
        test_split_start,
        test_split_end,
        **fillnaargs
        ):
        
        split_dict = self.split_transform(
        	df = df,
			train_split_start = train_split_start, 
			train_split_end = train_split_end,
			test_split_start = test_split_start,
			test_split_end = test_split_end,
			**fillnaargs
        )

        transformed_dict = self.transform_array(split_dict)
        return transformed_dict

    def train_test_split(
        self,
        train_split_start,
        train_split_end,
        test_split_start,
        test_split_end,
        ):

        self.train_split_start = pd.to_datetime(train_split_start) 
        self.train_split_end = pd.to_datetime(train_split_end)
        self.test_split_start = pd.to_datetime(test_split_start)
        self.test_split_end = pd.to_datetime(test_split_end)
        assert self.train_split_start >= self.min_past_date
        assert self.test_split_end <= self.max_future_date
        assert self.train_split_end < self.test_split_start
	    
        self.split_params = True
        return