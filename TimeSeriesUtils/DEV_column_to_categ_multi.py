# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 18:30:26 2019

@author: User Ambev
"""

cols = [
            'Vol_Beer_Dist',
            'Vol_Beer_Min',
            'Vol_Beer_Super',
            'Vol_Refri_Dist',
            'Vol_Refri_Min',
            'Vol_Refri_Super',
            
            'TTV_Beer_Dist',
            'TTV_Beer_Min',
            'TTV_Beer_Super',
            'TTV_Refri_Dist',
            'TTV_Refri_Min',
            'TTV_Refri_Super'
            ]
data = TSU.column_to_categorical(data,cols, column_names = ['unit','prod','channel'])
data = data.sort_index()
data_volume = data[data['unit'] == 'Vol']
data_volume = data_volume.rename(columns = {'Value':'Vol'})
data_volume = data_volume.drop(columns = ['unit'])
data_TTV = data[data['unit'] == 'TTV']
data_TTV = data_TTV.rename(columns = {'Value':'TTV'})
data_TTV = data_TTV.drop(columns = ['unit'])
data = pd.merge(data_TTV,data_volume, on = list(data_TTV.columns[0:-1])+[data_TTV.index.name], how = 'left')
    