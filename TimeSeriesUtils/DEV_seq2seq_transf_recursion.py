# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 23:20:00 2019

@author: User Ambev
"""


final_keys = [
        'X_train',
        'X_covars_train',
        'y_train',
        ...
        ]


from collections import defaultdict

                
def iter_data_dict(d, array_dict):
    for k,v in d.items():        
        if isinstance(v, dict):
            iter_data_dict(v,array_dict)
        else:            
            try:                
                array_dict[k] = np.concatenate([array_dict[k],v],axis = 0)
            except:
                print(k)
                array_dict[k] = v
    return array_dict

def dd():
    return defaultdict(dd)        

data_dict_teste ={}
data_dict_teste['a'] = data_dict 
data_dict_teste['b'] = data_dict 


arr_dict = dd()
teste = iter_data_dict(data_dict_teste,arr_dict)

for dfr in df.groupby(level=0):
    pass
