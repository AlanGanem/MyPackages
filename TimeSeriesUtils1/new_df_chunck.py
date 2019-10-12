# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 09:18:03 2019

@author: User Ambev
"""


def chunk_data_by_date_df_covars(df,pred_period,look_back_period,input_columns, output_columns,future_covars_columns = [] ,feature_axis =-1,n_validation_intervals = 1,flatten = False):
    '''
    Groups data in date period chuncks predefined for X and y and splits both
    in train and validation sets.
    The order of the features will be the same as specified in the '_columns' params
    return format:
        X_train, y_train, X_val, y_val
    OBS.:make sure the temporal axis is the first one
    '''
    if not all([i in list(df.columns) for i in output_columns+future_covars_columns+output_columns]):
        columns_not_in_frame = [i for i in output_columns+future_covars_columns+output_columns if i not in list(df.columns)]
        raise Exception("{}".format(columns_not_in_frame) + ' not in data frame')
    
    df = df[input_columns+future_covars_columns+output_columns]
    
    X = df.values
    indexes = df.index
    output_index = [list(df.columns).index(i) for i in future_covars_columns+output_columns if i in list(df.columns)]
    input_index = [list(df.columns).index(i) for i in input_columns if i in list(df.columns)]

    X_shape = X.shape
    X_n_dim = len(X_shape)
    assert np.abs(feature_axis) <= X_n_dim
    
    X_train_past = []
    y_train_past = []
    indexes_past = []
    
    for i in range(look_back_period, X_shape[0]):
        X_train_past.append(X[i-look_back_period:i].take(input_index, axis = feature_axis))
        try:
            indexes_past.append(indexes[i-1])
        except IndexError:
            indexes_past.append('NotInIndex')
            
    for i in range(look_back_period,X_shape[0]-pred_period):
        y_train_past.append(X[i:i+pred_period].take(output_index, axis = feature_axis))
        
    X_new = np.array(X_train_past)[:-pred_period]
    #X_train = np.reshape(np.array(X_train_past),[np.array(X_train_past).shape[0],np.array(X_train_past).shape[1],1])
    y_new =  np.array(y_train_past)
    
    
    assert  X_new.shape[0] == y_new.shape[0]
    
    #y_train= min_max_scaler.fit_transform(y_train)
    #y_new = np.reshape(y_new,list(y_new.shape)+[1])
    
    X_val = X_new[-pred_period*n_validation_intervals:]
    y_val = y_new[-pred_period*n_validation_intervals:]
    index_val = indexes_past[-pred_period*n_validation_intervals:]
    X_train = X_new[:-pred_period*n_validation_intervals]
    y_train = y_new[:-pred_period*n_validation_intervals]
    index_train = indexes_past[:-pred_period*n_validation_intervals]
    
    X_covars_train = y_train.take(range(1,y_train.shape[-1]-1),axis = -1)
    X_covars_val = y_val.take(range(1,y_val.shape[-1]-1),axis = -1)
    period_train = y_train.take([0],axis = -1)
    period_val = y_val.take([0],axis = -1)
    y_train = y_train.take([-1],axis = -1)
    y_val = y_val.take([-1],axis = -1)
    
    if X_covars_val.size == 0:
        assert X_covars_val.size == X_covars_train.size
        list_shape_X_cov_val = list(X_covars_val.shape)
        list_shape_X_cov_train = list(X_covars_train.shape)
        list_shape_X_cov_val[-1] = 1
        list_shape_X_cov_train[-1] = 1
        X_covars_val = np.zeros(list_shape_X_cov_val)
        X_covars_train = np.zeros(list_shape_X_cov_train)
        
    if flatten:
        y_train,y_val =  y_train.reshape(y_train.shape[:-1]), y_val.reshape(y_val.shape[:-1])
    
    assert y_train.shape[0] ==X_train.shape[0] 
    assert y_val.shape[0] ==X_val.shape[0] 
    print((' {} = {} \n {} = {} \n {} = {} \n {} = {} \n {} = {} \n {} = {} \n').format(
            'X_train.shape', X_train.shape,
            'y_train.shape', y_train.shape,
            'X_covars_train.shape',X_covars_train.shape,
            'X_val.shape', X_val.shape,
            'y_val.shape', y_val.shape,
            'X_covars_val.shape',X_covars_val.shape))
    
    print('total amount of samples = {} \n learning window = {} \n prediction horizon = {}'.format((X_train.shape[0] + X_val.shape[0]),X_train.shape[1],y_train.shape[1]))    
    return {
            'train':
                {'X':X_train,'y':y_train,'future_covars':X_covars_train ,'index':index_train},
            'val':
                {'X':X_val,'y':y_val,'future_covars':X_covars_val ,'index':index_val}
            }
