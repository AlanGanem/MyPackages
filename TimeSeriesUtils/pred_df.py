import pandas as pd
import numpy as np 

def pred_df(y_true,y_pred, index ,fix_dim = -1, prefix = 'f_'):
    assert (len(y_true.shape),len(y_pred.shape)) == (2,2)
    index = np.array(index).flatten()
    error_dict = {}
    for forecast in range(y_pred.shape[fix_dim]):
        error_dict[forecast] = pd.DataFrame(y_pred.take(axis = fix_dim, indices = forecast).flatten(),index = index, columns = [forecast])
    #col_names = [str(prefix)+ str(forecast) for forecast in error_dict.keys()]
    i = 0
    for key,df in error_dict.items():
        df.columns = [str(prefix)+str(key)]
        if i ==0:
            error_df = df
            actual_index = df.index
        else:
            error_df = pd.concat([error_df,df], axis = 1)
        i+=1
    error_df = pd.concat([error_df, pd.DataFrame(y_true.take(axis = fix_dim, indices = 0), index = actual_index, columns = ['actual'])],axis = 1)
    return error_df
