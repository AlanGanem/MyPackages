
import pandas as pd

def period_mape(preds_df, freq = 'W',actual_col = 'actual', abs = True, fillna_val = 0):
    preds_df = preds_df.fillna(fillna_val)
    f_cols = [i for i in preds_df.columns if i[:2] == 'f_']
    error_df = pd.DataFrame()
    for col in f_cols:
        error_df[str(col)] = (preds_df[col]-preds_df[actual_col])
    error_df['actual'] = preds_df[actual_col]
    
    mape_abs = pd.DataFrame()
    for col in f_cols:
        mape_abs[str(col)] = error_df[col].abs().resample(freq).sum()/error_df['actual'].abs().resample(freq).sum()
        
    mape_signal = pd.DataFrame()
    for col in f_cols:
        mape_signal[str(col)] = error_df[col].resample(freq).sum()/error_df['actual'].resample(freq).sum()
    
    return mape_abs if abs == True else mape_signal
