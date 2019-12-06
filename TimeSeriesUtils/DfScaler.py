import pandas as pd

class DfScaler():

    def __init__(self, method):
        assert method in ['MinMaxScaler','StandardScaler','RobustScaler']
        self.method = method
        return

    def fit(self, df,columns, percentile_1 = 0.25, percentile_3 = 0.75):
        assert 0<percentile_1<percentile_3<1
        df = df.astype(float)
        self.columns = columns
        self.min = {columns[i]:df[self.columns[i]].min() for i in range(len(columns))}
        self.max = {columns[i]:df[self.columns[i]].max() for i in range(len(columns))}
        self.mean = {columns[i]:df[self.columns[i]].mean() for i in range(len(columns))}
        self.median = {columns[i]:df[self.columns[i]].median() for i in range(len(columns))}
        self.std = {columns[i]:df[self.columns[i]].std() for i in range(len(columns))}
        self.q1 = {columns[i]:df[self.columns[i]].quantile(percentile_1) for i in range(len(columns))}
        self.q3 = {columns[i]:df[self.columns[i]].quantile(percentile_3) for i in range(len(columns))}
        

        self.no_variation_list = [key for key in self.std if self.std[key] == 0]
        if len(self.no_variation_list) >0:
            print('{} columns has variance = 0 and will not be scaled'.format(self.no_variation_list))
            self.columns = [col for col in self.columns if col not in self.no_variation_list]
        return
    
    def transform(self,df):
        df = df.astype(float)
        scaled_df = df
        
        if self.method == 'MinMaxScaler':
            for column in self.columns:
                scaled_df[[column]] = (df[[column]]-self.min[column])/(self.max[column]-self.min[column])
        
        elif self.method == 'StandardScaler':
            for column in self.columns:
                scaled_df[[column]] = (df[[column]]-self.median[column])/(self.std[column])
        
        elif self.method == 'RobustScaler':
        	for column in self.columns:
        		scaled_df[[column]] = ((df[[column]]-self.q1[column])/(self.q3[column]-self.q1[column]))
        
        return scaled_df


    def inverse_transform(self, df, inv_columns):
        inv_df = pd.DataFrame()
        inv_columns = inv_columns
        
        if self.method == 'MinMaxScaler':
            for column in inv_columns:
                inv_df[[column]] = df[[column]]*(self.max[column]-self.min[column])+self.min[column]
        
        elif self.method == 'StandardScaler':
            for column in inv_columns:
                inv_df[[column]] = df[[column]]*self.std[column]+self.mean[column]
        
        if self.method == 'RobustScaler':
            for column in inv_columns:
                inv_df[[column]] = df[[column]]*(self.q3[column]-self.q1[column])+self.q1[column]
        
        return inv_df
