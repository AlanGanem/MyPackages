import pandas as pd

class DfScaler():

    def __init__(self, method = None, columns = None ,method_columns = None):
        
        self.avalible_scalers = ['MinMaxScaler','StandardScaler','RobustScaler']
        
        if isinstance(method_columns,dict):    
            self.method_columns = method_columns
        else:
            self.method_columns = {method: columns}
        
        self.columns = sum((lst for lst in [values for key,values in self.method_columns.items()]),[])
        self.method = [key for key in self.method_columns.keys()]

        return

    def fit(self, df, percentile_1 = 0.25, percentile_3 = 0.75):
        
        assert 0<percentile_1<percentile_3<1
        
        df = df.astype(float)
        self.min = {self.columns[i]:df[self.columns[i]].min() for i in range(len(self.columns))}
        self.max = {self.columns[i]:df[self.columns[i]].max() for i in range(len(self.columns))}
        self.mean = {self.columns[i]:df[self.columns[i]].mean() for i in range(len(self.columns))}
        self.median = {self.columns[i]:df[self.columns[i]].median() for i in range(len(self.columns))}
        self.std = {self.columns[i]:df[self.columns[i]].std() for i in range(len(self.columns))}
        self.q1 = {self.columns[i]:df[self.columns[i]].quantile(percentile_1) for i in range(len(self.columns))}
        self.q3 = {self.columns[i]:df[self.columns[i]].quantile(percentile_3) for i in range(len(self.columns))}
        

        self.no_variation_list = [key for key in self.std if self.std[key] == 0]
        if len(self.no_variation_list) >0:
            print('{} columns has variance = 0 and will not be scaled'.format(self.no_variation_list))
            self.columns = [col for col in self.columns if col not in self.no_variation_list]
        return
    
    def transform(self,df):
        df = df.astype(float)
        scaled_df = df
        
        for method in self.method_columns.keys():
            
            if method == 'MinMaxScaler':
                for column in self.method_columns[method]:
                    scaled_df[[column]] = (df[[column]]-self.min[column])/(self.max[column]-self.min[column])
            
            elif method == 'StandardScaler':
                for column in self.method_columns[method]:
                    scaled_df[[column]] = (df[[column]]-self.median[column])/(self.std[column])
            
            elif method == 'RobustScaler':
            	for column in self.method_columns[method]:
            		scaled_df[[column]] = ((df[[column]]-self.q1[column])/(self.q3[column]-self.q1[column]))
            
            return scaled_df


    def inverse_transform(self, df):
        inv_df = pd.DataFrame()
        
        for method in self.method_columns.keys():
        
            if method == 'MinMaxScaler':
                for column in self.method_columns[method]:
                    inv_df[[column]] = df[[column]]*(self.max[column]-self.min[column])+self.min[column]
            
            elif method == 'StandardScaler':
                for column in self.method_columns[method]:
                    inv_df[[column]] = df[[column]]*self.std[column]+self.mean[column]
            
            if method == 'RobustScaler':
                for column in self.method_columns[method]:
                    inv_df[[column]] = df[[column]]*(self.q3[column]-self.q1[column])+self.q1[column]
            
            return inv_df
