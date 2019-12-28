# -*- coding: utf-8 -*-
"""
Created on Fri Dec 20 14:12:06 2019

@author: User Ambev
"""


import joblib
from .Base import BaseEstimator, Capsula

class ModelPipeline():
    
    @classmethod
    def load(cls, loading_path, **joblibargs):
        return joblib.load(loading_path, **joblibargs)

    def save(self, saving_path, **joblibargs):
        joblib.dump(self, saving_path, **joblibargs)

    def __init__(self, flow = []):
        self.flow = [encapsulate(**step) if isinstance(step,dict) else encapsulate(**{'estimator':step}) for step in flow]
        
        self.flow = []
        i = 0
        for step in flow:

            if isinstance(step,dict):
                step = encapsulate(**step)
                setattr(step,'name',step['name']+'_'+str(i))
                self.flow.append(step)
            
            else:
                step = encapsulate(**{'estimator':step})
                setattr(step,'name',step['name']+'_'+str(i))
                self.flow.append(step)
            
            i+=1
        return
    
    def fit(
            self,
            return_ledger = True,
            **inputs
            ):
        
        flow_ledger = {}
        for step in self.flow:
            
            if return_ledger:
                flow_ledger[step['name']] = {}
                flow_ledger[step['name']]['input'] = inputs
            
            step.fit(**inputs)
            output = step.transform(**inputs)

            if return_ledger:
                flow_ledger[step['name']]['output'] = output
            
            inputs = output
        if return_ledger:
            self.fit_flow_ledger = flow_ledger

        return self
        
    def transform(
                self,
                return_ledger = True,
                **inputs
                ):
                    
            flow_ledger = {}
            for step in self.flow:
                
                if return_ledger:
                    flow_ledger[step['name']] = {}
                    flow_ledger[step['name']]['input'] = inputs
                
                output = step.transform(**inputs)

                if return_ledger:
                    flow_ledger[step['name']]['output'] = output

                inputs = output
            
            if return_ledger:
                self.fit_flow_ledger = flow_ledger
                return flow_ledger
            else:
                return output

def encapsulate( **capsulaargs):
        return Capsula(**capsulaargs)