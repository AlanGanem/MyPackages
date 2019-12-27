# -*- coding: utf-8 -*-
"""
Created on Fri Dec 20 14:12:06 2019

@author: User Ambev
"""


import joblib
from .Base import BaseEstimator

class ModelPipeline():
    
    @classmethod
    def load(cls, loading_path, **joblibargs):
        return joblib.load(loading_path, **joblibargs)
    
    def save(self, saving_path, **joblibargs):
        joblib.dump(self, saving_path, **joblibargs)

    def __init__(self, flow = {}):
        
        assert isinstance(flow, dict)
        if not [step for step in flow] == list(range(len(flow))):
            print('flow keys must be sequential and starting at index 0 "0,1,2,3,..."')
            raise AssertionError
        
        self.flow = flow        
        
        self.step_names = {self.flow[step]['name']:step for step in self.flow.keys()}
        
        
    def transform(
            self,
            flow_step = None,
            name = None,
            transform_method = 'transform',
            **inputs
            ):
        
        '''
        transformargs is a dict containing arguments accepted in the estimator transform method 
        {estimator_name:{**args}}
        
        '''
        
        if name and (not flow_step):
            flow_step = self.step_names[name]
                            
        return getattr(self.flow[flow_step]['estimator'], transform_method)(**inputs)
    
    def fit(
            self,
            return_ledger = True,
            **inputs
            ):
        
        '''fitflowargs is a dict containing fit and predict args for each estimator.
            method specific args are:
                input_preprocessor,
                output_preprocessor,
                inputs (indicates de step input if it takes inputs from bypassed outputs),
                bypass (wheter the estimator's output becomes input to the next estimator)
                **fitargs,
                **transformargs
        '''
        
        
        flow_ledger = {}
        for step in self.flow:
            
            #checks if step is function
            if callable(step):
                inputs = step(**inputs)
                continue

            if isinstance(step, BaseEstimator):
                estimator = step
                step = {
                'estimator':estimator,
                'name':str(estimator),
                }

            if isinstance(step, dict):
                #checks if its a fit only method
                if not isinstance(step['estimator'],BaseEstimator):
                    print('estimator must be an instance of BaseEstimator class')
                    raise TypeError

                if 'fit_only' in step:
                    if step['fit_only'] == False:
                        continue
                
                if not 'fit_method' in step:
                    step['fit_method'] = 'fit'

                if not 'transform_method' in step:
                    step['transform_method'] = 'transform'

                if not 'fitargs' in step:
                    step['fitargs'] = {}
                
                if not 'transformargs' in step:
                    step['transformargs'] = {}

                if not 'name' in step:
                    step['name'] = step['estimator']

                assert isinstance(step['transformargs'], dict)
                assert isinstance(step['fitargs'], dict)

                getattr(step['estimator'],step['fit_method'])(
                                                **inputs,
                                                **step['fitargs']
                                                )
                
                if return_ledger:
                    flow_ledger[step['name']] = {}
                    flow_ledger[step['name']]['input'] = inputs
            
                inputs = gettattr(step,'transform_method')(
                    **inputs,
                    **step['transformargs']
                    )
            
            if return_ledger:
                flow_ledger[step['name']]['output'] = inputs
        
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
            
            if isinstance(step, BaseEstimator):
                estimator = step
                step = {
                'estimator':estimator,
                'name':str(estimator),
                }

            elif callable(step):
                estimator = step
                step = {
                    'estimator':estimator,
                    'name':str(estimator),
                    'fitargs' = '__call__',
                    'transformargs' = '__call__'
                }

            else:

                print('estimator must be an instance of BaseEstimator class or function')
                raise TypeError


                ####################################
                if not isinstance((step['estimator']),BaseEstimator):
                    print('estimator must be an instance of BaseEstimator class or function')
                    raise TypeError


                if 'fit_only' in step:
                    if step['fit_only'] == True:
                        continue
                
                if not 'fit_method' in step:
                    step['fit_method'] = 'fit'

                if not 'transform_method' in step:
                    step['transform_method'] = 'transform'

                if not 'fitargs' in step:
                    step['fitargs'] = {}
                
                if not 'transformargs' in step:
                    step['transformargs'] = {}

                if not 'name' in step:
                    step['name'] = step['estimator']

                assert isinstance(step['transformargs'], dict)
                assert isinstance(step['fitargs'], dict)

                if not step['fit_method'] == '__call__':
                    getattr(step['estimator'],step['fit_method'])(
                                                    **inputs,
                                                    **step['fitargs']
                                                    )
                
                if return_ledger:
                    flow_ledger[step['name']] = {}
                    flow_ledger[step['name']]['input'] = inputs
            
                inputs = gettattr(step,'transform_method')(
                    **inputs,
                    **step['transformargs']
                    )
            
            if return_ledger:
                flow_ledger[step['name']]['output'] = inputs
        
        if return_ledger:
            self.fit_flow_ledger = flow_ledger

        return self





    def transform_flow(
        self,
        return_ledger = True,
        init_step = 0,
        **inputs
        ):
        
        '''fitflowargs is a dict containing fit and predict args for each estimator.
            method specific args are:
                input_preprocessor,
                output_preprocessor,
                inputs (indicates de step input if it takes inputs from bypassed outputs),
                bypass (wheter the estimator's output becomes input to the next estimator)
                **fitargs,
                **transformargs
        '''
        
        
        flow_ledger = {}
        for step in self.flow:
            if step == 0:
                step = init_step
                continue

            if 'transform_only' in self.flow[step]:
                if self.flow[step]['transform_only']:
                    continue

            if 'input_preprocessor' in self.flow[step]:
                inputs = self.flow[step]['input_preprocessor'](**inputs)
                            
            
            if return_ledger:
                flow_ledger[step] = {}
                flow_ledger[step]['input'] = inputs
            
            inputs = self.transform(
                **inputs,
                flow_step = step,
                **self.flow[step]['transformargs']
                )
            
            if 'output_preprocessor' in self.flow[step]:
                inputs = self.flow[step]['output_preprocessor'](**inputs)
            
            if return_ledger:
                flow_ledger[step]['output'] = inputs
        
        
        if return_ledger:
            return flow_ledger
        else :
            return inputs