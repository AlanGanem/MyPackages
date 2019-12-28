import joblib
from abc import ABC, abstractmethod
import warnings
import copy

class BaseEstimator(ABC):
    
    @classmethod
    def load(cls, loading_path, **joblibargs):        
        return joblib.load(loading_path, **joblibargs)
    
    def save(self, saving_path, **joblibargs):
        joblib.dump(self, saving_path, **joblibargs)
            
                
    def __init__(
        self,
        inputs = None,
        outputs = None,
        name = None,
        input_check_mode = 'filter',
        output_check_mode = 'filter',
        fitargs = {},
        transformargs = {},
        **estimator_args
        ):
        
        if inputs:
            assert isinstance(inputs, list)
        if outputs:
            assert isinstance(outputs, list)
        
        
        if  name:
            self.__name__ = name
        
        self.inputs = inputs
        self.outputs = outputs
        self.input_check_mode = input_check_mode
        self.output_check_mode = output_check_mode
        self.estimator_args = estimator_args

        return

    def check_input(self, inputs):
        
        assert isinstance(inputs, dict)        
        
        if not isinstance(self.inputs, list):
            print('must assign the allowed inputs in constructor (list). not checking performed.')
            return        

        if self.input_check_mode == 'filter':
            inputs = {input_name:value in input_name in self.inputs for input_name,value in inputs.items()}
            return inputs
        
        elif self.input_check_mode == 'raise':
            if not set(self.inputs) == set(inputs):
                print('input json must contain exactly {} keys'.format(self.inputs))
                raise AssertionError
        
        elif self.input_check_mode == 'ignore':
            return

        else:
            print('input_check_mode should be one of ["filter","raise","ignore"]')
            raise ValueError

    def check_output(self, outputs):
        
        assert isinstance(outputs, dict)        
        
        if not isinstance(self.outputs, list):
            print('must assign the allowed outputs in constructor (list). not checking performed.')
            return

        if self.output_check_mode == 'filter':
            outputs = {output_name:value in output_name in self.outputs for output_name,value in output.items()}
            return outputs        
        
        elif self.output_check_mode == 'raise':
            if not set(self.outputs) == set(outputs):
                print('input json must contain exactly {} keys'.format(self.outputs))
                raise AssertionError
        
        elif self.output_check_mode == 'ignore':
            return

        else:
            print('output_check_mode should be one of ["filter","raise","ignore"]')
            raise ValueError

    @abstractmethod
    def fit(self,**inputs):
        pass

    @abstractmethod
    def transform(self,**inputs):
        pass


class Capsula():
    
    def __init__(
        self,
        estimator,
        name = None,
        fit_method = 'fit',
        fit_only = None,
        transform_method = 'transform',
        transform_only = False,
        estimator_fitargs = {},
        estimator_transformargs = {},
        allowed_inputs = None,
        allowed_outputs = None,
        input_check_mode = 'filter',
        output_check_mode = 'filter',
        **dismissed
        ):
        
        if callable(estimator):
            transform_only = True
            transform_method = '__call__'
        if not name:
            name = str(estimator)
        
        local_vars = locals()
        for var_name in local_vars:            
            setattr(self, var_name,local_vars[var_name])
        
        return

    def hatch(self):
        return self['estimator']

    def fit(
        self,
        **inputs
        ):
        
        if self['transform_only'] == True:
            print('{} is a tranform_only estimator. fit method will not be performed'.format(self['name']))
            return

        if self.allowed_inputs:
            self.check(
                allowed = self.allowed_inputs,
                check_mode = self.input_check_mode,
                inputs = inputs
                )

        getattr(
            self['estimator'],
            self['fit_method']
            )(
                **inputs,
                **self['estimator_fitargs']
            )
        
        return self

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self,key,value):
        setattr(self,key,value)
        return


    def transform(
        self,
        **inputs
        ):

        if self['fit_only'] == True:
            print('{} is a fit_only estimator. transform method will not be performed'.format(self['name']))
            return

        if self.allowed_inputs:
            inputs = self.check(
                allowed = self.allowed_inputs,
                check_mode = self.input_check_mode,
                inputs = inputs
                )
        
        output = getattr(
            self['estimator'],
            self['transform_method']
            )(
                **inputs,
                **self['estimator_transformargs']
            )

        if self.allowed_outputs:
            output = self.check(
                allowed = self.allowed_outputs,
                check_mode = self.output_check_mode,
                inputs = output
                )

        return output

    def check(self, allowed, check_mode, inputs):
        
        checked_input = check_flow(
            allowed = allowed,
            check_mode = check_mode,
            inputs = inputs
            )
        if checked_input:
            return checked_input


def check_flow(allowed, check_mode, inputs):
    
    if not isinstance(allowed, list):
        print('Allowed keys must be passed as a list. No checking performed.')
        return inputs

    if not isinstance(inputs, dict):
        print('inputs must be dict. No checking performed.')
        return inputs


    intersec = set(allowed).intersection(set(inputs))
    missing = set(allowed) - intersec
    extra = set(inputs) - intersec

    if check_mode == 'filter':        
        if (not missing) and (not (set(allowed) == set(inputs))):            
            inputs = {input_name:value in input_name in allowed for input_name,value in inputs.items()}
            return inputs        
        elif set(allowed) == set(inputs):
            return inputs
        else:
            raise AssertionError('in order to filter, input json must contain {}\n{} is missing'.format(set(allowed), missing))
    
    elif check_mode == 'raise':
        if not set(allowed) == set(inputs):
            raise AssertionError('input json must contain exactly {} keys'.format(allowed))
    
    elif check_mode == 'ignore':
        return inputs
    
    elif check_mode == 'warn':
        warnings.warn("not allowed items passed: {} \nmissing items: {}".format(str(extra),str(missing)))
        return inputs

    else:        
        raise ValueError('check_mode should be one of ["filter","raise","ignore", "warn"]')