import joblib
from abc import ABC, abstractmethod
import warnings

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

    def check_flow(self, inputs):
        
        assert isinstance(inputs, dict)        
    
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
        **dismissed
        ):
        
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
            print('tranform_only method. fit method will not be performed')
            return

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
            print('fit_only method. transform method will not be performed')
            return

        output = getattr(
            self['estimator'],
            self['transform_method']
            )(
                **inputs,
                **self['estimator_transformargs']
            )

        return output


    def check_flow(allowed, check_mode, inputs):
        
        assert isinstance(inputs, dict)        
    
        if check_mode == 'filter':
            inputs = {input_name:value in input_name in allowed for input_name,value in inputs.items()}
            return inputs
        
        elif check_mode == 'raise':
            if not set(allowed) == set(inputs):
                print('input json must contain exactly {} keys'.format(allowed))
                raise AssertionError
        
        elif check_mode == 'ignore':
            return inputs
        
        elif check_mode == 'warn'
            intersec = set(allowed).intersection(set(inputs))
            missing = intersec - set(allowed)
            extra = intersec - set(inputs)
            
            warnings.warn("not allowed items passed: {}")

        else:
            print('check_mode should be one of ["filter","raise","ignore", "warn"]')
            raise ValueError