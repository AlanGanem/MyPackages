import pickle
from abc import ABC, abstractmethod
import warnings
import inspect
import re

### Capsula not checking properly optional and required arguments of methods and functions

class BaseEstimator(ABC):

    @classmethod
    def load(cls, loading_path, **pickleargs):
        with open(loading_path, 'rb') as file:
            loaded_pipe = pickle.load(file, **pickleargs)
        return loaded_pipe

    def save(self, saving_path, **pickleargs):
        with open(saving_path, 'wb') as file:
            pickle.dump(self, file, **pickleargs)
            
                
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
        
        
        if name:
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
            outputs = {output_name:value in output_name in self.outputs for output_name,value in outputs.items()}
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
        fit_only = False,
        transform_method = 'transform',
        transform_only = False,
        estimator_fitargs = {},
        estimator_transformargs = {},
        required_inputs = None,
        allowed_outputs = None,
        input_check_mode = 'warn',
        output_check_mode = 'warn',
        input_nodes = {},
        output_nodes = {},
        departures = [],
        landing_zone = {},
        takeoff_zone = {},
        takeoff_state_mode = 'data',
        requried_inputs_landed = False,
        is_fitted = False,
        output_name = None,
        wrapped_output = False,
        is_transformed = False,
        is_callable = False,
        **dismissed
        ):
        
        
        if isinstance(estimator, self.__class__):
            return
        
        if fit_only and transform_only:
            raise AssertionError('fit_only and transform_only cannot be assigned simutaneously')
        if callable(estimator):
            is_callable = True
            transform_method = '__call__'
            fit_method = '__call__'

        if not name:
            try:
                name = estimator.__name__
            except:
                name = str(estimator)

        if estimator != None:
            
            if not required_inputs:
                if fit_only:
                    inspectobj = inspect.getfullargspec(getattr(estimator, fit_method))
                elif transform_only:
                    inspectobj = inspect.getfullargspec(getattr(estimator, transform_method))
                else:
                    inspectobj = inspect.getfullargspec(getattr(estimator, fit_method))

                try:
                    required_inputs = inspectobj.args[:-len(inspectobj.defaults)]
                    optional_inputs = inspectobj.args[-len(inspectobj.defaults):]
                except TypeError:
                    required_inputs = inspectobj.args
                    optional_inputs = 'None'

        if not allowed_outputs:
            allowed_outputs = get_output_json_keys(estimator, transform_method)
        if not output_name:
            output_name = name+'_output'
        output_nodes = set(output_nodes)
        input_nodes = input_nodes

        ##### define everythong before this line
        local_vars = locals()
        for var_name in local_vars:            
            setattr(self, var_name,local_vars[var_name])
        
        return

    def __str__(self):
        return self.name
    
    #def __repr__(self):
    #    return self.name
    
    def __call__(self, inputs):
        if not (isinstance(inputs,list) or isinstance(inputs,Capsula)):
            raise TypeError('input must be instance of Capsula or list o Capsulas')

        if isinstance(inputs,list):
            self.input_nodes = inputs
        else:
            self.input_nodes = [inputs]
        return self

    def hatch(self):
        return self.estimator

    def send(self,variables, to_node, pipe_call):

        if self.takeoff_state_mode == 'data':
            if not self.is_transformed == True:
                self.transform(pipe_call=pipe_call)
            if variables == 'all':          
                out = self.takeoff_zone
                self.departures.append(to_node)
                self.output_nodes.add(to_node)
                return out
            else:
                assert isinstance(variables,list)
                out = {self.takeoff_zone[out] for out in self.takeoff_zone if out in variables}
                self.departures.append(to_node)
                self.output_nodes.add(to_node)
                return out
        
        elif self.takeoff_state_mode == 'generator':

            if variables == 'all':                          
                out = self.transform(pipe_call = pipe_call)
                self.departures.append(to_node)
                self.output_nodes.add(to_node)
                return out
            else:
                assert isinstance(variables,list)
                output = self.transform(pipe_call = pipe_call)
                out = {output[out] for out in self.takeoff_zone if out in variables}
                self.departures.append(to_node)
                self.output_nodes.add(to_node)
                return out
        
        else:
            raise ValueError('{}.takeoff_state_mode must be one of ["data","generator"]'.format(self.name))

    def store(self, values):

        storing_colisions =  set(self.takeoff_zone).intersection(values)
        if storing_colisions:
            warnings.warn('an output colision occured in {} takeoff_zone with the following variables: {}. old values will be overwritten.'.format(self.name,storing_colisions))
        self.takeoff_zone = {**self.takeoff_zone,**values}

    def take(self, sender, pipe_call):
        
        inputs = sender.send(variables = 'all', to_node = self.name, pipe_call = pipe_call)
        landing_intersection =  set(self.landing_zone).intersection(inputs)
        if landing_intersection:
            raise KeyError('an input colision occured in the landing zone with the following variables: {}'.format(landing_intersection))
        
        self.landing_zone = {**self.landing_zone,**inputs}
    
    def clear_landing_zone(self):
        self.landing_zone = {}

    def clear_takeoff_zone(self):
        self.landing_zone = {}
        self.departures = []


    def check_landing_zone(self, params, return_missing = True):
        
        if set(params).issubset(set(self.landing_zone)):
            self.required_inputs_landed = True
            return set(self.landing_zone)
        else:           
            self.required_inputs_landed = False
            if return_missing == True:
                return set(params) - set(self.landing_zone)
            else:
                return set(self.landing_zone)

    def fit(self, pipe_call):

        self.clear_landing_zone() 

        if self.is_callable == True:
            print('{} is a callable estimator and fit method will not be performed'.format(self.name))
            self.is_fitted = True
            return

        ### check if all inputs are in landing zone
        self.check_landing_zone(self.required_inputs)        
        ### if not all required inputs are in landing zone, get it from previous nodes in graph
        if self.required_inputs_landed == False:
            for sender in self.input_nodes:
                self.take(sender, pipe_call = pipe_call)
        ### check again
        lz_args = str(self.check_landing_zone(self.required_inputs, return_missing = True))

        ### if all required inputs are in landing zone, perform fit
        if self.required_inputs_landed == True:
            
            inputs = self.landing_zone

            if self.required_inputs:
                self.check(
                    allowed = self.required_inputs,
                    check_mode = self.input_check_mode,
                    inputs = inputs
                    )

            getattr(
                self.estimator,
                self.fit_method
                )(
                    **inputs,
                    **self.estimator_fitargs
                )
            
            self.is_fitted = True
            return self
        
        else:
            raise AttributeError('{} parameters are missing in {}.landing_zone'.format(lz_args,self.name))

    def __setitem__(self,key,value):
        setattr(self,key,value)
        return


    def transform(self, pipe_call):

        # Send only if call matches fit_only/transform_only
        if (self.fit_only) and (pipe_call == 'transform'):
            return self.bypass()
        elif (self.transform_only) and (pipe_call == 'fit'):
            return self.bypass()

        self.clear_landing_zone()

        if self.is_fitted == False:
            self.fit(pipe_call = pipe_call)

        ### check if all inputs are in landing zone
        self.check_landing_zone(self.required_inputs)
        
        ### if not all required inputs are in landing zone, get it form previous nodes in graph
        if self.required_inputs_landed == False:
            for sender in self.input_nodes:
                self.take(sender, pipe_call= pipe_call)

        ### check again
        lz_args = str(self.check_landing_zone(self.required_inputs, return_missing = True))

        ### if all required inputs are in landing zone, perform transform
        if self.required_inputs_landed == True:
            
            inputs = self.landing_zone
        
            if self.required_inputs:
                self.check(
                    allowed = self.required_inputs,
                    check_mode = self.input_check_mode,
                    inputs = inputs
                    )
            

            output = getattr(
                self.estimator,
                self.transform_method
                )(
                    **inputs,
                    **self.estimator_transformargs
                )

            self.wrapped_output = True
            if not isinstance(output, dict):
                output = self.output_wrapper(output, var_name = self.output_name)
                warnings.warn(('{} output type is {} instead of {}.\noutput have been wrapped in dict with key {}'.format(self.name, type(output), 'dict', str(self.output_name))))
                self.wrapped_output = True
            
            if self.takeoff_state_mode == 'data':
                self.store(output)
            self.is_transformed = True
            return output

        ### if inputs are still not in landing_zone, raise error
        else:
            raise AttributeError('{} parameters are missing in {}.landing_zone'.format(lz_args,self.name))

    def check(self, allowed, check_mode, inputs):
        
        checked_input = check_flow(
            allowed = allowed,
            check_mode = check_mode,
            inputs = inputs
            )
        if checked_input:
            return checked_input

    def output_wrapper(self, item ,var_name):
        self.wrapped_output = True
        return {str(var_name):item}

    def bypass(self, pipe_call):
        self.clear_landing_zone()
        ### check if all inputs are in landing zone
        self.check_landing_zone(self.required_inputs)        
        ### if not all required inputs are in landing zone, get it from previous nodes in graph
        if self.required_inputs_landed == False:
            for sender in self.input_nodes:
                self.take(sender, pipe_call = pipe_call)
        ### check again
        lz_args = str(self.check_landing_zone(self.required_inputs, return_missing = True))

        if self.required_inputs_landed == True:            
            inputs = self.landing_zone
            self.clear_takeoff_zone()
            self.store(**inputs)
        else:
            raise AttributeError('{} parameters are missing in {}.landing_zone'.format(lz_args,self.name))

        return inputs


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

def get_output_json_keys(estimator, transform_method):
    
    if estimator != None:
        if transform_method != '__call__':
            source_code = inspect.getsource(getattr(estimator, transform_method))
        else:
            source_code = inspect.getsource(estimator)
        
        json_str = source_code.split('return')[-1]
        json_str = json_str.replace(' ', '')
        json_str = json_str.replace("'", '"')
        try:
            values = re.findall('"([^"]+?)"\s*:', json_str)
            allowed_outputs = values
        except:
            allowed_outputs = [json_str]
    else:
        allowed_outputs = {}

    return allowed_outputs