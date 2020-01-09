from Base import Capsula
from functools import partial
import inspect

class Node(Capsula):
	'''
	rename the capsula class in order to avoid calling from Base
	'''
	pass

class Inputer(Capsula):
	def __init__(self,**kwargs):
		super().__init__(
			None,
			takeoff_state_mode = 'data',
			name = '',
			transform_only = False,
			is_fitted = True,
			is_callable = False,
			required_inputs = {},
			**kwargs
			)

	def __call__(self,kwargs):
		self.store(kwargs)
	def transform(self, pipe_call):
		# Send only if call matches fit_only/transform_only
		if (self.fit_only) and (pipe_call == 'transform'):
			return {}
		elif (self.transform_only) and (pipe_call == 'fit'):
			return {}
		
		return self.takeoff_zone

class Renamer(Capsula):
	'''
	renames the output dict keys.
	map renames from key to value
	'''
	def __init__(self,map, **nodeargs):
		estimator = partial(self.renamer, map = map)
		super().__init__(
			estimator=estimator,
			**nodeargs
		)

	def renamer(self,inputs,map ={}):
		assert isinstance(map, dict)
		for key in map:
			inputs[map[key]] = inputs.pop(key)
		outputs_dict = inputs
		return outputs_dict