from Base import Capsula
import inspect

class Node(Capsula):
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

	def __call__(self,**kwargs):
		self.store(kwargs)
	def transform(self, pipe_call):
		# Send only if call matches fit_only/transform_only
		if (self.fit_only) and (pipe_call == 'transform'):
			return {}
		elif (self.transform_only) and (pipe_call == 'fit'):
			return {}
		
		return self.takeoff_zone



