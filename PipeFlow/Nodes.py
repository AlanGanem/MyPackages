from Base import Capsula
import inspect

class Inputer(Capsula):

	def __init__(self,inputer,**kwargs):
		if callable(inputer):
			inspectobj = inspect.getfullargspec(inputer)
			required_vars = inspectobj.args[:-len(inspectobj.defaults)]
			if required_vars != []:
				raise ValueError('inputer takes {} but should have no required variable insted.'.format(required_vars))
			super().__init__(
				inputer,
				takeoff_state_mode = 'generator',
				transform_only = True,
				is_fitted = True,
				**kwargs
				)

		else:
			super().__init__(
				None,
				takeoff_zone = inputer,
				takeoff_state_mode = 'data',
				transform_only = True,
				is_fitted = True,
				**kwargs
				)
		
		
