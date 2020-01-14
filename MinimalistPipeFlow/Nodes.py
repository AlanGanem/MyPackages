from .Base import Capsula
import inspect
import warnings

class Node(Capsula):
	'''
	rename the capsula class in order to avoid calling from Base
	'''
	pass

class Inputer(Capsula):
	def __init__(self,**kwargs):
		super().__init__(
			None,
			is_fitted = True,
			is_callable = False,
			required_inputs = {'fit':None,'transform':None},
			**kwargs
			)

	def __call__(self,inputs):
		if not isinstance(inputs,dict):
			raise TypeError('Inputs must be a dict')
		self.store(inputs)

	def transform(self,):
		return self.takeoff_zone

class Renamer(Capsula):
	'''
	renames the output dict keys.
	mapper renames from key to value
	'''
	def __init__(self,mapper, **nodeargs):
		estimator = self.renamer
		estimator_fitargs = {'mapper': mapper}
		super().__init__(
			estimator=estimator,
			estimator_fitargs = estimator_fitargs,
			estimator_transformargs = estimator_fitargs,
            required_inputs = {'fit':list(mapper.keys()),'transform':list(mapper.keys())},
			optional_inputs= {'fit': list(mapper.keys()), 'transform': list(mapper.keys())},
			filter_inputs = False,
			**nodeargs
		)
		self.mapper = mapper

	def take(self, variables, sender):
		variables = 'all'
		inputs = sender.send(
			variables=variables,
			to_node=self.name,
		)
		landing_intersection = set(self.landing_zone).intersection(inputs)
		if landing_intersection:
			warnings.warn('an input colision occured in the landing zone with the following variables: {}'.format(
				landing_intersection))

		print({**self.landing_zone, **inputs})

		self.landing_zone = {**self.landing_zone, **inputs}


	def renamer(self, mapper, **inputs):
		print(mapper)
		print(inputs.keys())
		assert isinstance(mapper, dict)
		for key in mapper:
			try:
				inputs[mapper[key]] = inputs.pop(key)
			except KeyError:
				pass
		outputs_dict = inputs
		return outputs_dict

class Getter(Capsula):
	def __init__(self,attributes,**nodeargs):
		assert isinstance(attributes, list)
		assert all([isinstance(i, str) for i in attributes])
		required_inputs = {
			'fit': attributes,
			'transform': attributes
		}
		super().__init__(
			estimator = None,
			required_inputs = required_inputs,
			none_estim_outputs_fit = attributes,
        	none_estim_outputs_transform = attributes,
			**nodeargs
		)
		self.attributes = attributes

	def take(self, variables ,sender):
		attributes = self.attributes
		estimator = sender.hatch()
		self.landing_zone = {}
		for attribute in attributes:
			try:
				attribute_value = getattr(estimator, attribute)
				self.landing_zone = {**self.landing_zone, **{attribute:attribute_value}}
				print(self.landing_zone)
			except:
				print('{} does not have the attribute {}'.format(sender, attribute))
				pass


	def fit(self):
		self.bypass()
		self.is_fitted = True
	def transform(self):
		self.bypass()
		self.is_transformed = True