# -*- coding: utf-8 -*-
"""
Created on Fri Dec 20 14:12:06 2019

@author: User Ambev
"""


import pickle

import networkx as nx
from matplotlib import pyplot as plt
import datetime

from .Base import *
from .Nodes import *
from .Utils import create_graph, model_to_dot


class Custom():
    ### IMPLEMENT __getitem__, __iteritem__, __repr__, __str__
    #### implement sequential mode

    @classmethod
    def load(cls, loading_path,**pickleargs):
        with open(loading_path, 'rb') as file:
            loaded_pipe = pickle.load(file, **pickleargs)
        return loaded_pipe
    
    def save(self, saving_path, **pickleargs):
        self.clear_landing_zones()
        self.clear_takeoff_zones()
        with open(saving_path, 'wb') as file:
            pickle.dump(self, file, **pickleargs)

    def __init__(self, input_nodes, output_nodes, clear_zones = None):
        if not isinstance(input_nodes, list):
            raise TypeError('input_nodes must be list')

        if not isinstance(output_nodes, list):
            raise TypeError('output_nodes must be list')

        for node in input_nodes+output_nodes:
            if not isinstance(node, Capsula):
                raise TypeError('{} should be an instance of Capsula'.format(node.name))
        
        
        self.clear_zones = clear_zones
        graph = self.build_graph(output_nodes)        
        
        self.check_graph(graph,input_nodes, output_nodes)
        self.graph = graph
        self.nodes = [node for node in self.graph]
        
        self.index_name_map = {node.name:index for index,node in enumerate(graph)}
        self.input_nodes = input_nodes
        self.output_nodes = output_nodes
        self.creation_date = datetime.datetime.now()
        

        
    
    def __iter__(self):
        return iter(self.graph)

    def __getitem__(self, name):
        return (list(self.graph.nodes)[self.index_name_map[name]])


    def build_graph(self, output_nodes):        
                    
        graph = nx.DiGraph()
        for output in output_nodes:
            create_graph(output, graph)

        return graph

    def populate_inputers(self, inputs):
        print(self.input_nodes)
        i = 0
        for node in self.input_nodes:
            node(inputs[i])
            i+=1

    def fit(self, inputs, clear_zones = True):
        assert isinstance(inputs, list)
        assert len(inputs) == len(self.input_nodes)

        self.populate_inputers(inputs)

        for node in self.output_nodes:
            self.recursive_fit([node])

        if clear_zones == True:
            self.clear_landing_zones()
            self.clear_takeoff_zones()
            
        self.is_fitted = True
        self.last_fit_date = datetime.datetime.now()
        return self     

    def transform(self, inputs, clear_zones = True):
        assert isinstance(inputs, list)
        assert len(inputs) == len(self.input_nodes)
        self.populate_inputers(inputs)

        outputs = []
        for node in self.output_nodes:
            self.recursive_transform([node])
            outputs.append(node.takeoff_zone)
            
        if clear_zones == True:
            self.clear_landing_zones()
            self.clear_takeoff_zones()

        self.is_transformed = True
        self.last_transform_date = datetime.datetime.now()
        return outputs
                            
    def check_graph(self,graph, input_nodes, output_nodes):
        #create fit and transform subgraph (only for checking)
        #self.fit_graph
        fit_graph = graph.subgraph([node for node in graph.nodes if not node.transform_only])
        transform_graph = graph.subgraph([node for node in graph.nodes if not node.fit_only])
        #check for connectedness and input nodes as endpoints
        if not nx.algorithms.components.weakly_connected.is_weakly_connected(graph):
            raise AssertionError('graph is disconnected')
        if not nx.algorithms.components.weakly_connected.is_weakly_connected(fit_graph):
            raise AssertionError('fit_graph is disconnected. {}'.format([node.name for node in transform_graph]))
        if not nx.algorithms.components.weakly_connected.is_weakly_connected(transform_graph):
            raise AssertionError('transform_graph is disconnected. {}'.format([node.name for node in transform_graph]))

        degrees = graph.degree
        degree1_nodes = set([node for node, degree in degrees if (degree == 1 and node not in output_nodes)])

        if (degree1_nodes - set(input_nodes)) != set({}) :
            not_input_nodes = degree1_nodes - set(input_nodes)
            raise AssertionError('{} are not an isntance of Inputer'.format(not_input_nodes))

        ###CHECK FOR DUPLICATE NAMES
        node_names = [node.name for node in graph]
        duplicates = [node for node in graph if node_names.count(node.name) >1]
        
        if duplicates:
            raise ValueError('node names should be unique. duplicated names in graph: {}'.format(duplicates))        

    def clear_landing_zones(self):
        for node in self.graph:
            node.clear_landing_zone()


    def clear_takeoff_zones(self):
        for node in self.graph:
            node.clear_landing_zone()
            node.is_transformed = False
    
    def plot_graph(self, prog = 'dot', root = None ,**drawargs):
        for node in self.graph:
            node.color = 'b'
        for node in self.input_nodes:
            node.color = 'g'
        for node in self.output_nodes:
            node.color = 'orange'
        
        color_map = [node.color for node in self.graph]
        nx.draw(self.graph, **drawargs, node_color = color_map)
        plt.show()

    def plot_pipeline(self, file_path, **plotargs):
        graph = self.graph
        dot = model_to_dot(graph, **plotargs)
        dot.write_png(file_path)

    def recursive_fit(self, nodes):
        nodes = [node for node in nodes if not node.transform_only]
        for node in nodes:
            required_inputs = node.required_inputs['fit']
            node_predecessors = [node for node in self.graph.predecessors(node) if not node.transform_only]
            node_successors = [node for node in self.graph.successors(node) if not node.transform_only]

            if not node.is_fitted:
                if node.landingzone_ready(required_inputs):
                    node.fit()
                    self.recursive_fit(node_successors)
                if not node.landingzone_ready(required_inputs):
                    for pre_node in node_predecessors:
                        if pre_node.is_transformed:
                            node.take(variables = 'all', sender = pre_node)
                        if not pre_node.is_transformed:
                            if pre_node.is_fitted:
                                self.recursive_transform([pre_node])
                                node.take(variables = 'all', sender = pre_node)
                            if not pre_node.is_fitted:
                                self.recursive_fit([pre_node])
                                self.recursive_transform([pre_node])
                                node.take(variables = 'all', sender = pre_node)
                    if node.landingzone_ready(required_inputs):
                        print('inputs, apply recursive fit in {}'.format(node.name))
                        self.recursive_fit([node])
                    else:
                        raise AssertionError('{} missing required_inputs'.format(node.name))
        return

    def recursive_transform(self, nodes):
        nodes = [node for node in nodes if not node.fit_only]
        for node in nodes:
            required_inputs = node.required_inputs['transform']
            node_predecessors = [node for node in self.graph.predecessors(node) if not node.fit_only]
            node_successors = [node for node in self.graph.successors(node) if not node.fit_only]
            print('recursive_transformed called by {}'.format(node.name))
            if node.is_fitted:
                if not node.is_transformed:
                    if node.landingzone_ready(required_inputs):
                        if not node.is_transformed:
                            node.transform()
                            self.recursive_transform(node_successors)
                    else:
                        for pre_node in node_predecessors:
                            node.take('all', pre_node)
                        if node.landingzone_ready(required_inputs):
                            node.transform()
                            self.recursive_transform(node_successors)
                        else:
                            self.recursive_transform(node_predecessors)
                            for pre_node in node_predecessors:
                                node.take('all', pre_node)
                            node.transform()
                            self.recursive_transform(node_successors)
                else:
                    return

            else:
                return
                #raise AssertionError('{} is not fitted'.format(node.name))



class Sequential(Custom):

    def __init__(self, estimators):
        input_nodes, output_nodes = self.encapsulate(estimators)
        super().__init__(
            input_nodes= input_nodes,
            output_nodes= output_nodes
        )

    def encapsulate(self, estimators):
        assert isinstance(estimators, list)
        i = 0
        estim_list = []
        for i, estim in enumerate(estimators):
            if i == 0:
                estim_list.append(Inputer())
            else:
                if isinstance(estim, Capsula):
                    setattr(estim, 'input_nodes', estim_list[i-1:i])
                    estim_list.append(estim)
                else:
                    estim_list.append(Capsula(estim, input_nodes = estim_list[i-1:i]))
            i+=1
        input_nodes = [estim_list[0]]
        output_nodes = [estim_list[-1]]
        return input_nodes, output_nodes