# -*- coding: utf-8 -*-
"""
Created on Fri Dec 20 14:12:06 2019

@author: User Ambev
"""


import pickle

import networkx as nx
from matplotlib import pyplot as plt

from Base import *
from Utils import create_graph, model_to_dot


class Custom():
    ### IMPLEMENT __getitem__, __iteritem__, __repr__, __str__
    #### implement sequential mode

    @classmethod
    def load(cls, loading_path,**pickleargs):
        with open(loading_path, 'rb') as file:
            loaded_pipe = pickle.load(file, **pickleargs)
        return loaded_pipe
    
    def save(self, saving_path, **pickleargs):
        with open(saving_path, 'wb') as file:
            pickle.dump(self, file, **pickleargs)

    def __init__(self, input_nodes, output_nodes):
        if not isinstance(input_nodes, list):
            raise TypeError('input_nodes must be list')

        if not isinstance(output_nodes, list):
            raise TypeError('output_nodes must be list')
        
        assert all([isinstance(node, Capsula) for node in input_nodes+output_nodes])
        graph = self.build_graph(output_nodes)        
        self.check_graph(graph,input_nodes, output_nodes)
        self.graph = graph
        
        self.index_name_map = {node.name:index for index,node in enumerate(graph)}
        self.input_nodes = input_nodes
        self.output_nodes = output_nodes
        
    
    def __iter__(self):
        return iter(self.graph)

    def __getitem__(self, name):
        return (list(self.graph.nodes)[self.index_name_map[name]])


    def build_graph(self, output_nodes):        
                    
        graph = nx.DiGraph()
        for output in output_nodes:
            create_graph(output, graph)
        
        return graph
        
    def fit(self):
        for node in self.output_nodes:
            if node.transform_only == True:
                node.bypass()
            else:
                node.fit()

        return self     
    
    def transform(self):
        self.clear_landing_zones()
        outputs = {}
        for node in self.output_nodes:
            if node.fit_only == True:
                outputs[str(node)] = node.bypass()                
            else:
                outputs[str(node)] = node.transform()

        if len(self.output_nodes) == 1:
            outputs = outputs[str(node)]
        return outputs
                            
    def check_graph(self,graph, input_nodes, output_nodes):
        #check for connectedness and input nodes as endpoints
        if not nx.algorithms.components.weakly_connected.is_weakly_connected(graph):
            raise AssertionError('graph is disconnected')
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
                estim_list.append(Inputer(estim))
            else:
                estim_list.append(Capsula(estim, input_nodes = estim_list[i-1]))
            i+=1
        input_nodes = [estim_list[0]]
        output_nodes = [estim_list[-1]]
        return input_nodes, output_nodes