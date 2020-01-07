# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 18:57:23 2020

@author: User Ambev
"""
import pydot_ng as pydot
from Nodes import Inputer, Node

def model_to_dot(graph,
                 show_layer_names=True,
                 rankdir='TB',
                 expand_nested=False,
                 dpi=96,
                 subgraph=False):

    def add_edge(dot, src, dst):
      if not dot.get_edge(src, dst):
        dot.add_edge(pydot.Edge(src, dst))

    #create dot object
    dot = pydot.Dot()
    dot.set('rankdir', rankdir)
    dot.set('concentrate', True)
    dot.set('dpi', dpi)
    dot.set_node_defaults(shape='record')

    for node in graph:
        label = '{}: {}'.format(node.__class__.__name__,node.name)
        mode = ''
        if node.fit_only:
            mode = 'fit_only'
        elif node.transform_only:
            mode = 'transform_only'
        #create node_labels
        if isinstance(node, Inputer):
            required_input_labels = 'None'
            optional_input_labels = 'None'

        else:
            if not node.is_callable:
                required_input_labels = node.required_inputs
                optional_input_labels = node.optional_inputs
            else:
                required_input_labels = node.required_inputs['fit']
                optional_input_labels = node.optional_inputs['fit']

        if node.allowed_outputs:
            output_labels = node.allowed_outputs
        else:
            output_labels = 'all'

        if not isinstance(node, Inputer):
            label = "%s\n%s|{required_input:|optional_input:|output:}|{{%s}|{%s}|{%s}}" % (
                label,
                mode,
                required_input_labels,
                optional_input_labels,
                output_labels)
        else:
            label = "Inputer\n%s" % (
                node.name if node.name != 'None' else '',
                )


        node = pydot.Node(node.name, label=label)
        dot.add_node(node)
    for edge in graph.edges:
        add_edge(dot, edge[0].name, edge[1].name)

    return dot

def create_graph(output, graph):
    if not output in graph:
        graph.add_node(output)
    for node in output.input_nodes:
        if node.input_nodes:
            if not node in graph:
                graph.add_node(node)
            graph.add_edge(node,output)
            create_graph(node,graph)
        else:
            if not node in graph:
                graph.add_node(node)
            graph.add_edge(node,output)

