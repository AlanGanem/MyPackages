# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 18:57:23 2020

@author: User Ambev
"""
import pydot_ng as pydot


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
        #create node_labels
        input_labels = node.required_inputs

        if node.allowed_outputs:
            output_labels = node.allowed_outputs
        else:
            output_labels = 'all'

        label = "%s\n|{input:|output:}|{{%s}|{%s}}" % (
            label,
            input_labels,
            output_labels)

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

