# -*- coding: utf-8 -*-
"""
Created on Fri Oct 11 11:57:20 2019

@author: User Ambev
"""

import os
import sys
import pandas as pd

def path_tree():
    return {
            'Check':{
                    
                    },
            'Inputs':{
                    
                    },
            'Model':{
                    'Architecture':{},
                    'Preprocessing':{},
                    'TrainedModels':{}
                    },
            'Outputs':{
                    'TrainedModels':{}
                    }
            }

def create_root_folder(root,model_name):
    tree = path_tree()
    root_path = r'{}\{}'.format(root,model_name) 
    def rec(directory, current_path):
        if len(directory):
            for direc in directory:
                print(current_path)
                rec(directory[direc], os.path.join(current_path, direc))
        else:
            os.makedirs(current_path)
            
    rec(tree, r"{}".format(root_path))    
    return

def save_keras_model(model,main_project_path,model_name):
    sub_folder = r'model'
    trained_models_folder = r'trained_models'
    today = pd.Timestamp.today().date()
    model_name = r'{}_{}'.format(model_name,today)
    trained_model_path = r'{}\{}\{}\{}.h5'.format(main_project_path,sub_folder,trained_models_folder,model_name)
    if os.path.isfile(trained_model_path):
        raise AssertionError('file already exists')
    else:
        model.save(trained_model_path)
    return