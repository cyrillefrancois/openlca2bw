# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:09:12 2021

@author: cyrille.francois
"""
import brightway2 as bw
import pandas as pd

def flattenNestedList(nestedList):
    ''' Converts a nested list to a flat list '''
    flatList = []
    if isinstance(nestedList, list):  
        # Iterate over all the elements in given list
        for elem in nestedList:
            # Check if type of element is list
            if isinstance(elem, list):
                # Extend the flat list by adding contents of this element (list)
                flatList.extend(flattenNestedList(elem))
            else:
                # Append the elemengt to the list
                flatList.append(elem)
    else:
        # nestedList is not list but it's transformed into list
        flatList.append(nestedList)
    return flatList

def get_item(list, str_id=str):
    for f in list:
        if f['@id'] == str_id:
            return f

def return_attribute(data,elements):
    if not type(elements) is tuple:
        elements = tuple([elements])
    for i in elements:
        try:
            data = data[i]
        except KeyError:
            return(None)
    return(data)


def uncertainty_convert(uncertainty_dict):
    if return_attribute(uncertainty_dict,'distributionType') == 'LOG_NORMAL_DISTRIBUTION':
        return({
            'uncertainty type': 2,
            'loc': return_attribute(uncertainty_dict,'geomMean'),
            'scale': return_attribute(uncertainty_dict,'geomSd')
            })
    elif return_attribute(uncertainty_dict,'distributionType') == 'NORMAL_DISTRIBUTION':
        return({
            'uncertainty type': 3,
            'loc': return_attribute(uncertainty_dict,'mean'),
            'scale': return_attribute(uncertainty_dict,'sd')
            })
    elif return_attribute(uncertainty_dict,'distributionType') == 'UNIFORM_DISTRIBUTION':
        return({
            'uncertainty type': 4,
            'minimum': return_attribute(uncertainty_dict,'minimum'),
            'maximum': return_attribute(uncertainty_dict,'maximum')
            })
    elif return_attribute(uncertainty_dict,'distributionType') == 'UNIFORM_DISTRIBUTION':
        return({
            'uncertainty type': 4,
            'minimum': return_attribute(uncertainty_dict,'minimum'),
            'maximum': return_attribute(uncertainty_dict,'maximum'),
            'scale': return_attribute(uncertainty_dict,'mode')
            })
    else:
        return(None)

def main_flow_table():
    DF_ids = pd.DataFrame(columns=['database','code','flow'])
    for db in bw.databases:
        if db == 'biosphere3':
            continue
        for act in bw.Database(db):
            DF_ids = DF_ids.append({k: v for k, v in act.items() if k in ['database','code','flow']},ignore_index=True)
    return DF_ids


        
