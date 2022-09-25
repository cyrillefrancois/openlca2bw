# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:09:12 2021

@author: cyrille.francois
"""
import brightway2 as bw
import pandas as pd
import re
from bw2parameters.utils import EXISTING_SYMBOLS
import math
from enum import Enum
from numbers import Number
from bw2io.errors import UnsupportedExchange
from stats_arrays import UndefinedUncertainty, NoUncertainty, NormalUncertainty, LognormalUncertainty, TriangularUncertainty, UniformUncertainty

#unit normalization based on bw2io-0.9.DEV9, issues with previous version
UNITS_NORMALIZATION = {
    "a": "year",  # Common in LCA circles; could be confused with are
    "bq": "Becquerel",
    "g": "gram",
    "gj": "gigajoule",
    "h": "hour",
    "ha": "hectare",
    "hr": "hour",
    "item(s)": "unit",
    "kbq": "kilo Becquerel",
    "kg": "kilogram",
    "kgkm": "kilogram kilometer",
    "km": "kilometer",
    "kj": "kilojoule",
    "kwh": "kilowatt hour",
    "l": "litre",
    "lu": "livestock unit",
    "m": "meter",
    "m*year": "meter-year",
    "m2": "square meter",
    "m2*year": "square meter-year",
    "m2a": "square meter-year",
    "m2*a": "square meter-year",
    "m2y": "square meter-year",
    "m3": "cubic meter",
    "m3*year": "cubic meter-year",
    "m3a": "cubic meter-year",
    "m3y": "cubic meter-year",
    "ma": "meter-year",
    "metric ton*km": "ton kilometer",
    "mj": "megajoule",
    "my": "meter-year",
    "nm3": "cubic meter",
    "p": "unit",
    "personkm": "person kilometer",
    "person*km": "person kilometer",
    "pkm": "person kilometer",
    "tonnes": "ton",
    "t": "ton",
    "tkm": "ton kilometer",
    "t*km": "ton kilometer",
    "vkm": "vehicle kilometer",
    "kg sw": "kilogram separative work unit",
    "km*year": "kilometer-year",
    "metric ton*km": "ton kilometer",
    "person*km": "person kilometer",
    "wh": "watt hour",
}

normalize_units = lambda x: UNITS_NORMALIZATION.get(
    (x.lower() if isinstance(x, str) else x), x
)


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

def return_attribute(data,elements):
    if not type(elements) is tuple:
        elements = tuple([elements])
    for i in elements:
        try:
            data = data[i]
        except TypeError:
            try:
                i = re.sub(r'@','',i)
                data = getattr(data,i)
            except AttributeError:
                i = re.sub(r'[A-Z]+', lambda pat: '_'+pat.group().lower(),i)
                try:
                    data = getattr(data,i)
                except AttributeError:
                    return(None)
        except KeyError:
            return(None)
    if isinstance(data, Enum):
        data = data.value
    return(data)

def ref_flow(process,name = False):
    if return_attribute(process,'flow'):
        return(return_attribute(process,'flow'))
    else:
        ref_exc = [return_attribute(exc,'flow') 
                for exc in return_attribute(process,'exchanges') 
                if return_attribute(exc,'quantitativeReference')]
        if len(ref_exc) != 1:
            print("Zero or multiple reference flow for process"+return_attribute(process,"name"))
            return(None)
        if name:
            return(return_attribute(ref_exc[0],'name'))
        else:
           return(return_attribute(ref_exc[0],'@id'))

def root_folder(process):
    if not isinstance(process, dict):
        p_category = return_attribute(process,"categoryPath")
    else:
        p_category = return_attribute(process,("category",'categoryPath'))
        if p_category is dict or p_category is None:
            p_category = return_attribute(process,("category",'name')) 
    if isinstance(p_category,list):
        return(p_category[0])
    else:
        return(p_category)
    
def uncertainty_convert(uncertainty_dict, negative=False):
    sign = 1
    if negative:
        sign = -1
    if return_attribute(uncertainty_dict,'distributionType') == 'LOG_NORMAL_DISTRIBUTION':
        return({
            'uncertainty type': 2,
            'loc': math.log(abs(return_attribute(uncertainty_dict,'geomMean'))),
            'scale': math.log(return_attribute(uncertainty_dict,'geomSd'))
            })
    elif return_attribute(uncertainty_dict,'distributionType') == 'NORMAL_DISTRIBUTION':
        return({
            'uncertainty type': 3,
            'loc': sign * return_attribute(uncertainty_dict,'mean'),
            'scale': return_attribute(uncertainty_dict,'sd')
            })
    elif return_attribute(uncertainty_dict,'distributionType') == 'UNIFORM_DISTRIBUTION':
        return({
            'uncertainty type': 4,
            'minimum': sign * return_attribute(uncertainty_dict,'minimum'),
            'maximum': sign * return_attribute(uncertainty_dict,'maximum')
            })
    elif return_attribute(uncertainty_dict,'distributionType') == 'TRIANGLE_DISTRIBUTION':
        return({
            'uncertainty type': 5,
            'minimum': sign * return_attribute(uncertainty_dict,'minimum'),
            'maximum': sign * return_attribute(uncertainty_dict,'maximum'),
            'scale': sign * return_attribute(uncertainty_dict,'mode')
            })
    else:
        return(None)

def main_flow_table():
    DF_ids = pd.DataFrame(columns=['database','code','flow'])
    for db in bw.databases:
        if db == 'biosphere3':
            continue
        for act in bw.Database(db):
            DF_ids = pd.concat([DF_ids,pd.DataFrame.from_records([{k: v for k, v in act.items() if k in ['database','code','flow']}])],ignore_index=True)
    return DF_ids

def reformulate_formule(formula):
    formula = re.sub('\^','**',formula)
    if_search = re.search(r"if\(.*\)", formula)
    if if_search is None:
        return formula
    if_exp = reformulate_formule(if_search.group(0)[3:-1])
    if_split = if_exp.split(';')
    if len(if_split) == 3:
        res1 = if_split[1]
        res2 = if_split[2]
        condition = if_split[0]
        if_exp = '( ' + str(res1) + ' if ' + condition + ' else ' + res2 + ' )'
    else:
        if_exp =  if_exp
    return formula[:if_search.start()] + if_exp + formula[if_search.end():]


def change_formula(formula,changes):
    if formula is None:
        return None
    formula = reformulate_formule(formula)
    for k, v in changes.items():
        formula = re.sub(rf'\b{k}\b',rf'{v}',formula)
    return(formula)

def change_param_names(param_names):
    changes_dict = {}
    for param in param_names:
        if param in EXISTING_SYMBOLS:
            if not "p_"+param in param_names:
                changes_dict.update({param: "p_"+param})
                param_names = ["p_"+p if p == param else p for p in param_names]
            else:
                i = 1
                while "p"+str(i)+"_"+param.name in param_names:
                    i += 1
                changes_dict.update({param: "p"+str(i)+"_"+param})
                param_names = ["p"+str(i)+"_"+p if p == param else p for p in param_names]
    return(changes_dict)
        
def is_product(exchange):
    answer = False
    if return_attribute(exchange,('flow','flowType')) == 'WASTE_FLOW' and return_attribute(exchange,'input') == True:
        if return_attribute(exchange,'avoidedProduct') == False:
            answer = True
    if return_attribute(exchange,('flow','flowType')) == 'PRODUCT_FLOW' and return_attribute(exchange,'input') == False:
        if return_attribute(exchange,'avoidedProduct') == False:
            answer = True
    return(answer) 

def rescale_exchange(exc, factor):
    """
    Based on bw2io-0.9.DEV9, with same change on the log-normal rescaling
    Rescale exchanges, including formulas and uncertainty values, by a constant factor.

    No generally recommended, but needed for use in unit conversions. Not well tested.

    """
    assert isinstance(factor, Number)
    assert factor > 0 or exc.get("uncertainty type", 0) in {
        UndefinedUncertainty.id,
        NoUncertainty.id,
        NormalUncertainty.id,
    }
    if exc.get("formula"):
        exc["formula"] = "({}) * {}".format(exc["formula"], factor)
    if exc.get("uncertainty type", 0) in (UndefinedUncertainty.id, NoUncertainty.id):
        exc[u"amount"] = exc[u"loc"] = factor * exc["amount"]
    elif exc["uncertainty type"] == NormalUncertainty.id:
        exc[u"amount"] = exc[u"loc"] = factor * exc["amount"]
        exc[u"scale"] *= factor
    elif exc["uncertainty type"] == LognormalUncertainty.id:
        # ``scale`` in lognormal is scale-independent
        # loc is related to amount but with a logarithm factor
        exc[u"amount"] = factor * exc["amount"]
        if factor == 0:
            exc[u"loc"] = factor
        else:
            exc[u"loc"] = exc[u"loc"] + math.log(factor)
    elif exc["uncertainty type"] == TriangularUncertainty.id:
        exc[u"minimum"] *= factor
        exc[u"maximum"] *= factor
        exc[u"amount"] = exc[u"loc"] = factor * exc["amount"]
    elif exc["uncertainty type"] == UniformUncertainty.id:
        exc[u"minimum"] *= factor
        exc[u"maximum"] *= factor
        if "amount" in exc:
            exc[u"amount"] *= factor
    else:
        raise UnsupportedExchange(u"This exchange type can't be automatically rescaled")
    return exc


def convert_to_internal_ids(processes):
    find_index = lambda l, e: [i for i, v in enumerate(l) if v == e]
    index = [[] for i in processes]
    for db in bw.databases:
        if db != 'biosphere3' and db:
            db_list = list(bw.Database(db))
            act_ids = [act['code'] for act in db_list]
            act_params = [(act['name'],act['reference product'],return_attribute(act,'location')) for act in db_list]
            index = [(find_index(act_ids,v['id']) if index[i] == [] else index[i]) for i, v in enumerate(processes)]
            index = [(find_index(act_params,(v['name'],v['flow'],v['location'])) if index[i] == [] else index[i])
                        for i, v in enumerate(processes)]
            index = [((db,db_list[i[0]]['code']) if (i != [] and type(i) is not tuple) else i) for i in index]                
    return {v['id']: (index[i] if index[i] != [] else v['id']) for i, v in enumerate(processes)}
