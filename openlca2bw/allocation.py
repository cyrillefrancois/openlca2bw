import math #related to rescale_exchange function (cf bw2io.utils)
from numbers import Number #related to rescale_exchange function (cf bw2io.utils)
from stats_arrays import * #related to rescale_exchange function (cf bw2io.utils)
from .utils import rescale_exchange
import brightway2 as bw

"""These function will be used to conserve information about allocation
    And for multioutput process, it will be possible to apply an allocation rule when importing data
"""


VALID_METHODS = {
    "PHYSICAL_ALLOCATION",
    "ECONOMIC_ALLOCATION",
    "CAUSAL_ALLOCATION",
    "USE_DEFAULT_ALLOCATION",
    "NO_ALLOCATION",
}

def convert_alloc_factor(alloc_factors, process):
    list_factors = []
    for alloc_f in alloc_factors:
        if type(alloc_f) is not dict:
            alloc_f = alloc_f.to_json()
        alloc_dict = {
                    'allocationType': alloc_f['allocationType'],
                    'value': alloc_f['value']
                    }
        if alloc_f['allocationType'] == 'CAUSAL_ALLOCATION':
            alloc_exc = [exc for exc in process['exchanges'] if exc['internalId'] == alloc_f["exchange"]['internalId']][0]
            alloc_dict['key'] = (alloc_f["product"]["@id"],alloc_exc['flow'],alloc_exc['input'])
        else:
            alloc_dict['key'] = alloc_f["product"]["@id"]
        list_factors.append(alloc_dict)
    return(list_factors)

def split_Multioutputs_Process(databases_names, preferred_allocation="USE_DEFAULT_ALLOCATION"):
    if preferred_allocation is not None:
        assert preferred_allocation in VALID_METHODS, "Invalid allocation method given"
    else:
        preferred_allocation == "NO_ALLOCATION"
    changes_dict = {}
    for db in databases_names:
        for act in bw.Database(db):
            if act['Multioutputs']:
                if preferred_allocation == "USE_DEFAULT_ALLOCATION":
                    allocation_method = act['default allocation']
                else:
                    allocation_method == preferred_allocation
                alloc_factors = {alloc_f['key']: alloc_f['value'] 
                                for alloc_f in act['allocation factors'] 
                                if alloc_f['allocationType'] == allocation_method}

                
                products = act.production()
                for product in products:
                    if product['flow'] == act['flow']:
                        new_act = act
                    else:
                        new_act = act.copy()
                        changes_dict.update({(product['flow'],(db,act['code'])): (db,new_act['code'])})
                    
                    new_act['flow'] = product['flow']
                    new_act['reference product'] = product['name']
                    for coproducts in new_act.production():
                        if coproducts['flow'] != product['flow']:
                            coproducts.delete()
                    if allocation_method == "CAUSAL_ALLOCATION":
                        for exc in new_act.exchanges():
                            if exc['flow'] == product['flow']:
                                continue
                            exc = rescale_exchange(exc,alloc_factors[(product['flow'],exc['flow'],exc['input'])])
                            exc.save()
                    elif allocation_method in ["PHYSICAL_ALLOCATION","ECONOMIC_ALLOCATION"]:
                        for exc in new_act.exchanges():
                            if exc['flow'] == product['flow']:
                                continue
                            exc = rescale_exchange(exc,alloc_factors[product['flow']])
                            exc.save()
                    new_act.save()
    for db in databases_names:
        for act in bw.Database(db):
            for exc in act.exchanges():
                if (exc['flow'],exc['input']) in changes_dict.keys():
                    exc['input'] == changes_dict[(exc['flow'],exc['input'])]
                    exc.save()



""" def rescale_exchange(exc, factor):
     Rescale exchanges, including formulas and uncertainty values, by a constant factor.

    No generally recommended, but needed for use in unit conversions. Not well tested. 
    
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

 """



