# -*- coding: utf-8 -*-

from .utils import flattenNestedList, return_attribute, uncertainty_convert, main_flow_table, change_formula, change_param_names, is_product, ref_flow, root_folder, rescale_exchange, convert_to_internal_ids, normalize_units
from .bw_write import OpenLCABiosphereImporter, create_OpenLCA_biosphere3, register_method, create_OpenLCA_LCIAmethods, import_parameters, check_exchanges_units, single_provider_retriver
from .main import load_openLCA_IPC, update_openLCA_IPC, load_openLCA_Json, update_openLCA_Json
from .IPC_Extractor import IPC_Extractor
from .Json_Extractor import Json_Extractor
from .extract import Extraction_functions
from .allocation import convert_alloc_factor, split_Multioutputs_Process