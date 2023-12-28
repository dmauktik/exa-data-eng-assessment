"""ProcessFihr module reads fhir model object from the queue, parses and flattens the object
 and transform resource objects in tabular form representing resourceType as DB table."""
import importlib
import logging
from collections import OrderedDict
import simplejson as json
import pandas as pd
from  common.fhir_queue import FhirQueue
from common.storage_queue import StorageQueue

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', 
                    filename='transform_fhir.log', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

class ProcessFihr:
     """Class to fetch and process queue items/objects to dataframe"""
     def __init__(self) -> None:
            self.entity_df_dict = {}
     
     def _flatten_obj(self, d: dict, parent_key=''):
            """Recursive function to flatten fhir.resurce objects (array of dict)
            to json array object
            Input: d=dictionary object to be flattened
                   key_name=json parent key name used while breaking an object
            Returns: Dictionary of flattened object"""
            flat_list = []
            for k, v in d.items():
                  clild_key = parent_key + k
                  if isinstance(v, OrderedDict):
                        self._flatten_obj(v, clild_key).items()
                  else:
                        v_json = json.dumps(v, skipkeys=False, ensure_ascii=True, 
                        check_circular=True, allow_nan=True, cls=None, indent=None,
                        separators=None,encoding='utf-8', default=str, use_decimal=True,
                        namedtuple_as_object=True, tuple_as_array=True,bigint_as_string=False,
                        sort_keys=False, item_sort_key=None, for_json=False, ignore_nan=False)
                        flat_list.append((clild_key, v_json))
            return dict(flat_list)
      
     async def process_bundle(self):
        """Fetch fhir model objects from fhir queue, parses each resourceType 
        object to flattens it to transform in dataframe object. Finally put() in the storage queue.
        Input: None
        Returns: Boolean value representing status"""
        return_val = False
        logging.info("Starting to get items from fhir queue")
        while True:
            # Wait for the first fhir bundle object to go in the queue
            fhil_block = await FhirQueue().dequeue()
            self.entity_df_dict = {}
            if fhil_block is None:
                  break
            print(f"Transform task picking next object...Queue size {FhirQueue().queue_size()}")
            block_dict = fhil_block.dict()
            if "entry" not in block_dict:
                 logging.error("'entry' key missing in the fhil bundle dictionary")
                 return_val = False
                 break
            entry_dict = fhil_block.dict()["entry"]
            for dict_res in entry_dict:
                  rsrc = dict_res["resource"]
                  #method = dict_res["request"]["method"]
                  # Following logic is for the POST method i.e. insert new records in DB
                  # PUT method is not implemented for this PoC.
                  resource_type = rsrc["resourceType"]
                  resource_obj = None
                  try:
                        # Calling fhir.resources.R4B.<resourcetype>.<Resourcetype>.parsse_obj() method
                        # dynamically using importlib
                        module = importlib.import_module("fhir.resources.R4B." +  resource_type.lower())
                        class_ = getattr(module, resource_type)
                        resource_obj = class_.parse_obj(rsrc)
                        flat_data = self._flatten_obj(resource_obj.dict())
                        if flat_data["resourceType"] == "Patient":
                              logging.info(flat_data)
                        # transform parsed object to dataframe
                        df = pd.DataFrame([flat_data])
                        if resource_type in  self.entity_df_dict:
                              self.entity_df_dict[resource_type] = pd.concat([ self.entity_df_dict[resource_type], df], axis=0)
                        else:
                              self.entity_df_dict[resource_type] = df
                        logging.debug("Size of %s table is %d", resource_type, len(self.entity_df_dict[resource_type]))
                  except ModuleNotFoundError as ex:
                       logging.error("No module found %s", str(ex))
            await StorageQueue().enqueue( self.entity_df_dict)
            logging.debug("Size of resultant df dict is %d", len(self.entity_df_dict))
       
            #logging.debug(f"Queue items to process {FhirQueue().queue_size()}")
        return_val = FhirQueue().queue_size() == 0
        logging.info("Processed all the fhir queue items.")
        await StorageQueue().enqueue( None)
        return return_val