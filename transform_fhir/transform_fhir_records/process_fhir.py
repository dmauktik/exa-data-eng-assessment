"""ProcessFihr module reads fhir bundle from the fhir queue and transform it to dataframe"""
import importlib
import logging
import simplejson as json
import pandas as pd
from collections import OrderedDict
from  common.fhir_queue import FhirQueue
from common.storage_queue import StorageQueue

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.INFO)

class ProcessFihr:
      """ProcessFihr class reads parsed fhir object from fhir queue, transforms it to tabular
      format and stores dataframe object to storage queue"""
      def __init__(self) -> None:
            self.has_began = False

      def _flatten_obj(self, d, parent_key=''):
            """Flatten fhir.resurce objects (array of dict) to represent as 2d table"""
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
        """Fetch fhir bundle objects from fhir queue and flatten it"""
        while True:
            # Wait for the first bundle to go in the queue
            if self.has_began is True and FhirQueue().queue_size() == 0:
                 break
            fhil_block = await FhirQueue().dequeue()
            self.has_began = True
            block_dict = fhil_block.dict()
            if "entry" not in block_dict:
                 logging.error("'entry' key missing in the fhil bundle dictionary")
                 return -1
            entry_dict = fhil_block.dict()["entry"]
            entity_df_dict = {}
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
                        # transform parsed object to dataframe
                        df = pd.DataFrame([flat_data])
                        if resource_type in entity_df_dict:
                            entity_df_dict[resource_type] = pd.concat([entity_df_dict[resource_type], df], axis=0) 
                        else:
                              entity_df_dict[resource_type] = df
                  except ModuleNotFoundError as ex:
                       logging.error("No module found %s", str(ex))
            await StorageQueue().enqueue(entity_df_dict)
            logging.info("Size of resultant df dict is %d", len(entity_df_dict))
            
            print(FhirQueue().queue_size())
        return FhirQueue().queue_size()