"""Module to read fhir bundle from the queue and transform it to dataframe"""
import importlib
import logging
import simplejson as json
import pandas as pd
from collections import OrderedDict
from tabulate import tabulate
from  common.fhir_queue import FhirQueue
from common.storage_queue import StorageQueue

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.INFO)

class ProcessFihr:
      """Classs reads data from the queue and based on resourceType <do something>"""
      def __init__(self) -> None:
            self.has_began = False

      def _flatten_obj(self, d, parent_key=''):
            """Flatten fhir.resurce objects to represent as 2d table"""
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
            logging.info(flat_list)
            return dict(flat_list)
      
      async def process_bundle(self):
        """get fhir bundle from queue and flatten it"""
        while True:
            # Wait for first bundle to go in the queue
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
                  resource_type = rsrc["resourceType"]
                  resource_obj = None
                  try:
                        # Calling fhir.resources.R4B.<resourcetype>.<Resourcetype>.parsse_obj() method
                        module = importlib.import_module("fhir.resources.R4B." +  resource_type.lower())
                        class_ = getattr(module, resource_type)
                        resource_obj = class_.parse_obj(rsrc)
                        flat_data = self._flatten_obj(resource_obj.dict())
                        df = pd.DataFrame([flat_data])
                        if resource_type in entity_df_dict:
                            entity_df_dict[resource_type] = pd.concat([entity_df_dict[resource_type], df], axis=0) 
                        else:
                              entity_df_dict[resource_type] = df
                  except ModuleNotFoundError as ex:
                       logging.error("No module found %s", str(ex))
            #for k,v in entity_df_dict.items():
                  #logging.info(tabulate(entity_df_dict["Encounter"], headers='keys', tablefmt='psql'))
            #     logging.info(v.to_html())
            await StorageQueue().enqueue(entity_df_dict)
            logging.info("Size of resultant df dict is %d", len(entity_df_dict))

            print(FhirQueue().queue_size())
        return FhirQueue().queue_size()