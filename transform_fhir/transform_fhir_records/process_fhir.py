"""Module to read fhir bundle from the queue and transform it to dataframe"""
import importlib
import logging
import pandas as pd
from tabulate import tabulate
from  common.fhir_queue import FhirQueue

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.INFO)

class ProcessFihr:
      """Classs reads data from the queue and based on resourceType <do something>"""
      def __init__(self) -> None:
            self.has_began = False
      
      async def process_bundle(self):
        """get fhir bundle from queue and flatten it"""
        while True:
            # Wait for first bundle to go in the queue
            if self.has_began is True and FhirQueue().queue_size() == 0:
                 break
            fhil_block = await FhirQueue().dequeue()
            self.has_began = True
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
            #logging.info(tabulate(entity_df_dict["Encounter"], headers='keys', tablefmt='psql'))
            print(FhirQueue().queue_size())

      def _flatten_obj(self, d, parent_key=''):
            """Flatten fhir.resurce objects to represent as 2d table"""
            flat_list = []
            for k, v in d.items():
                  clild_key = parent_key + k
                  if isinstance(v, dict):
                        flat_list.extend(self._flatten_obj(v, clild_key).items())
                  else:
                        flat_list.append((clild_key, v))
            return dict(flat_list)
      

           
           