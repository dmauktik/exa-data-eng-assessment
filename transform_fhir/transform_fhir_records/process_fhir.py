"""Module to read fhir bundle from the queue and transform it to dataframe"""
from  common.fhir_queue import FhirQueue

class ProcessFihr:
      """Classs reads data from the queue and based on resourceType <do something>"""
      def __init__(self) -> None:
            self.has_began = False
      
      async def process_bundle(self):
        """tbd"""
        while True:
            # Wait for first bundle to go in the queue
            if self.has_began is True and FhirQueue().queue_size() == 0:
                 break
            fhil_block = await FhirQueue().dequeue()
            self.has_began = True
            entry_dict = fhil_block.dict()["entry"]
            for dict_res in entry_dict:
                  rsrc = dict_res["resource"]
                  resource_type = rsrc["resourceType"]
                  print(resource_type)
            print(FhirQueue().queue_size())
      