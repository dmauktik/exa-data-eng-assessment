"""tbd"""
from  common import fhir_queue
class ProcessFihr:
      """tbd"""
      def __init__(self) -> None:
            pass
      
      def process_bundle(self, fhil_block):
        """tbd"""
        que = fhir_queue.FhirQueue()
        fhil_block = que.dequeue()
        entry_dict = fhil_block.dict()["entry"]
        for dic in entry_dict:
            resrc = dic["resource"]
            resourceType = resrc["resourceType"]