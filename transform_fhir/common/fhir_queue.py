"""FhirQueue holds async Queue object. Inject module puts object in this queue and transform
module consumes and processes the objects. Queue helps to decouple Extract and Transform
process and improves scalability, reliability and availability"""
from asyncio import Queue, sleep
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', 
                    filename='transform_fhir.log', encoding='utf-8', level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

class FhirQueue(object):
    """A singleton class holding queue object and used in ingest and transform modules"""
    _common_instance = None
    _queue = None
    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._common_instance, cls):
            cls._common_instance = object.__new__(cls, *args, **kwargs)
            cls._queue = Queue()
        return cls._common_instance

    def queue_size(self):
        """Returns queue size
        Input: None
        Returns: Number of items in the queue"""
        return self._queue.qsize()

    def empty(self):
        """Bool value to tell if queue is empty or not
        Input: None
        Returns: Boolean value to tell to the queue is empty or not"""
        return self._queue.empty()

    async def enqueue(self, item):
        """Push an item to queue.
        Input: item: Python object representing fhir data"""
        await sleep(0)
        await self._queue.put(item)


    async def dequeue(self):
        """Pops an item from the queue and returns it
        Input: None
        Returns: Poped value from the queue"""
        ret_val = await self._queue.get()
        await sleep(0)
        return ret_val
