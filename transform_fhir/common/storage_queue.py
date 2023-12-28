"""StorageQueue holds async Queue object. Transfrm module puts the processed object in this queue
and storage module consumes and pushes to database. Queue helps to decouple Transform and Load
process to improves scalability, reliability and availability"""
from asyncio import Queue
import logging

logging.basicConfig(filename='transform_fhir.log', encoding='utf-8', level=logging.INFO)

class StorageQueue(object):
    """A singleton class holding queue and used in transform and storage modules"""
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
        """push item to queue.
        Input: item: Python object representing fhir data"""
        await self._queue.put(item)

    async def dequeue(self):
        """Pops an item from the queue and returns it
        Input: None
        Returns: Poped value from the queue"""
        ret_val = await self._queue.get()
        return ret_val
