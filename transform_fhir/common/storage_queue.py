"""Common module has shared resources and functionalities """
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
        """Returns queue size"""
        return self._queue.qsize()
    
    def empty(self):
        """Bool value to tell if queue is empty or not"""
        return self._queue.empty()
    
    async def enqueue(self, item):
        """push item to queue. Risk of overflow"""
        await self._queue.put(item)
        #print(self._queue.qsize())

    async def dequeue(self):
        """return item from the queue"""
        ret_val = await self._queue.get()
        self._queue.task_done()
        return ret_val
